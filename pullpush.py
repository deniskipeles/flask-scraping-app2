import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import threading
from config import GROQ_API_KEY, HEADERS_TO_POST, TEMP_API_URL, params, redis_client

# Set up logging
logging.basicConfig(filename='reddit_scraper.log', level=logging.INFO)

# Configuration
BASE_URL = "https://reddit.com/r/{subreddit}/new.json"
HEADERS = {'User-Agent': 'Mozilla/5.0'}
PROXY_API_URL = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"

def fetch_proxies():
    """Fetch a list of proxies from the API."""
    try:
        response = requests.get(PROXY_API_URL)
        response.raise_for_status()
        proxies_list = [proxy.strip() for proxy in response.text.split('\n') if proxy.strip()]
        return proxies_list
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching proxies: {e}")
        return []

def split_list(lst, n):
    """Split a list into n equal-sized sublists."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def fetch_data_with_proxy(url, headers, proxies, stop_event):
    """Fetch data from a URL using a list of proxies."""
    result = None
    for proxy_ip in proxies:
        if stop_event.is_set():
            break
        proxy = {
            "http": f"http://{proxy_ip}",
            "https": f"http://{proxy_ip}"
        }

        try:
            response = requests.get(url, proxies=proxy, headers=headers, timeout=5)
            response.raise_for_status()
            result = response.json()
            break
        except requests.exceptions.ProxyError:
            pass
        except requests.exceptions.RequestException as e:
            pass
    return result

def parallel_fetch(url, headers, proxy_groups):
    """Fetch data from a URL using multiple proxy groups in parallel."""
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=len(proxy_groups)) as executor:
        future_to_proxy_group = {executor.submit(fetch_data_with_proxy, url, headers, group, stop_event): group for group in proxy_groups}

        logging.info("Submitted all futures")
        try:
            for future in as_completed(future_to_proxy_group):
                if stop_event.is_set():
                    break
                try:
                    result = future.result()
                    if result:
                        stop_event.set()
                        return result
                except CancelledError:
                    logging.info("Future cancelled: %s", future)
                except Exception as e:
                    logging.error(f"Error in future: {e}")
        except Exception as e:
            logging.error(f"Error during parallel fetching: {e}")
    logging.info("No result received")
    return None

def fetch_comments(post, headers, proxy_groups, min_comments_to_cache):
    """Fetch comments for a post using multiple proxy groups in parallel."""
    comments_url = f"https://oauth.reddit.com/r/{post['data']['subreddit']}/comments/{post['data']['id']}/.json"
    comments_data = parallel_fetch(comments_url, headers, proxy_groups)

    if comments_data:
        comments = []
        for comment in comments_data[1]["data"]["children"]:
            try:
                comments.append({
                    "author": comment["data"]["author"] or 'anonymous',
                    "body": comment["data"]["body"] or 'no comment'
                })
            except KeyError as e:
                logging.error(f"Error: Missing key {e} in comment data.")
            except Exception as e:
                logging.error(f"Error: {e}")

        # Cache comments if more than MIN_COMMENTS_TO_CACHE comments
        if len(comments) >= min_comments_to_cache:
            redis_client.setex(post["data"]["name"], params['cache_expirations'], json.dumps(comments))

        return comments
    return []

def fetch_subreddit_posts(params):
    """Fetch posts from a subreddit using parameters."""
    default_params = {
        'subreddit': 'your_subreddit',
        'min_ups': 10,
        'min_comments': 2,
        'min_content_length': 50,
        'num_proxy_groups': 100,
        'num_comment_proxy_groups': 100,
        'min_comments_to_cache': 4,
        'cache_expirations': 18000
    }

    # Update the default parameters with the provided parameters
    default_params.update(params)

    url = BASE_URL.format(subreddit=default_params['subreddit'])
    proxies_list = fetch_proxies()
    proxy_groups = split_list(proxies_list, default_params['num_proxy_groups'])

    logging.info('Started fetching subreddit posts')
    data = parallel_fetch(url, HEADERS, proxy_groups)
    logging.info('Finished parallel fetch')

    if data:
        logging.info('Data received from Reddit')
        posts = []
        for post in data["data"]["children"]:
            if post["data"]["num_comments"] >= default_params['min_comments'] and post["data"]["ups"] >= default_params['min_ups'] and len(post["data"]["selftext"]) >= default_params['min_content_length']:
                reddit_data = {
                    "name": post["data"]["name"],
                    "title": post["data"]["title"],
                    "author": post["data"]["author"],
                    "created_utc": post["data"]["created_utc"],
                    "subreddit": post["data"]["subreddit"],
                    "content": post["data"]["selftext"],
                    "num_comments": post["data"]["num_comments"],
                    "ups": post["data"]["ups"],
                    "score": post["data"]["score"],
                    "link_flair_text": post["data"]["link_flair_text"],
                    "url": post["data"]["url"],
                    "permalink": post["data"]["permalink"],
                    "comments": []
                }
                if redis_client.exists(post["data"]["name"]) and post["data"]["num_comments"] >= default_params['min_comments_to_cache']:
                    cached_comments = json.loads(redis_client.get(post["data"]["name"]))
                    reddit_data["comments"] = cached_comments
                elif not redis_client.exists(post["data"]["name"]) and post["data"]["num_comments"] >= default_params['min_comments_to_cache']:
                    comment_proxies_list = fetch_proxies()
                    comment_proxy_groups = split_list(comment_proxies_list, default_params['num_comment_proxy_groups'])
                    reddit_data["comments"] = fetch_comments(post, HEADERS, comment_proxy_groups, default_params['min_comments_to_cache'])
                    if len(reddit_data["comments"]) >= default_params['min_comments_to_cache']:
                        redis_client.setex(post["data"]["name"], default_params['cache_expirations'], json.dumps(reddit_data["comments"]))
                if len(reddit_data["comments"]) >= default_params['min_comments_to_cache'] or len(reddit_data["content"]) >= default_params['min_content_length'] or reddit_data["ups"] >= default_params['min_ups']:
                    posts.append(reddit_data)
        return posts
    else:
        logging.error("No data received from parallel fetch")
        return None

