
import requests
import json
# import redis
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import threading

# Set up logging
logging.basicConfig(filename='reddit_scraper.log', level=logging.INFO)

# Set up Redis client
# redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Configuration
BASE_URL = "https://reddit.com/r/{subreddit}/new.json"
HEADERS = {'User-Agent': 'Mozilla/5.0'}
PROXY_API_URL = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
NUM_PROXY_GROUPS = 100
NUM_COMMENT_PROXY_GROUPS = 100
MIN_COMMENTS_TO_CACHE = 4
REDIS_CACHE_EXPIRATION = 3600

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
            logging.info("Received response from proxy %s", proxy_ip)
            result = response.json()
            break
        except requests.exceptions.ProxyError:
            pass#logging.error(f"Error: Could not connect to the proxy {proxy_ip}.")
        except requests.exceptions.RequestException as e:
            pass#logging.error(f"Error with proxy {proxy_ip}: {e}")
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
                        logging.info("Received result: %s", result)
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

def fetch_comments(post, headers, proxy_groups):
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
        return comments
    return []

def fetch_subreddit_posts(subreddit):
    """Fetch posts from a subreddit."""
    url = BASE_URL.format(subreddit=subreddit)
    proxies_list = fetch_proxies()
    proxy_groups = split_list(proxies_list, NUM_PROXY_GROUPS)

    print('started')
    data = parallel_fetch(url, HEADERS, proxy_groups)
    print('parallel_fetch finished')  # This should always be printed

    if data:
        print('data received')  # This should print if data is received
        
        posts = []

        for post in data["data"]["children"]:
            if post["data"]["num_comments"] >= 2:
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

                # Check Redis cache for post with more than MIN_COMMENTS_TO_CACHE comments
                if redis_client.exists(post["data"]["name"]) and post["data"]["num_comments"] >= MIN_COMMENTS_TO_CACHE:
                    cached_comments = json.loads(redis_client.get(post["data"]["name"]))
                    reddit_data["comments"] = cached_comments
                else:
                    # Fetch comments if not in cache or less than MIN_COMMENTS_TO_CACHE comments in cache
                    comment_proxies_list = fetch_proxies()
                    comment_proxy_groups = split_list(comment_proxies_list, NUM_COMMENT_PROXY_GROUPS)
                    reddit_data["comments"] = fetch_comments(post, HEADERS, comment_proxy_groups)

                    # Cache comments if more than MIN_COMMENTS_TO_CACHE comments
                    if len(reddit_data["comments"]) >= MIN_COMMENTS_TO_CACHE:
                        redis_client.setex(post["data"]["name"], REDIS_CACHE_EXPIRATION, json.dumps(reddit_data["comments"]))

                posts.append(reddit_data)

        return posts
    else:
        logging.error("No data received from parallel fetch")
        return None


