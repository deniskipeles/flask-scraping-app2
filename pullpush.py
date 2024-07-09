
from config import redis_client
from fetcher import get_proxy_from_cache

import requests
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, CancelledError
import threading
import urllib.parse
import random

# Set up logging
logging.basicConfig(filename='reddit_scraper.log', level=logging.INFO)

# Configuration
HEADERS = {'User-Agent': 'Mozilla/5.0'}
PROXY_API_URL = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"

# Default agent configuration
default_agent = {
    "cache_expirations": 3600,
    "min_comments": 10,
    "min_comments_to_cache": 10,
    "min_content_length": 1000,
    "min_ups": 20,
    "min_score": 80,
    "num_comment_proxy_groups": 100,
    "num_proxy_groups": 100,
    "subreddit": "news",
    "post_types": ["hot"],
    "url_json_object": {},
    "max_selftext_words": 500
}

# Load user-agents from file
with open('user-agents.txt', 'r') as file:
    user_agents = file.readlines()
    user_agents = [agent.strip() for agent in user_agents]

def fetch_proxies():
    """Fetch a list of proxies from the API."""
    try:
        return get_proxy_from_cache()
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

        # Randomly select five user-agents
        selected_agents = random.sample(user_agents, 5)

        for agent in selected_agents:
            headers['User-Agent'] = agent
            try:
                response = requests.get(url, proxies=proxy, headers=headers, timeout=5)
                response.raise_for_status()
                result = response.json()
                break
            except requests.exceptions.ProxyError:
                pass
            except requests.exceptions.RequestException as e:
                pass
        if result:
            break
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

def fetch_comments(post, headers, proxy_groups, agent):
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

        if len(comments) >= agent["min_comments_to_cache"]:
            redis_client.setex(f"comments:{post['data']['name']}", agent['cache_expirations'], "1")

        return comments
    return []

def create_reddit_api_url(json_obj):
    """
    Create a dynamic URL to fetch data from the Reddit API.

    Args:
        json_obj (dict): A JSON object containing the subreddit, limit, tags, sort, t, and comment parameters.

    Returns:
        str: The dynamic URL to fetch data from the Reddit API.
    """
    # Define default values for the JSON object
    default_json_obj = {
        "subreddit": "AskReddit",
        "limit": 50,
        "tags": [],
        "sort": "relevance",
        "t": "year",
        "comment": None
    }

    # Merge default values with passed JSON object using spread operator
    merged_json_obj = {**default_json_obj, **json_obj}

    # Extract values from the merged JSON object
    subreddit = merged_json_obj["subreddit"]
    tags = merged_json_obj["tags"]
    limit = merged_json_obj["limit"]
    sort = merged_json_obj["sort"]
    t = merged_json_obj["t"]
    comment = merged_json_obj.get("comment")

    # Construct the base URL
    base_url = f"https://oauth.reddit.com/r/{subreddit}/search.json"

    # Initialize the params dictionary with limit, sort, and t parameters
    params = {
        "limit": limit,
        "sort": sort,
        "t": t
    }

    # Handle list of list tags
    if isinstance(tags, list) and all(isinstance(tag, list) for tag in tags):
        # Join each set of tags with "+" and then join the sets with "|"
        tag_queries = ["+".join(tag) for tag in tags]
        params["q"] = "|".join(tag_queries)
    else:
        # Join all tags with "+"
        params["q"] = "+".join(tags)

    # Add the comment parameter if it exists
    if comment:
        params["comment"] = comment

    # Construct the final URL by encoding the query parameters and appending them to the base URL
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    return url

def fetch_subreddit_posts(agent=None):
    """Fetch subreddit posts for all specified post types."""
    if agent is None:
        agent = default_agent
    else:
        agent = {**default_agent, **agent}

    all_posts = []

    for post_type in agent['post_types']:
        found_posts = False
        timeframes = ["hour", "day", "week", "month", "year", "all"]

        for timeframe in timeframes:
            print(f"fetching for timeframe {timeframe}")
            # Use the create_reddit_api_url function to generate the URL
            url_json_object = agent.get("url_json_object", {})
            url_json_object["subreddit"] = agent['subreddit']
            url_json_object["sort"] = post_type
            url_json_object["t"] = timeframe
            url = create_reddit_api_url(url_json_object)

            proxies_list = fetch_proxies()
            logging.info(f"Fetched {len(proxies_list)} proxies for URL {url}")
            proxy_groups = split_list(proxies_list, agent['num_proxy_groups'])

            logging.info(f'Started fetching subreddit {post_type} posts for timeframe {timeframe}')
            data = parallel_fetch(url, HEADERS, proxy_groups)
            logging.info('Finished parallel fetch')

            if data:
                logging.info(f'Data received from Reddit for {post_type} with timeframe {timeframe}')
                print(len(data["data"]["children"]))
                for post in data["data"]["children"]:
                    post_data = post["data"]
                    if post_data["num_comments"] >= agent['min_comments'] or post_data["ups"] >= agent['min_ups'] or post_data["score"] >= agent['min_score']:
                        reddit_data = {
                            "name": post_data["name"],
                            "title": post_data["title"],
                            "author": post_data["author"],
                            "created_utc": post_data["created_utc"],
                            "subreddit": post_data["subreddit"],
                            "content": post_data["selftext"],
                            "num_comments": post_data["num_comments"],
                            "ups": post_data["ups"],
                            "score": post_data["score"],
                            "link_flair_text": post_data["link_flair_text"],
                            "url": post_data["url"],
                            "permalink": post_data["permalink"],
                            "comments": []
                        }
                        # Check Redis for a processed flag
                        if redis_client.exists(f"processed:{post_data['name']}"):
                            logging.info(f"Post {post_data['name']} already processed.")
                        else:
                            word_count = len(post_data["selftext"].split())
                            if word_count <= agent["max_selftext_words"]:
                                if post_data["num_comments"] >= agent['min_comments_to_cache']:
                                    comment_proxies_list = fetch_proxies()
                                    comment_proxy_groups = split_list(comment_proxies_list, agent['num_comment_proxy_groups'])
                                    reddit_data["comments"] = fetch_comments(post, HEADERS, comment_proxy_groups, agent)

                                    # if len(reddit_data["comments"]) >= agent['min_comments_to_cache'] or len(reddit_data["content"]) >= agent['min_content_length'] or reddit_data["ups"] >= agent['min_ups']:
                                    all_posts.append(reddit_data)
                                    # Set processed flag in Redis
                                    redis_client.setex(f"processed:{post_data['name']}", agent['cache_expirations'], "1")
                                    found_posts = True
                            else:
                                all_posts.append(reddit_data)
                                # Set processed flag in Redis
                                redis_client.setex(f"processed:{post_data['name']}", agent['cache_expirations'], "1")
                                found_posts = True

                # If we have found unprocessed posts, break the loop to avoid fetching for longer timeframes
                if found_posts:
                    break
            else:
                logging.warning(f"No data received from Reddit for {post_type} with timeframe {timeframe}")

    return all_posts