
from config import redis_client
from fetcher import get_proxy_from_cache
from proxies import get_fastest_proxies,fetch_proxies

import redis
import requests
import json
import logging
import urllib.parse
import random

# Set up logging
logging.basicConfig(filename='reddit_scraper.log', level=logging.INFO)

# Default agent configuration
default_agent = {
    "cache_expirations": 3600,
    "min_comments": 10,
    "min_comments_to_cache": 10,
    "min_content_length": 1000,
    "min_ups": 20,
    "min_score": 80,
    "subreddit": "news",
    "post_types": ["hot"],
    "url_json_object": {},
    "max_selftext_words": 500,
    "timeframes": ["hour", "day", "week"]  # ["hour", "day", "week", "month", "year", "all"]
}

# Load user-agents from file
path="user-agents.txt"
with open(path, 'r') as file:
    user_agents = file.readlines()
    user_agents = [agent.strip() for agent in user_agents]


def get_fast_proxies():
    try:
        p = redis_client.get("fastest_proxies")
        if p is None:
            proxies_list = fetch_proxies()
            sorted_proxies = get_fastest_proxies(proxies_list)
            return sorted_proxies
        p = p.decode('utf-8')
        p = json.loads(p)
        return p
    except redis.exceptions.RedisError as e:
        print(f"Error connecting to Redis: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

def scrape_url(proxies, user_agents, url, last_working_proxy=None, max_retries=30):
    """
    Scrape a URL using a rotating proxy and user agent.

    Args:
        proxies (list): List of proxy URLs
        user_agents (list): List of user agent strings
        url (str): URL to scrape
        last_working_proxy (str, optional): Last working proxy, if any
        max_retries (int, optional): Maximum number of retries for a single proxy

    Returns:
        response_text (str): HTML response text
        last_working_proxy (str): Last working proxy
    """
    if last_working_proxy:
        proxy_index = proxies.index(last_working_proxy) if last_working_proxy in proxies else 0
    else:
        proxy_index = 0

    retries = 0
    while retries < len(proxies):
        proxy = proxies[proxy_index]
        user_agent = random.choice(user_agents)
        headers = {'User-Agent': user_agent}
        print(headers)
        try:
            response = requests.get(url, proxies={'http': proxy, 'https': proxy}, headers=headers, timeout=10)
            if response.status_code == 200:
                logging.info(f"Successful request with proxy {proxy}")
                data = response.json()
                
                return data, proxy
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error with proxy {proxy}: {e}")
            retries += 1
        proxy_index = (proxy_index + 1) % len(proxies)

    logging.error(f"Failed to scrape URL after {max_retries} attempts")
    proxies_list = fetch_proxies()
    get_fastest_proxies(proxies_list)
    
    return None, None


last_working=None
def fetch_comments(post, user_agents, proxies, agent):
    """Fetch comments for a post using the scrape_url function."""
    comments_url = f"https://oauth.reddit.com/r/{post['subreddit']}/comments/{post['id']}/.json"
    response_text, last_working_ = scrape_url(proxies, user_agents, comments_url)
    last_working = last_working_

    if response_text:
        comments_data = response_text  # Parse the JSON response
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
            redis_client.setex(f"comments:{post['name']}", agent['cache_expirations'], "1")

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
        "limit": 20,
        "tags": [],
        "sort": "new",
        "t": "year",
        "comment": None
    }

    # Merge default values with passed JSON object
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
        timeframes = agent.get("timeframes", ["week"])

        for timeframe in timeframes:
            print(f"fetching for timeframe {timeframe}")
            # Use the create_reddit_api_url function to generate the URL
            url_json_object = agent.get("url_json_object", {})
            url_json_object["subreddit"] = agent['subreddit']
            url_json_object["sort"] = post_type
            url_json_object["t"] = timeframe

            search_tags = agent.get("search_tags", [])
            random.shuffle(search_tags)
            url_json_object["tags"] = [search_tags[0]+search_tags[1],search_tags[2]+search_tags[3]]

            url = create_reddit_api_url(url_json_object)
            print(url)

            proxies_list = get_fast_proxies()
            print(f"Fetched {len(proxies_list)} proxies for URL {url}")

            logging.info(f'Started fetching subreddit {post_type} posts for timeframe {timeframe}')
            response_text, last_working_proxy = scrape_url(proxies_list, user_agents, url)
            last_working = last_working_proxy

            logging.info('Finished fetching')

            if response_text:
                data = response_text
                print(f'Data received from Reddit for {post_type} with timeframe {timeframe}')
                print(len(data["data"]["children"]))
                for post in data["data"]["children"]:
                    post_data = post["data"]
                    print(post_data["title"])
                    if  len(post_data["selftext"].split()) > agent["max_selftext_words"] or post_data["num_comments"] >= agent['min_comments'] or post_data["ups"] >= agent['min_ups'] or post_data["score"] >= agent['min_score']:
                        reddit_data = {
                            "id": post_data["id"],
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
                            print(f"Post {post_data['name']} already processed.")
                        else:
                            word_count = len(post_data["selftext"].split())
                            if word_count <= agent["max_selftext_words"] or post_data["num_comments"] >= agent['min_comments']:
                                    #if post_data["num_comments"] >= agent['min_comments_to_cache']:
                                    comment_proxies_list = get_fast_proxies()
                                    reddit_data["comments"] = fetch_comments(post_data, user_agents, comment_proxies_list, agent)

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