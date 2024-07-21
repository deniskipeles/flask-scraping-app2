
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import logging
import json
import urllib.parse
from cachetools import cached, TTLCache
from config import redis_client


# Base URL for fetching API endpoints and extraction logic
BASE_URL = "https://stories-blog.pockethost.io"

# Fetch API endpoints and extraction logic from remote server
def fetch_api_endpoints(key):
    url = f"{BASE_URL}/api/collections/lambda_fxns/records"
    filter_str = f"key ?~ \"{key}\""
    params = {
        "page": 1,
        "perPage": 50,
        "filter": filter_str,
        "sort": "-created",
        "fields": "key, function"
    }
    encoded_params = urllib.parse.urlencode(params, safe='()')
    link = f"{url}?{encoded_params}"
    response = requests.get(link)
    if response.status_code == 200:
        json_obj = response.json()
        return json_obj.get("items", [])
    else:
        logging.error(f"Error fetching API endpoints: {response.status_code}")
        return []

# Fetch proxies from API endpoint
def fetch_proxies_0(endpoint):
    try:
        response = requests.get(endpoint['url'])
        response.raise_for_status()
        proxies_list = endpoint['extract'](response)
        return proxies_list
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching proxies: {e}")
        return []

# Save proxies to Redis cache
def save_proxies(api_endpoints):
    proxies = []
    for endpoint in api_endpoints:
        endpoint['extract'] = eval(f"lambda response: {endpoint['extract']}")
        proxies.extend(fetch_proxies_0(endpoint))
    redis_client.setex('proxies', 3600, '\n'.join(proxies))  # save for 10 minutes

# Local cache function
#cache = TTLCache(maxsize=1000, ttl=600)
#@cached(cache)
def get_proxy_from_cache():
    res = redis_client.get('proxies')
    if res:
       return res.decode('utf-8').split('\n')

# Main function
def fetch_proxies_main():
    api_endpoints = fetch_api_endpoints("fetch-proxies")
    if api_endpoints:
        save_proxies(api_endpoints[0]["function"])
        #print(len(get_proxy_from_cache()))
    else:
        logging.error("No API endpoints found")

def fetch_proxies():
    """Fetch a list of proxies from the API."""
    try:
        cache_p = get_proxy_from_cache()
        if cache_p:
           return cache_p
        else:
           fetch_proxies_main()
           time.sleep(2)
           return get_proxy_from_cache()
    except requests.exceptions.RequestException as e:
        fetch_proxies_main()
        time.sleep(2)
        return get_proxy_from_cache()



# Redis configuration
redis_key = 'fastest_proxies'

# Test URL
test_url = 'https://httpbin.org/get'

# Maximum number of threads
max_threads = 100

# Maximum response time
max_response_time = 5

def test_proxy(proxy):
    try:
        start_time = time.time()
        response = requests.get(test_url, proxies={"http": proxy, "https": proxy}, timeout=max_response_time)
        response_time = time.time() - start_time
        if response.status_code == 200 and response_time <= max_response_time:
            return proxy, response_time
    except:
        return proxy, float('inf')  # Return the proxy and infinity response time on exception
    return proxy, float('inf')  # Return the proxy and infinity response time if no exception

def get_fastest_proxies(proxies_list):
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_proxy = {executor.submit(test_proxy, proxy): proxy for proxy in proxies_list}
        results = []
        for future in as_completed(future_to_proxy):
            proxy, response_time = future.result()
            if response_time != float('inf') and response_time <= max_response_time:
                results.append((proxy, response_time))

    results.sort(key=lambda x: x[1])

    fastest_proxies = [proxy for proxy, _ in results]

    redis_client.set(redis_key, 7200, json.dumps(fastest_proxies))

    return fastest_proxies