## fetcher.py
import requests
import json
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from config import redis_client, REDIS_CACHE_EXPIRATION

import urllib.parse
import random
from cachetools import TTLCache

#logging.basicConfig(level=logging.DEBUG)

cache = TTLCache(maxsize=128, ttl=300)  # 300 seconds = 5 minutes



def fetch_and_cache(url, cache_expiration=REDIS_CACHE_EXPIRATION):
    cache_key = f"cache:{url}"
    cached_value = redis_client.get(cache_key)
    if cached_value:
        return json.loads(cached_value)

    response = requests.get(url)
    if response.status_code == 200:
        value = response.json()
        if value is not None:
            redis_client.setex(cache_key, cache_expiration, json.dumps(value))
        return value
    else:
        return None

def fetch_article_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def find_element(soup, selectors):
    for selector in selectors:
        if len(selector) == 3:
            tag, attr, value = selector
            if isinstance(value, list):
                soup = soup.find(tag, {attr: value}) if soup else None
            else:
                soup = soup.find(tag, {attr: value}) if soup else None
        else:
            tag = selector[0]
            soup = soup.find(tag) if soup else None
    return soup

def find_elements(soup, selectors):
    elements = [soup]
    for selector in selectors:
        new_elements = []
        for elem in elements:
            if len(selector) == 3:
                tag, attr, value = selector
                if isinstance(value, list):
                    new_elements.extend(elem.find_all(tag, {attr: value}) if elem else [])
                else:
                    new_elements.extend(elem.find_all(tag, {attr: value}) if elem else [])
            else:
                tag = selector[0]
                new_elements.extend(elem.find_all(tag) if elem else [])
        elements = new_elements
    return elements


def get_tags(tags, base_url):
    encoded_url = urllib.parse.quote_plus(str(tags))

    def fetch_tags():
        print (f"Cache miss: {tags}")
        url = f"{base_url}/api/collections/view_articles_list/records"
        filter_str = " || ".join(f"tags ?~ \"{tag}\"" for tag in tags)
        filter_str = f"({filter_str})"
        params = {
            "page": 1,
            "perPage": 50,
            "filter": filter_str,
            "sort": "-created",
            "fields": "tags"
        }
        encoded_params = urllib.parse.urlencode(params, safe='()')
        link = f"{url}?{encoded_params}"
        response = requests.get(link)
        if response.status_code == 200:
            json_obj = response.json()
            tags_list = []
            for item in json_obj.get("items", []):
                tags_list.append(item.get("tags", []))
            random.shuffle(tags_list)
            logging.debug(f"Cache hit: {tags}")
            return tags_list if len(tags_list)>0 else [[tags]]  # return default pass tags if tags_list is empty
        else:
            return [[tags]]#"Error:", response.status_code

    if encoded_url in cache:
        res_tags = random.shuffle(cache[encoded_url])
        return res_tags[:10]
    else:
        result = fetch_tags()
        cache[encoded_url] = result
        return result[:10]


