## processor.py

import json
import requests
import logging
import time

from datetime import datetime
import pika
from config import GROQ_API_KEY, HEADERS_TO_POST, TEMP_API_URL, params, redis_client
from fetcher import fetch_and_cache, fetch_article_data, find_element, find_elements
from json_utils import extract_json_data

from urllib.parse import urljoin

import re

def extract_time(text):
    pattern = r"(\d+(?:\.\d+)?)s"
    match = re.search(pattern, text)
    if match:
        time_str = match.group(1)
        return float(time_str)
    else:
        return 5


def generate_title_summary_tags(content, system_prompt_tst, model="gemma-7b-it"):
    print('gen tags called')
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }
    data = {
        "messages": [
            {"role": "system", "content": system_prompt_tst},
            {"role": "user", "content": content}
        ],
        "model": model,
        "temperature": 1,
        "max_tokens": 1024,
        "top_p": 1,
        "stream": False
    }

    while True:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            content_ = response.json()['choices'][0]['message']['content']
            jsonData = extract_json_data(content_)
            if jsonData.get('title') or jsonData.get('summary') or jsonData.get('tags'):
                return jsonData
        else:
            time.sleep(1)

def get_dynamic_content_controller(key, value):
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    return next((item for item in data['items'] if item.get(key) == value), None)

def process_with_groq_api(article, model="mixtral-8x7b-32768"):
    print('proc with groq called')
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }

    print("Article received for processing:")
    #print(json.dumps(article, indent=2))

    if 'data' not in article or 'processor' not in article['data']:
        logging.error("The article's data does not contain required keys.")
        return

    id = article['data']['processor']
    processor = get_dynamic_content_controller('id', id)
    if processor is None:
        logging.error(f"No processor found for id: {id}")
        return

    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ai_content_system_prompt = processor['ai_content_system_prompt'].replace("___DATETIME___", current_datetime)
    data = {
        "messages": [
            {"role": "system", "content": ai_content_system_prompt},
            {"role": "user", "content": article['data'].get("content")}
        ],
        "model": model,
        "temperature": 1,
        "max_tokens": 1024,
        "top_p": 1,
        "stream": False
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        content = response.json()['choices'][0]['message']['content']
        system_prompt_tst = processor['ai_tst_system_prompt']
        jsonData = generate_title_summary_tags(content, system_prompt_tst)
        payload = {
            'id':article['id'],
            'title': jsonData.get('title') or article['data'].get('title'),
            'developer_id': processor.get('author_id') or article['data'].get('developer_id'),
            'content': content,
            'sub_menu_list_id': processor.get('sub_menu_list_id') or article['data'].get('sub_menu_list_id'),
            'tags': jsonData.get('tags') or processor.get('tags') or ['news', 'sports', 'politic'],
            'excerpt': jsonData.get('summary'),
            'image_links': article['data'].get('image_links')
        }
        api_url = 'https://stories-blog.pockethost.io/api/collections/articles/records'
        try:
            response = requests.post(api_url, json=payload, headers=HEADERS_TO_POST)
            response.raise_for_status()
            print("Data posted successfully for AI")
        except requests.RequestException as e:
            logging.error(f"Error posting data for AI: {e}")
    else:
        print(f"Error processing with Groq API: {response.status_code} - {response.text}")
        t = extract_time(response.text)
        time.sleep(t)
        process_with_groq_api(article)
        
        

def producer(data):
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='hello')

    for item in data:
        channel.basic_publish(exchange='', routing_key='hello', body=item)

    logging.info(f"Sent {len(data)} messages")
    connection.close()

def get_data_api(article_id):
    try:
        response = requests.get(f"{TEMP_API_URL}/{article_id}", headers=HEADERS_TO_POST)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            if data.get('id'):
                process_with_groq_api(data)
    except requests.RequestException as e:
        logging.error(f"Error posting data for link {article_id}: {e}")



def consumer(consumer_running=True):

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='hello')

    start_time = time.time()
    while consumer_running:
        method_frame, header_frame, body = channel.basic_get(queue='hello')
        if method_frame:
            print(f"Received {body}")
            get_data_api(body.decode('utf-8'))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            start_time = time.time()
        elif time.time() - start_time > 10:
            break
        time.sleep(3)

    connection.close()
    consumer_running = False  # Ensure the flag is reset when consumer stops



def post_data_to_api(data):
    for article in data:
        link = article.get('link')
        if redis_client.exists(link):
            logging.info(f"Link {link} already posted, skipping...")
            continue
        payload = {
            'data': article,
            'link': link
        }
        try:
            response = requests.post(TEMP_API_URL, json=payload, headers=HEADERS_TO_POST)
            response.raise_for_status()
            logging.info(f"Data posted successfully for link: {link}")
            redis_client.setex(link, 3600, "posted")
            if response.status_code == 200:
                data_id = response.json()['id']
                producer([data_id])
            time.sleep(0.1)
        except requests.RequestException as e:
            logging.error(f"Error posting data for link {link}: {e}")
            redis_client.setex(link, 3600, 'posted')

def scrape_data(scrape_configuration):
    results = []
    scrape_config = scrape_configuration['controller']
    link = scrape_config['main_link']
    headers = {'User-Agent': 'Mozilla/5.0'}
    soup = fetch_article_data(link, headers)

    if soup is None:
        return results

    link_elements = find_elements(soup, scrape_config['link']['selector'])
    unique_links = set()

    for link_element in link_elements:
        link_href = link_element.get('href')
        if link_href:
            if not link_href.startswith('http'):
                link_href = urljoin(link, link_href)
            if link_href in unique_links or redis_client.exists(link_href):
                print(f"Link {link_href} already processed, skipping...")
                continue

            unique_links.add(link_href)
            article_soup = fetch_article_data(link_href, headers)
            if article_soup is None:
                continue

            title_element = find_element(article_soup, scrape_config['title']['selector'])
            title = title_element.text.strip() if title_element else None
            content_element = find_element(article_soup, scrape_config['visit']['content']['selector'])
            content = content_element.text.strip() if content_element else None

            image_links = set()
            if content_element:
                for img in content_element.find_all('img'):
                    img_src = img.get('src')
                    if img_src and not img_src.startswith('http'):
                        img_src = urljoin(link_href, img_src)
                    image_links.add(img_src)

            obj = {
                'link': link_href,
                'title': title,
                'image_links': list(image_links),
                'content': content,
                'processor': scrape_configuration['id'],
                'developer_id': scrape_configuration['author_id']
            }
            #print(obj)
            if content and len(content) > 100:
                results.append(obj)
                logging.info(f"Scraped data: {obj['title']}")
                post_data_to_api([obj])
            else:
                logging.info("Content is too short, skipping...")
    return results
