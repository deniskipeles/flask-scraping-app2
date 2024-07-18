## processor.py

import json
import requests
import logging
import time

from datetime import datetime
import pika
from config import GROQ_API_KEY, GEMINI_API_KEY, HEADERS_TO_POST, TEMP_API_URL, params, redis_client
from fetcher import fetch_and_cache, fetch_article_data, find_element, find_elements
from json_utils import extract_json_data
from gemini import gemini_generate_content

from urllib.parse import urljoin

import re

def extract_time(time_str):
    # Match and extract the time in minutes and seconds or just seconds
    pattern = re.compile(r"(?:(\d+)m)?([\d\.]+)s")
    match = pattern.match(time_str)
    
    if match:
        minutes = match.group(1)
        seconds = match.group(2)
        
        # Convert minutes and seconds to total seconds
        total_seconds = 0
        if minutes:
            total_seconds += int(minutes) * 60
        if seconds:
            total_seconds += float(seconds)
        
        return total_seconds
    else:
        return 50

        
def process_text(text, length):
    # Split the text into tokens
    tokens = text.split()
    
    # Check if the number of tokens exceeds 4200
    if len(tokens) > length:
        # Trim the tokens to 4200
        tokens = tokens[:length]
    
    # Join the tokens back into a single string
    processed_text = ' '.join(tokens)
    
    return processed_text
    


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
    data = fetch_and_cache(url,1200)
    return next((item for item in data['items'] if item.get(key) == value), None)



"""process with ai logic"""

MAX_CALLS = 20
PERIOD = 1
last_call_time = time.time()
call_count = 0

def process_with_groq_api(article, model="mixtral-8x7b-32768", change_model=True):
    logging.info('process_with_groq_api called')

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }

    logging.info("Article received for processing:")

    if 'data' not in article or 'processor' not in article['data']:
        logging.error("The article's data does not contain required keys.")
        return

    processor_id = article['data']['processor']
    processor = get_dynamic_content_controller('id', processor_id)
    if processor is None:
        logging.error(f"No processor found for id: {processor_id}")
        return

    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ai_content_system_prompt = processor['ai_content_system_prompt'].replace("___DATETIME___", current_datetime)
    text_context = article['data'].get("content")
    if change_model:
        model = processor.get('model', model)

    content = generate_content(model, text_context, ai_content_system_prompt,headers)

    if content:
        system_prompt_tst = processor['ai_tst_system_prompt']
        json_data = generate_title_summary_tags(content, system_prompt_tst)
        payload = create_payload(article, processor, content, json_data)
        post_data(payload)
    else:
      id_ = article["id"]
      trial_times = article.get("trial_times",0)
      base_url = "https://full-bit.pockethost.io"
      url = f"{base_url}/api/collections/scrape_data/records/{id_}"
      payload = {"failed_to_process": True,"trial_times":trial_times+1}
      headers = {"Content-Type": "application/json"}
      
      response = requests.patch(url, json=payload, headers=headers)
      
      if response.status_code == 200:
        print("Record updated successfully!")
      else:
        print("Error updating record:", response.text)
      if trial_times < 2:
        producer([id_],q='data_to_process_consumer')

def make_api_call(url, headers, data):
    global last_call_time, call_count
    current_time = time.time()
    if current_time - last_call_time < PERIOD:
        call_count += 1
        if call_count >= MAX_CALLS:
            time.sleep(PERIOD - (current_time - last_call_time))
            last_call_time = time.time()
            call_count = 0
    else:
        last_call_time = current_time
        call_count = 0

    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response

def generate_content(model, text_context, ai_content_system_prompt, headers):
    if model == 'gemini' or len(text_context.split()) > 2500:
        text = f"""
        <prompt>{ai_content_system_prompt}</prompt>
        <context>{text_context}</context>
        """
        for _ in range(3):
            content = gemini_generate_content(GEMINI_API_KEY, text)
            if content and len(content.split()) > 300:
                return content
        else:
            text_context = process_text(text_context, 2000)
            return generate_content("mixtral-8x7b-32768", text_context, ai_content_system_prompt, headers)
    else:
        data = {
            "messages": [
                {"role": "system", "content": ai_content_system_prompt},
                {"role": "user", "content": text_context}
            ],
            "model": model,
            "temperature": 1,
            "max_tokens": 1024,
            "top_p": 1,
            "stream": False
        }
        try:
          url="""https://api.groq.com/openai/v1/chat/completions"""
          for _ in range(5):
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
              res_content = response.json()['choices'][0]['message']['content']
              if res_content and len(res_content.split()) > 300:
                return res_content
              else:
                time.sleep(10)
            else:
              t = response.headers["x-ratelimit-reset-requests"] if response.headers else response.text
              t = extract_time(str(t))
              t = t + 2 if t > 5 else 10
              time.sleep(t)
          else:
              return None
        except requests.RequestException as e:
            logging.error(f"Error processing with Groq API:{model} {e}")
            t = extract_time(str(e))
            t = t + 2 if t > 5 else 10
            time.sleep(t)
            return generate_content(model, repr(text_context), repr(ai_content_system_prompt), headers)

def create_payload(article, processor, content, json_data):
    return {
        'id': article['id'],
        'title': json_data.get('title') or article['data'].get('title'),
        'author_id': processor.get('author_id') or article['data'].get('developer_id') or processor.get('developer_id') or article['data'].get('author_id'),
        'content': content,
        'sub_menu_list_id': processor.get('sub_menu_list_id') or article['data'].get('sub_menu_list_id'),
        'tags': json_data.get('tags') or processor.get('tags') or ['news', 'sports', 'politics'],
        'excerpt': json_data.get('summary'),
        'image_links': article['data'].get('image_links',[]) or []
    }

def post_data(payload):
    api_url = 'https://stories-blog.pockethost.io/api/collections/articles/records'
    for _ in range(3):
        try:
          if payload["content"] and len(payload["content"]) > 500:
            response = requests.post(api_url, json=payload, headers=HEADERS_TO_POST)
            response.raise_for_status()
            logging.info("Data posted successfully for AI")
            break
          else:
            break
        except requests.RequestException as e:
            logging.error(f"Error posting data for AI: {e}")
    else:
        logging.error("Failed to post data for AI after 3 attempts")

"""end process with ai logic"""


        

def producer(data,q='hello'):
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=q)

    for item in data:
        channel.basic_publish(exchange='', routing_key=q, body=item)

    logging.info(f"Sent {len(data)} {q} messages")
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
                producer([data_id],'data_to_process_consumer')
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
                'developer_id': scrape_configuration['author_id'],
                'author_id': scrape_configuration['author_id']
            }
            #print(obj)
            if content and len(content) > 200:
                results.append(obj)
                logging.info(f"Scraped data: {obj['title']}")
                post_data_to_api([obj])
            else:
                logging.info("Content is too short, skipping...")
                redis_client.setex(obj['link'], 7200, 'ban')
    return results
