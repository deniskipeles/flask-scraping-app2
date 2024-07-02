import multiprocessing
import pika
import json
import logging
import requests

from config import HEADERS_TO_POST, TEMP_API_URL, params, redis_client
from processor import get_data_api, post_data_to_api, scrape_data
from fetcher import fetch_and_cache
from pullpush import fetch_subreddit_posts

def process_reddit_data(agent=None):
    subreddit = agent.get('controller', None)
    if subreddit:
        try:
            posts = fetch_subreddit_posts(subreddit)
        except Exception as e:
            logging.error(f"Error fetching subreddit posts: {e}:{subreddit}")
            return

        for post in posts:
            post_obj = [{
                'link': f"{post.get('subreddit', '')}-{post.get('name', '')}-reddit-name",
                'title': post.get('title', ''),
                'image_links': [],
                'content': json.dumps({
                    'title': post.get('title', ''),
                    'content': post.get('content', ''),
                    'comments': post.get('comments', [])[:50]
                }),
                'processor': agent.get('id', ''),
                'developer_id': agent.get('author_id', ''),
                'author_id': agent.get('author_id', '')
            }]
            try:
                post_data_to_api(post_obj)
            except Exception as e:
                logging.error(f"Error posting data: {e}")

def data_scraper(scraper_id):
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    if data:
        for scraper_config in data['items']:
            if scraper_config['source'] == "website" and scraper_config['id'] == scraper_id:
                scrape_data(scraper_config)
            elif scraper_config['source'] == "reddit" and scraper_config['id'] == scraper_id:
                process_reddit_data(scraper_config)

def data_to_process_consumer(consumer_running=True):
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='data_to_process_consumer')
    
    while consumer_running:
        method_frame, header_frame, body = channel.basic_get(queue='data_to_process_consumer')
        if method_frame:
            print(f"Received {body}")
            get_data_api(body.decode('utf-8'))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def scraper_consumer(consumer_running=True):
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='scraper_consumer')
    
    while consumer_running:
        method_frame, header_frame, body = channel.basic_get(queue='scraper_consumer')
        if method_frame:
            print(f"Received scraper {body}")
            data_scraper(body.decode('utf-8'))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

if __name__ == '__main__':
    data_to_process_consumer_process = multiprocessing.Process(target=data_to_process_consumer)
    data_to_process_consumer_process.start()

    scraper_consumer_process = multiprocessing.Process(target=scraper_consumer)
    scraper_consumer_process.start()

