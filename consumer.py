import threading
import pika
import json
import logging

from config import params
from processor import get_data_api, post_data_to_api, scrape_data
from fetcher import fetch_and_cache
from pullpush import fetch_subreddit_posts

logging.basicConfig(level=logging.DEBUG)

def process_reddit_data(agent=None):
    subreddit = agent.get('controller', None)
    if subreddit:
        try:
            posts = fetch_subreddit_posts(subreddit)
            logging.debug(f"Fetched posts for subreddit {subreddit}")
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
                logging.debug(f"Posted data to API for post: {post.get('title', '')}")
            except Exception as e:
                logging.error(f"Error posting data: {e}")

def data_scraper(scraper_id):
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    logging.debug(f"Fetched scraper configuration data: {data}")

    if data:
        for scraper_config in data['items']:
            if scraper_config['source'] == "website" and scraper_config['id'] == scraper_id:
                logging.debug(f"Starting website scraper for ID: {scraper_id}")
                scrape_data(scraper_config)
            elif scraper_config['source'] == "reddit" and scraper_config['id'] == scraper_id:
                logging.debug(f"Starting Reddit scraper for ID: {scraper_id}")
                process_reddit_data(scraper_config)

def data_to_process_consumer(consumer_running=True):
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue='data_to_process_consumer')
        logging.debug("Connected to RabbitMQ for data_to_process_consumer")
    except Exception as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        return

    while consumer_running:
        method_frame, header_frame, body = channel.basic_get(queue='data_to_process_consumer')
        if method_frame:
            logging.debug(f"Received message: {body}")
            try:
                get_data_api(body.decode('utf-8'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logging.debug("Message acknowledged")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

def scraper_consumer(consumer_running=True):
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue='scraper_consumer')
        logging.debug("Connected to RabbitMQ for scraper_consumer")
    except Exception as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        return

    while consumer_running:
        method_frame, header_frame, body = channel.basic_get(queue='scraper_consumer')
        if method_frame:
            logging.debug(f"Received scraper message: {body}")
            try:
                data_scraper(body.decode('utf-8'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logging.debug("Scraper message acknowledged")
            except Exception as e:
                logging.error(f"Error processing scraper message: {e}")

data_to_process_consumer_thread = threading.Thread(target=data_to_process_consumer)
scraper_consumer_thread = threading.Thread(target=scraper_consumer)

