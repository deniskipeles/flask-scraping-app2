import threading
import pika
import json
import logging
import time

from config import params
from processor import get_data_api, post_data_to_api, scrape_data
from fetcher import fetch_and_cache,gen_tags
from pullpush import fetch_subreddit_posts


# Example usage:
base_url = "https://stories-blog.pockethost.io"


logging.basicConfig(level=logging.DEBUG)

RETRY_DELAY = 5  # Delay in seconds before retrying a failed connection

def process_reddit_data(agent=None):
    subreddit = agent.get('controller', None)
    if subreddit:
        try:
            new_tags = agent.get("tags", [])
            new_tags.extend(subreddit.get("tags", []))
            tags = get_tags(new_tags, base_url)
            if "url_json_object" in subreddit and "tags" in agent["url_json_object"]:
                subreddit["url_json_object"]["tags"].clear()
                subreddit["url_json_object"]["tags"] = new_tags
            else:
                subreddit["url_json_object"]["tags"] = new_tags
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

def connect_to_rabbitmq(queue_name):
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            logging.debug(f"Connected to RabbitMQ for {queue_name}")
            return connection, channel
        except Exception as e:
            logging.error(f"Error connecting to RabbitMQ: {e}")
            logging.debug(f"Retrying connection in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

def data_to_process_consumer(consumer_running=True):
    connection, channel = connect_to_rabbitmq('data_to_process_consumer')
    
    while consumer_running:
        try:
            method_frame, header_frame, body = channel.basic_get(queue='data_to_process_consumer')
            if method_frame:
                logging.debug(f"Received message: {body}")
                get_data_api(body.decode('utf-8'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logging.debug("Message acknowledged")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            connection, channel = connect_to_rabbitmq('data_to_process_consumer')

def scraper_consumer(consumer_running=True):
    connection, channel = connect_to_rabbitmq('scraper_consumer')
    
    while consumer_running:
        try:
            method_frame, header_frame, body = channel.basic_get(queue='scraper_consumer')
            if method_frame:
                logging.debug(f"Received scraper message: {body}")
                data_scraper(body.decode('utf-8'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                logging.debug("Scraper message acknowledged")
        except Exception as e:
            logging.error(f"Error processing scraper message: {e}")
            connection, channel = connect_to_rabbitmq('scraper_consumer')

data_to_process_consumer_thread = threading.Thread(target=data_to_process_consumer)
scraper_consumer_thread = threading.Thread(target=scraper_consumer)

data_to_process_consumer_thread.start()
scraper_consumer_thread.start()

