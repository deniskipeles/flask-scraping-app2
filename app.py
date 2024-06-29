from flask import Flask, jsonify, request
import threading
import logging
import requests
import json
from processor import consumer, scrape_data
from fetcher import fetch_and_cache
from pullpush import fetch_subreddit_posts
from processor import post_data_to_api
from config import flush_keys_containing_pattern, flush_all
import random

app = Flask(__name__)

# Global variable to track consumer status
consumer_running = False

def run_consumer():
    global consumer_running
    try:
        consumer_running = True
        consumer(consumer_running)
    except Exception as e:
        logging.error(f"Error in consumer: {e}")
    finally:
        consumer_running = False

@app.route('/start-consumer')
def start_consumer():
    global consumer_running

    if consumer_running:
        return jsonify({"status": "Consumer is already running"}), 200

    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.start()
    return jsonify({"status": "Consumer starting"}), 200

@app.route('/stop-consumer')
def stop_consumer():
    global consumer_running

    if not consumer_running:
        return jsonify({"status": "Consumer is not running"}), 200

    consumer_running = False
    return jsonify({"status": "Consumer stopping"}), 200

def run_scan_and_consumer():
    global consumer_running

    # Initiate scraping process
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    if data:
        for scraper_config in data['items']:
            if scraper_config['source'] == "website":
                scrape_data(scraper_config)

    # Check if the consumer is already running before starting it
    if not consumer_running:
        run_consumer()

@app.route('/scan')
def scan_and_start_consumer():
    scan_thread = threading.Thread(target=run_scan_and_consumer)
    scan_thread.start()
    return jsonify({"status": "Scan initiated and consumer will start if not already running"}), 200

def fetch_data(url):
    try:
        return fetch_and_cache(url)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return None



def process_reddit_data(data):
    items = data.get('items', [])
    random.shuffle(items)  # shuffle the list of items

    unique_items = []
    sub_menu_list_ids = set()

    for agent in items:
        if agent.get('source') == 'reddit' and agent.get('name') not in sub_menu_list_ids:
            unique_items.append(agent)
            sub_menu_list_ids.add(agent.get('name'))

    for agent in unique_items:
        subreddit = agent.get('controller', None)
        if subreddit:
            
            try:
                posts = fetch_subreddit_posts(subreddit)
            except Exception as e:
                logging.error(f"Error fetching subreddit posts: {e}:{subreddit}")
                continue
            
            for post in posts:
                post_obj = [{
                    'link': post.get('subreddit', '') + '-' + post.get('name', '') + 'reddit-name',
                    'title': post.get('title', ''),
                    'image_links': [],
                    'content': json.dumps({
                        'title': post.get('title', ''),
                        'content': post.get('content', ''),
                        'comments': post.get('comments', [])[:50]
                    }),
                    'processor': agent.get('id', ''),
                    'developer_id': agent.get('author_id', '')
                }]
                try:
                    post_data_to_api(post_obj)
                except Exception as e:
                    logging.error(f"Error posting data: {e}")     


def run_r_data():
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_data(url)
    if data:
        process_reddit_data(data)
    else:
        logging.info("No data available")

@app.route('/rscan')
def r_data():
    r_thread = threading.Thread(target=run_r_data)
    r_thread.start()
    return jsonify({"status": "r data initiated"}), 200

@app.route('/flush-keys', methods=['GET'])
def flush_all_keys():
    flush_all()
    return "Flushed all keys"

@app.route('/flush')
def flush_keys():
    pattern = request.args.get('pattern')
    if pattern is None:
        return 'Error: pattern is required', 400

    flush_keys_containing_pattern(pattern)
    return f"Flushed keys containing the pattern: {pattern}"

@app.route('/')
def hello_world():
    return 'Hello, World!'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)

