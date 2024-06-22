from flask import Flask, jsonify
import threading
import logging
import requests
from processor import consumer, scrape_data
from fetcher import fetch_and_cache
from pullpush import fetch_subreddit_posts
from processor import post_data_to_api


app = Flask(__name__)

# Global variable to track consumer status
consumer_running = False

def run_consumer():
    global consumer_running
    try:
        consumer_running = True
        consumer(consumer_running)
    except Exception as e:
        print(f"Error in consumer: {e}")
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
    global consumer_running
    
    scan_thread = threading.Thread(target=run_scan_and_consumer)
    scan_thread.start()
    return jsonify({"status": "Scan initiated and consumer will start if not already running"}), 200

def run_r_data():
    # Initiate scraping process
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    
    try:
        data = fetch_and_cache(url)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    
    if data:
        for agent in data.get('items', []):  
            if agent.get('source') == 'reddit':  
                subreddit = agent.get('controller', {}).get('subreddit')
                if subreddit:
                    try:
                        posts = fetch_subreddit_posts(subreddit)
                    except Exception as e:
                        print(f"Error fetching subreddit posts: {e}")
                        continue
                    
                    for post in posts:
                        obj = {
                            'link': post.get('name', '') + 'reddit-name',  
                            'title': post.get('title', ''),  
                            'image_links': [],
                            'content': post.get('selftext', '') + (json.dumps({'comments': post.get('comments', [])})),
                            'processor': agent.get('id', ''),  
                            'developer_id': agent.get('author_id', '')  
                        }
                        try:
                            post_data_to_api(obj)
                        except Exception as e:
                            print(f"Error posting data: {e}")
    else:
        print("No data available")

            
@app.route('/rscan')
def r_data():
    r_thread = threading.Thread(target=run_r_data)
    r_thread.start()
    return jsonify({"status": "r data initiated"}), 200


@app.route('/')
def hello_world():
    return 'Hello, World!'

