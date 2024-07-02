
from config import flush_keys_containing_pattern, flush_all
from flask import Flask, request
import logging
from processor import producer
from fetcher import fetch_and_cache
from consumer import scraper_consumer_thread,data_to_process_consumer_thread

app = Flask(__name__)

def agents():
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    if data:
        for agent in data['items']:
            producer(agent['id'])

@app.route('/flush-keys', methods=['GET'])
def flush_all_keys():
    flush_all()
    return "Flushed all keys"

@app.route('/flush', methods=['GET'])
def flush_keys():
    pattern = request.args.get('pattern')
    if not pattern:
        return 'Error: pattern is required', 400

    flush_keys_containing_pattern(pattern)
    return f"Flushed keys containing the pattern: {pattern}"

@app.route('/scan', methods=['GET'])
def scan():
    agents()
    return 'Agents initiated'

@app.route('/')
def hello_world():
    return 'Hello, World!'

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Starting Flask app and consumers")

    try:
        data_to_process_consumer_thread.start()
        logging.debug("Started data_to_process_consumer_thread")
    except Exception as e:
        logging.error(f"Error starting data_to_process_consumer_thread: {e}")

    try:
        scraper_consumer_thread.start()
        logging.debug("Started scraper_consumer_thread")
    except Exception as e:
        logging.error(f"Error starting scraper_consumer_thread: {e}")

    app.run(debug=True)
