from config import flush_keys_containing_pattern, flush_all
from flask import Flask, request
import logging
from processor import producer
from fetcher import fetch_and_cache
import consumer
import threading

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Flag to ensure consumers are started only once
consumers_started = False

def agents():
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    if data:
        for agent in data['items']:
            producer([agent['id']], 'scraper_consumer')

def run_agents_in_background():
    agents_thread = threading.Thread(target=agents)
    agents_thread.start()
    logger.debug("Agents processing started in background")

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
    run_agents_in_background()
    return 'Agents processing started'

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.before_request
def start_consumers():
    global consumers_started
    if not consumers_started:
        logger.info("Starting consumers before the first request")
        
        consumer_threads = [
            threading.Thread(target=consumer.data_to_process_consumer, name="DataToProcessConsumerThread"),
            threading.Thread(target=consumer.scraper_consumer, name="ScraperConsumerThread")
        ]
        
        for thread in consumer_threads:
            thread.start()
            logger.debug(f"Started {thread.name}")
        
        consumers_started = True

if __name__ == '__main__':
    logger.info("Starting Flask app")
    app.run(debug=True)

