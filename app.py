from flask import Flask, request
import logging
from processor import producer, flush_all, flush_keys_containing_pattern
from fetcher import fetch_and_cache
import consumer

app = Flask(__name__)

def agents():
    url = "https://stories-blog.pockethost.io/api/collections/scraper_controllers/records"
    data = fetch_and_cache(url)
    if data:
        for agent in data['items']:
            id = agent['id']
            producer([id])

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
    logging.basicConfig(level=logging.INFO)
    consumer.scraper_consumer_process.start()
    consumer.data_to_process_consumer_process.start()
    app.run(debug=True)
