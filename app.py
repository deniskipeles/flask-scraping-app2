
from flask import Flask, jsonify
import threading
import time
import logging
from processor import consumer

app = Flask(__name__)

# Global variable to track consumer status
consumer_running = False
consumer_thread = None

@app.route('/start-consumer', methods=['POST'])
def start_consumer():
    global consumer_running, consumer_thread
    
    if consumer_running:
        return jsonify({"status": "Consumer is already running"}), 200

    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.start()
    consumer_running = True

    return jsonify({"status": "Consumer started"}), 200

def run_consumer():
    global consumer_running

    try:
        consumer()
    except Exception as e:
        logging.error(f"Error in consumer: {e}")
    finally:
        consumer_running = False

@app.route('/stop-consumer', methods=['POST'])
def stop_consumer():
    global consumer_running, consumer_thread

    if not consumer_running:
        return jsonify({"status": "Consumer is not running"}), 200

    consumer_running = False
    if consumer_thread is not None:
        consumer_thread.join()
        consumer_thread = None

    return jsonify({"status": "Consumer stopped"}), 200
    
@app.route('/')
def hello_world():
    return 'Hello, World!'

