## config.py

import os
import redis
import pika
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
CLOUDAMQP_URL = os.getenv('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
HEADERS_TO_POST = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {GROQ_API_KEY}"
}
TEMP_API_URL = 'https://full-bit.pockethost.io/api/collections/scrape_data/records'
REDIS_CACHE_EXPIRATION = 30 * 60  # 30 minutes in seconds

# Initialize Redis
redis_client = redis.Redis.from_url(REDIS_URL)
def flush_keys_containing_pattern(pattern):
    cursor = '0'
    while True:
        (count, keys) = redis_client.scan(cursor, match=pattern)
        if count == 0:
            break
        if keys:  # Check if the keys list is not empty
            for key in keys:
                redis_client.delete(key)
        cursor = keys[-1] if keys else ''  # Set cursor to an empty string if keys is empty
    print(f"Flushed keys containing the pattern: {pattern}")


# RabbitMQ connection
params = pika.URLParameters(CLOUDAMQP_URL)


