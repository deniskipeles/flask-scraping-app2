import multiprocessing
import pika
import json

from config import HEADERS_TO_POST, TEMP_API_URL, params, redis_client
from processor import get_data_api

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
            get_data_api(body.decode('utf-8'))
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            




if __name__ == '__main__':
    reddit_consumer_process = multiprocessing.Process(target=data_to_process_consumer)
    reddit_consumer_process.start()
  
    scraper_consumer_process = multiprocessing.Process(target=scraper_consumer)
    scraper_consumer_process.start()
