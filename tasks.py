import requests
import time
from rq import Queue
import redis
from requests.exceptions import RequestException
import os
import datetime
import json

# Redis connection URL
redis_url = 'redis://red-cplhati1hbls73ef82gg:6379'

# Establish a connection to Redis
redis_conn = redis.from_url(redis_url)

# Define two queues: one for Groq processing and one for posting data
queue_groq = Queue('groq', connection=redis_conn)
queue_post = Queue('post', connection=redis_conn)

def process_with_groq_api(article, retries=5):
    api_key = os.getenv('GROQ_API_KEY')  # Replace with your actual API key
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    system_prompt = f"""
        You are a news reporter (Denis Kipeles Kemboi) at Ktechs media department designed to provide detailed, accurate, and timely news reports. The current date and time is {datetime.datetime.now().isoformat()}. Your goal is to produce engaging and dynamic content that captures the reader's attention. Expand on the given context with comprehensive details, including background information, key facts, human interest elements, and different perspectives. Ensure that your report is well-structured, clear, and adheres to journalistic standards of accuracy and impartiality.

        Use markdown for styling:
        - Use `##` for the main headline
        - Use `###` for section headings
        - Use `####` for sub-section headings
        - Use bullet points or numbered lists for lists
        - Emphasize important points with **bold text**
        - Use quotes for citations and quotes
        - Ensure proper paragraph breaks and formatting for readability

        Mark the title and tags clearly for extraction don't wrap with markdown the title and tags:
        - {{title}}Generated Title{{/title}}
        - {{tags}}tag1, tag2, tag3{{/tags}}

        Make sure to include in content:
        - Vivid descriptions that bring the story to life
        - Quotes from experts, officials, or eyewitnesses
        - Insightful analysis and context
        - Human interest elements that add a personal touch

        Your reports should be formatted with a headline, an introductory paragraph summarizing the key points, followed by detailed sections elaborating on different aspects of the story.
    """
    article_data_str = json.dumps(article, indent=2)
    data = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": article_data_str}
        ],
        "model": "mixtral-8x7b-32768",
        "temperature": 1,
        "max_tokens": 1024,
        "top_p": 1,
        "stream": False,
        "stop": None
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        content = response.json()['choices'][0]['message']['content']
        
        # Post processed data to your server
        payload = {
            'title': extract_field(content, 'title'),
            'developer_id': 'vlj7s3cppx8e17n',
            'content': remove_markers(content),
            'sub_menu_list_id': 'bt1qckexcqmbust',
            'tags': extract_field(content, 'tags').split(',')
        }
        api_url = 'https://stories-blog.pockethost.io/api/collections/articles/records'
        response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        print("Data posted successfully for AI processed content")

    except RequestException as e:
        print(f"Error processing with Groq API: {e}")
        if retries > 0:
            print("Retrying in 30 seconds...")
            time.sleep(30)
            queue_groq.enqueue(process_with_groq_api, article, retries - 1)
        else:
            print("Max retries reached for Groq API. Skipping this task.")

def post_data_to_api(article, api_url, headers_to_post):
    payload = {
        'data': article,
        'link': article.get('link')
    }
    try:
        response = requests.post(api_url, json=payload, headers=headers_to_post)
        response.raise_for_status()
        print(f"Data posted successfully for link: {article.get('link')}")
    except RequestException as e:
        print(f"Error posting data for link {article.get('link')}: {e}")
        print("Retrying in 3 seconds...")
        time.sleep(3)
        queue_post.enqueue(post_data_to_api, article, api_url, headers_to_post)

def add_to_queue(data, api_url, headers_to_post):
    for article in data:
        queue_groq.enqueue(process_with_groq_api, article)
        queue_post.enqueue(post_data_to_api, article, api_url, headers_to_post)

def extract_field(content, field):
    start_marker = f'{{{field}}}'
    end_marker = f'{{/{field}}}'
    start_index = content.find(start_marker) + len(start_marker)
    end_index = content.find(end_marker)
    title = content[start_index:end_index].strip() if start_index < end_index else ''
    return title.replace('*','')

def remove_markers(content):
    markers = ['title', 'tags']
    for marker in markers:
        start_marker = f'{{{marker}}}'
        end_marker = f'{{/{marker}}}'
        start_index = content.find(start_marker)
        end_index = content.find(end_marker) + len(end_marker)
        content = content.replace(content[start_index:end_index], '')
    return content

