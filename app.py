import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import json
from flask import Flask, jsonify
import threading
import os
from datetime import datetime

# Get the value of the environment variable
api_key = os.getenv('GROG_API_KEY')
current_time = datetime.now().strftime("%B %d, %Y at %H:%M %p")

system_prompt = f"""
    You are a news reporter (Denis Kipeles Kemboi) at Ktechs Communication Organization, designed to provide detailed, accurate, and timely news reports. The current date and time is {current_time}. Your goal is to produce engaging and dynamic content that captures the reader's attention. Expand on the given context with comprehensive details, including background information, key facts, human interest elements, and different perspectives. Ensure that your report is well-structured, clear, and adheres to journalistic standards of accuracy and impartiality.

    Use markdown for styling:
    - Use `##` for the main headline
    - Use `###` for section headings
    - Use `####` for sub-section headings
    - Use bullet points or numbered lists for lists
    - Emphasize important points with **bold text**
    - Use quotes for citations and quotes
    - Ensure proper paragraph breaks and formatting for readability

    
    Ensure the content includes:
    - Vivid descriptions that bring the story to life
    - Quotes from experts, officials, or eyewitnesses
    - Insightful analysis and context
    - Human interest elements that add a personal touch

    Your reports should be structured as follows:
    - A headline
    - An introductory paragraph summarizing the key points
    - Detailed sections elaborating on different aspects of the story
    
    You should Mark the title and tags clearly for extraction (do not wrap them with markdown):
    - {{title}}Generated Title{{/title}}
    - {{tags}}tag1, tag2, tag3{{/tags}}
"""

app = Flask(__name__)


def get_sports_content(url: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    stories = []
    for li in soup.find_all('li'):
        title_element = li.find('h1') or li.find('a')
        excerpt_element = li.find('p')
        image_element = li.find('img')
        link = title_element.get('href') if title_element else None

        if link and not link.startswith('http'):
            link = urljoin(url, link)

        story = {
            'title': title_element.get_text() if title_element else li.get_text(),
            'link': link,
            'excerpt': excerpt_element.get_text() if excerpt_element else None,
            'imageLink': image_element.get('src') if image_element else None,
            'sports':True
        }

        if story['excerpt'] and story['imageLink'] and story['link']:
            stories.append(story)

    # Function to get full content from a link
    def get_full_content(link):
        if not link:
            return None

        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')
        content_element = soup.find(class_='the-content') or soup.find('article')
        return content_element.get_text() if content_element else None

    # Add full-content to each story
    for story in stories:
        story['full-content'] = get_full_content(story['link'])

    return stories

def fetch_article_data(url: str, headers: dict):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def extract_star_articles(soup, base_url: str, headers: dict):
    articles = soup.find('div', {'class': 'homepage'}).find('ul').find_all('li', {'class': 'realtimeli'})
    article_info_list = []

    for article in articles:
        link_tag = article.find('a')
        link_href = urljoin(base_url, link_tag['href']) if link_tag else None

        if link_href:
            article_info = {
                'link': link_href,
                'imageLink': None,  # Since there is no image, set it to None
            }

            link_soup = fetch_article_data(link_href, headers)
            if link_soup:
                title_tag = link_soup.find('div', {'class': 'article-header'})
                body_tag = link_soup.find('div', {'class': 'article-body'})
                article_info['title'] = title_tag.text.strip() if title_tag else ''
                article_info['full-content'] = body_tag.text.strip() if body_tag else ''
                article_info_list.append(article_info)

    return article_info_list

def extract_citizen_articles(soup, base_url: str, headers: dict):
    topstories = soup.find_all('div', {'class': ['topstory', 'next-topstory', 'next-top-stories']})
    article_info_list = []

    for topstory in topstories:
        title_element = topstory.find(['h1', 'a'])
        excerpt_element = topstory.find('p')
        image_element = topstory.find('img')

        title = title_element.text.strip() if title_element else topstory.text.strip()
        link = urljoin(base_url, title_element.get('href')) if title_element and title_element.get('href') else None
        excerpt = excerpt_element.text.strip() if excerpt_element else None
        image_link = urljoin(base_url, image_element.get('src')) if image_element and image_element.get('src') else None

        if link:
            link_soup = fetch_article_data(link, headers)
            full_content = None
            if link_soup:
                content_element = link_soup.find('div', {'class': ['the-content', 'article-content']})
                full_content = content_element.text.strip() if content_element else None

            article_info_list.append({
                'title': title,
                'link': link,
                'excerpt': excerpt,
                'imageLink': image_link,
                'full-content': full_content
            })

    return article_info_list

def extract_nation_articles(soup, base_url: str, headers: dict):
    articles = soup.find_all('li')
    unique_links = set()
    article_info_list = []

    for article in articles:
        link_tag = article.find('a')
        img_tag = article.find('img')

        if link_tag and img_tag:
            link_href = urljoin(base_url, link_tag['href'])
            if link_href not in unique_links:
                unique_links.add(link_href)
                img_src = urljoin(base_url, img_tag.get('data-src') or img_tag.get('src'))
                title = article.find('h3').text.strip() if article.find('h3') else ''
                pub_time = article.find('span', class_='date').text.strip() if article.find('span', class_='date') else ''

                link_soup = fetch_article_data(link_href, headers)
                full_content = None
                if link_soup:
                    article_tag = link_soup.find('article')
                    full_content = article_tag.text.strip() if article_tag else None

                if full_content:
                    article_info_list.append({
                        'link': link_href,
                        'imageLink': img_src,
                        'title': title,
                        'publication_time': pub_time,
                        'full-content': full_content
                    })

    return article_info_list

def get_all_articles():
    headers = {'User-Agent': 'Mozilla/5.0'}
    urls = [
        ("https://www.the-star.co.ke/", extract_star_articles),
        ("https://www.citizen.digital/", extract_citizen_articles),
        ("https://nation.africa/kenya/", extract_nation_articles)
    ]

    all_articles = []
    for url, extractor in urls:
        soup = fetch_article_data(url, headers)
        if soup:
            all_articles.extend(extractor(soup, url, headers))
        time.sleep(1)  # Delay to avoid overwhelming the website

    return all_articles
    
headers_to_post = {
    'Content-Type': 'application/json',
    'src': 'vlj7s3cppx8e17n'
}

# Other functions (fetch_article_data, extract_star_articles, extract_citizen_articles, extract_nation_articles, get_all_articles, etc.) remain the same

def process_with_groq_api(article):
    groq_api_key = api_key
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {groq_api_key}"
    }
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

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        content = response.json()['choices'][0]['message']['content']
        title = extract_field(content, 'title')
        tags = extract_field(content, 'tags').split(', ')
        content_text = remove_markers(content)

        payload = {
            'title': title if len(title) > 0 else article.get('title'),
            'developer_id': 'vlj7s3cppx8e17n',
            'content': content_text,
            'sub_menu_list_id': 'b8901yq11qqka1y' if article.get('sports') else 'bt1qckexcqmbust',
            'tags': tags if len(tags) > 1 else ['news','sports','politics']
        }
        api_url = 'https://stories-blog.pockethost.io/api/collections/articles/records'
        try:
            response = requests.post(api_url, json=payload, headers=headers_to_post)
            response.raise_for_status()
            print("Data posted successfully for AI")

        except requests.RequestException as e:
            print(f"Error posting data for AI: {e}")

    else:
        print(f"Error processing with Groq API: {response.status_code} - {response.text}")

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

def post_data_to_api(data, api_url):
    for article in data:
        payload = {
            'data': article,
            'link': article.get('link')
        }
        try:
            response = requests.post(api_url, json=payload, headers=headers_to_post)
            response.raise_for_status()
            print(f"Data posted successfully for link: {article.get('link')}")

            # Stringify the article data and process with Groq API
            process_with_groq_api(article)
        except requests.RequestException as e:
            print(f"Error posting data for link {article.get('link')}: {e}")

def background_task():
    # Fetch all articles from the three sources
    all_articles = get_all_articles()

    # Post the articles to the specified API endpoint
    api_url = "https://full-bit.pockethost.io/api/collections/scrape_data/records"
    post_data_to_api(all_articles, api_url)

def background_task_sports():
    # Fetch all articles from the source
    stories = get_sports_content("https://www.goal.com")

    # Post the articles to the specified API endpoint
    api_url = "https://full-bit.pockethost.io/api/collections/scrape_data/records"
    post_data_to_api(stories, api_url)
    
@app.route('/sports', methods=['GET'])
def scan_sports():
    threading.Thread(target=background_task_sports).start()
    return jsonify({"message": "Sports scraping initialized"}), 202
    
@app.route('/scan', methods=['GET'])
def scan():
    threading.Thread(target=background_task).start()
    return jsonify({"message": "Scraping initialized"}), 202

@app.route('/')
def hello_world():
    return 'Hello, World!'


