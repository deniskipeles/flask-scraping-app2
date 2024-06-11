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
    You are a news reporter(Denis Kipeles Kemboi) at Ktechs communication organization designed to provide detailed, accurate, and timely news reports. The current date and time is {current_time}. Your goal is to produce engaging and dynamic content that captures the reader's attention. Expand on the given context with comprehensive details, including background information, key facts, human interest elements, and different perspectives. Ensure that your report is well-structured, clear, and adheres to journalistic standards of accuracy and impartiality.

    Format the report with markdown for proper styling:
    - Use `##` for the main headline
    - Use `###` for section headings
    - Use `####` for sub-section headings
    - Use bullet points or numbered lists for lists
    - Emphasize important points with bold text
    - if image links are provided add at the top after the heading

    Make sure to include:
    - Vivid descriptions that bring the story to life
    - Quotes from experts, officials, or eyewitnesses
    - Insightful analysis and context
    - Human interest elements that add a personal touch

    Your reports should be formatted with a headline, an introductory paragraph summarizing the key points, followed by detailed sections elaborating on different aspects of the story.
"""

app = Flask(__name__)

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
        "model": "llama3-8b-8192",
        "temperature": 1,
        "max_tokens": 1024,
        "top_p": 1,
        "stream": False,
        "stop": None
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        content = response.json()['choices'][0]['message']['content']
        payload = {
            'title': article.get('title')[:150] if article.get('title') else content[:100],
            'developer_id': 'vlj7s3cppx8e17n',
            'content': content,
            'sub_menu_list_id': 'bt1qckexcqmbust',
            'tags': ['news','sports','politics']
        }
        api_url = 'https://stories-blog.pockethost.io/api/collections/articles/records'
        try:
            response = requests.post(api_url, json=payload, headers=headers_to_post)
            response.raise_for_status()
            print("Data posted successfully for ai")
            
        except requests.RequestException as e:
            print(f"Error posting data for ai: {e}")

    else:
        print(f"Error processing with Groq API: {response.status_code} - {response.text}")

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

@app.route('/scan', methods=['GET'])
def scan():
    threading.Thread(target=background_task).start()
    return jsonify({"message": "Scraping initialized"}), 202

@app.route('/')
def hello_world():
    return 'Hello, World!'
