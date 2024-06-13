# app.py file
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import json
from flask import Flask, jsonify
import threading
import os
from datetime import datetime

from tasks import add_to_queue, redis_conn, queue_groq, queue_post
 
    

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

def background_task():
    # Fetch all articles from the three sources
    all_articles = get_all_articles()

    # Post the articles to the specified API endpoint
    api_url = "https://full-bit.pockethost.io/api/collections/scrape_data/records"
    add_to_queue(all_articles, api_url,headers_to_post)

def background_task_sports():
    # Fetch all articles from the source
    stories = get_sports_content("https://www.goal.com")

    # Post the articles to the specified API endpoint
    api_url = "https://full-bit.pockethost.io/api/collections/scrape_data/records"
    add_to_queue(stories, api_url,headers_to_post)

@app.route('/sports', methods=['GET'])
def scan_sports():
    job = post_queue.enqueue(background_task_sports)
    return jsonify({"message": "Sports scraping initialized", "job_id": job.get_id()}), 202

@app.route('/scan', methods=['GET'])
def scan():
    job = post_queue.enqueue(background_task)
    return jsonify({"message": "Scraping initialized", "job_id": job.get_id()}), 202

@app.route('/')
def hello_world():
    return 'Hello, World!'

