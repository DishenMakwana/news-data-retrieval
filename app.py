import os
from dotenv import load_dotenv
from newsapi import NewsApiClient
from pymongo import MongoClient
from datetime import datetime
import requests
import hashlib
import time

# Load environment variables
load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
MONGO_URL = os.getenv("MONGO_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

print("Environment variables loaded successfully.")
print(f"NEWS_API_KEY: {NEWS_API_KEY}")
print(f"MONGO_URL: {MONGO_URL}")
print(f"GEMINI_API_KEY: {GEMINI_API_KEY}")

# Initialize clients
newsapi = NewsApiClient(api_key=NEWS_API_KEY)
client = MongoClient(MONGO_URL)
db = client.get_default_database()

def save_to_mongo(collection_name, data):
    collection = db[collection_name]
    if isinstance(data, list):
        for item in data:
            item['fetched_at'] = datetime.utcnow()
        if data:
            collection.insert_many(data)
    elif isinstance(data, dict):
        data['fetched_at'] = datetime.utcnow()
        collection.insert_one(data)

def summarize_with_gemini(text):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
    headers = {
        'Content-Type': 'application/json',
        'X-goog-api-key': GEMINI_API_KEY
    }
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": text}
                ]
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        summary = result['candidates'][0]['content']['parts'][0]['text']

        return summary
    except Exception as e:
        print(f"Gemini summarization failed: {e}")
        return None

def group_articles_by_topic(articles):
    # Group by title similarity (hash of first 10 words)
    topic_map = {}
    for article in articles:
        title = article.get('title', '')
        topic_key = hashlib.md5(' '.join(title.split()[:10]).encode()).hexdigest()
        topic_map.setdefault(topic_key, []).append(article)
    return topic_map

def fetch_top_headlines():
    response = newsapi.get_top_headlines(language='en', page_size=100)
    articles = response.get('articles', [])
    save_to_mongo("top_headlines", articles)

    # Summarize each article individually
    summaries = []
    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        text = f"Title: {title}\nDescription: {description}\nContent: {content}"
        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(1)
        if summary:
            summaries.append({
                'article_title': title,
                'summary': summary,
                'source': 'top_headlines',
                'fetched_at': datetime.utcnow()
            })
    if summaries:
        save_to_mongo("summarization_data", summaries)

    print("Top headlines fetched successfully.")

def fetch_everything(query='bitcoin'):
    response = newsapi.get_everything(q=query, language='en', sort_by='publishedAt', page_size=100)
    articles = response.get('articles', [])
    save_to_mongo("everything", articles)

    # Summarize each article individually
    summaries = []
    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        text = f"Title: {title}\nDescription: {description}\nContent: {content}"
        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(1)
        if summary:
            summaries.append({
                'article_title': title,
                'summary': summary,
                'source': f'everything:{query}',
                'fetched_at': datetime.utcnow()
            })
    if summaries:
        save_to_mongo("summarization_data", summaries)

    print(f"Articles related to '{query}' fetched successfully.")

def fetch_sources():
    response = newsapi.get_sources(language='en')
    sources = response.get('sources', [])
    save_to_mongo("sources", sources)

    print("News sources fetched successfully.")

def clear_database():
    collections = db.list_collection_names()
    for collection in collections:
        db[collection].delete_many({})

    print("Database cleared successfully.")

if __name__ == "__main__":
    clear_database()
    fetch_top_headlines()
    fetch_everything("artificial intelligence")
    fetch_sources()
    print("News data fetched and stored in MongoDB successfully.")
