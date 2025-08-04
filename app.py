import os
from dotenv import load_dotenv
from newsapi import NewsApiClient
from pymongo import MongoClient
from datetime import datetime, timezone
import requests
import hashlib
import time
import json
import urllib.request

# Load environment variables
load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
MONGO_URL = os.getenv("MONGO_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")

print("Environment variables loaded successfully.")
print(f"NEWS_API_KEY: {NEWS_API_KEY}")
print(f"MONGO_URL: {MONGO_URL}")
print(f"GEMINI_API_KEY: {GEMINI_API_KEY}")
print(f"GNEWS_API_KEY: {GNEWS_API_KEY}")

# Initialize clients
newsapi = NewsApiClient(api_key=NEWS_API_KEY)
client = MongoClient(MONGO_URL)
db = client.get_default_database()

def save_to_mongo(collection_name, data):
    collection = db[collection_name]
    if isinstance(data, list):
        for item in data:
            item['fetched_at'] = datetime.now(timezone.utc)
        if data:
            collection.insert_many(data)
    elif isinstance(data, dict):
        data['fetched_at'] = datetime.now(timezone.utc)
        collection.insert_one(data)

def summarize_with_gemini(text):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent"
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

    summarized_articles = []
    summary_records = []

    # Summarize each article individually
    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        text = f"Title: {title}\nDescription: {description}\nContent: {content}"

        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")

        time.sleep(5)

        if summary:
            fetched_at = datetime.now(timezone.utc)

            full_article = {
                **article,
                'summary': summary,
                'fetched_at': fetched_at
            }
            summarized_articles.append(full_article)
            
            summary_record = {
                'article_title': title,
                'summary': summary,
                'source': 'top_headlines',
                'fetched_at': fetched_at
            }
            summary_records.append(summary_record)
        
    # Save after summarization
    if summarized_articles:
        save_to_mongo("top_headlines", summarized_articles)
    if summary_records:
        save_to_mongo("summarization_data", summary_records)

    print("Top headlines fetched successfully.")

def fetch_everything(query='bitcoin'):
    response = newsapi.get_everything(q=query, language='en', sort_by='publishedAt', page_size=100)
    articles = response.get('articles', [])

    summarized_articles = []
    summary_records = []

    # Summarize each article individually
    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        text = f"Title: {title}\nDescription: {description}\nContent: {content}"

        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(5)

        if summary:
            fetched_at = datetime.now(timezone.utc)

            # Store full article with summary
            full_article = {
                **article,
                'summary': summary,
                'fetched_at': fetched_at
            }
            summarized_articles.append(full_article)

            # Store summary-only record
            summary_record = {
                'article_title': title,
                'summary': summary,
                'source': f'everything:{query}',
                'fetched_at': fetched_at
            }
            summary_records.append(summary_record)

    # Save only after summarization
    if summarized_articles:
        save_to_mongo("everything", summarized_articles)
    if summary_records:
        save_to_mongo("summarization_data", summary_records)

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

def fetch_gnews_top_headlines(category="general", max_results=100):
    """
    Fetch top headlines from GNews API, summarize them, and save to MongoDB.
    """
    url = (
        f"https://gnews.io/api/v4/top-headlines"
        f"?category={category}&lang=en&country=us"
        f"&max={max_results}&apikey={GNEWS_API_KEY}"
    )

    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode("utf-8"))
            articles = data.get("articles", [])

            summarized_articles = []
            summary_records = []

            for article in articles:
                title = article.get('title', '')
                description = article.get('description', '')
                content = article.get('content', '')
                source = article.get('source', {}).get('name')
                text = f"Title: {title}\nDescription: {description}\nContent: {content} \nSource: {source}"

                summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
                time.sleep(5)

                if summary:
                    fetched_at = datetime.now(timezone.utc)

                    # Article + summary
                    full_article = {
                        **article,
                        'summary': summary,
                        'fetched_at': fetched_at
                    }
                    summarized_articles.append(full_article)

                    # Summary metadata
                    summary_record = {
                        'article_title': title,
                        'summary': summary,
                        'source': f'gnews_top_headlines:{category}',
                        'fetched_at': fetched_at
                    }
                    summary_records.append(summary_record)

            if summarized_articles:
                save_to_mongo("gnews_top_headlines", summarized_articles)
                print(f"Saved {len(summarized_articles)} GNews articles to MongoDB with summaries.")
            else:
                print("No articles to summarize or save from GNews.")

            if summary_records:
                save_to_mongo("summarization_data", summary_records)

    except Exception as e:
        print(f"Failed to fetch GNews headlines: {e}")

def fetch_from_mongo(collection_name, query=None):
    """
    Fetch data from MongoDB collection.
    """
    collection = db[collection_name]
    if query:
        return list(collection.find(query))
    else:
        return list(collection.find())

def summarize_newsapi_articles():
    """
    Summarize articles fetched from NewsAPI.
    """
    articles = fetch_from_mongo("top_headlines")
    summaries = []
    
    for article in articles:
        author = article.get('author', '')
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        source = article.get('source', {}).get('name')
        text = f"Title: {title}\nDescription: {description}\nContent: {content} \nAuthor: {author} \nSource: {source}"
        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(1)

        if summary:
            summaries.append({
                'article_title': title,
                "author": author,
                "source": source,
                'summary': summary,
                'source': 'top_headlines',
                'fetched_at': datetime.now(timezone.utc)
            })
    
    if summaries:
        save_to_mongo("summarization_data", summaries)
        print("NewsAPI articles summarized successfully.")
    else:
        print("No summaries generated.")

def summarize_everything_articles():
    """
    Summarize articles fetched from NewsAPI's everything endpoint.
    """
    articles = fetch_from_mongo("everything")
    summaries = []

    for article in articles:
        author = article.get('author', '')
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        source = article.get('source', {}).get('name')
        text = f"Title: {title}\nDescription: {description}\nContent: {content} \nAuthor: {author} \nSource: {source}"
        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(1)

        if summary:
            summaries.append({
                'article_title': title,
                "author": author,
                "source": source,
                'summary': summary,
                'source': f'everything',
                'fetched_at': datetime.now(timezone.utc)
            })

    if summaries:
        save_to_mongo("summarization_data", summaries)
        print(f"Articles related to summarized successfully.")
    else:
        print("No summaries generated.")

def summarize_gnews_articles():
    """
    Summarize articles fetched from GNews.
    """
    articles = fetch_from_mongo("gnews_top_headlines")
    summaries = []

    for article in articles:
        title = article.get('title', '')
        description = article.get('description', '')
        content = article.get('content', '')
        source = article.get('source', {}).get('name')
        text = f"Title: {title}\nDescription: {description}\nContent: {content} \nSource: {source}"
        summary = summarize_with_gemini(f"Summarize the following news article:\n{text}")
        time.sleep(1)

        if summary:
            summaries.append({
                'article_title': title,
                'source': source,
                'summary': summary,
                'source': 'gnews_top_headlines',
                'fetched_at': datetime.now(timezone.utc)
            })

    if summaries:
        save_to_mongo("summarization_data", summaries)
        print("GNews articles summarized successfully.")
    else:
        print("No summaries generated.")

if __name__ == "__main__":
    # clear_database()

    # Fetch news data from various sources
    fetch_top_headlines()
    fetch_everything("Artificial Intelligence AND Machine Learning AND Deep Learning AND Neural Networks AND Big Data")
    fetch_sources()
    fetch_gnews_top_headlines(category="ArtificialIntelligence", max_results=100)

    # # Summarize articles from different sources
    # summarize_newsapi_articles()
    # summarize_everything_articles()
    # summarize_gnews_articles()

    print("News data fetched and stored in MongoDB successfully.")