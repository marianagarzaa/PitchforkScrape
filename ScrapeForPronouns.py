import requests
import csv
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://pitchfork.com"
REVIEW_PATH = "/reviews/albums/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
session = requests.Session()

def fetch_reviews(page):
    """Fetches album review links from a given page."""
    current_link = f"{BASE_URL}{REVIEW_PATH}?page={page}"
    try:
        response = session.get(current_link, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if REVIEW_PATH in href:
                full_url = BASE_URL + href if href.startswith('/') else href
                links.append(full_url)

        return links
    except requests.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return []

def fetch_review_details(url):
    """Extracts album review details from a given review URL."""
    try:
        response = session.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract article text (first 500 characters for readability)
        article_paragraphs = soup.find_all('p')
        article_text = " ".join(p.get_text(strip=True) for p in article_paragraphs) if article_paragraphs else "Unknown"

        return [article_text]

    except requests.RequestException as e:
        print(f"Error fetching review {url}: {e}")
        return None

# Step 1: Fetch all review URLs concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    review_links = list(set(fetch_reviews(1)))

# Step 2: Fetch review details concurrently from around 100 articles
with ThreadPoolExecutor(max_workers=10) as executor:
    review_data = list(filter(None, executor.map(fetch_review_details, review_links)))

# Step 3: count pronouns in articles
for article in review_data:
    text = article[0]


print(f"\ngot text from all {len(review_links)} articles!")