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

        # Extract album title
        album_title_tag = soup.find('h1')
        album_title = album_title_tag.get_text(strip=True) if album_title_tag else "Unknown"

        # Extract artist (checking multiple possible tag structures)
        artist_tag = soup.select_one("ul.SplitScreenContentHeaderArtistWrapper-fiSZLT.iWNOTb")
        artist = artist_tag.get_text(strip=True) if artist_tag else "Unknown"

        # Extract genres
        genre_tags = soup.find_all('a', href=True)
        genres = soup.select_one("p.BaseWrap-sc-gjQpdd.BaseText-ewhhUZ.InfoSliceValue-tfmqg.iUEiRd.hUQWfW.fkSlPp")
        genres = genres.get_text(strip=True) if genres else "Unknown"

        # Extract author
        author_tag = next((a for a in genre_tags if "/staff/" in a["href"]), None)
        author = author_tag.get_text(strip=True) if author_tag else "Unknown"

        # Extract year
        year_tag = soup.find('time')
        year = year_tag.get_text(strip=True) if year_tag else "Unknown"

        # Extract article text (first 500 characters for readability)
        article_paragraphs = soup.find_all('p')
        article_text = " ".join(p.get_text(strip=True) for p in article_paragraphs)[:500] if article_paragraphs else "Unknown"

        # Extract artist links
        artist_links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if "/artists/" in href:
                full_url = BASE_URL + href if href.startswith('/') else href
                artist_links.add(full_url)
        artist_links = ", ".join(artist_links)

        # Debugging: Print details for verification
        print(f"\nExtracted: {album_title} | {artist} | {genres} | {author} | {year}")

        return [url, album_title, artist, genres, author, year, article_text, artist_links]

    except requests.RequestException as e:
        print(f"Error fetching review {url}: {e}")
        return None

# Step 1: Fetch all review URLs concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    review_links = list(set(link for links in executor.map(fetch_reviews, range(75)) for link in links))

# Step 2: Fetch review details concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    review_data = list(filter(None, executor.map(fetch_review_details, review_links)))

# Step 3: Save data to CSV
csv_filename = "pitchfork_reviews.csv"
with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["URL", "Album", "Artist", "Genres", "Author", "Year", "Article Excerpt", "Artist Links"])
    writer.writerows(review_data)

print(f"\n✅ Scraping complete! Data saved to {csv_filename}")