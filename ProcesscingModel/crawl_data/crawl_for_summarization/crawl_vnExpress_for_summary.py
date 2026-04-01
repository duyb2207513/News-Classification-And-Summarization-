import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
from urllib.parse import urlparse, urlunparse
import csv


def get_category_ids():
    """
    Scrape unique category IDs from the VnExpress homepage's ul with class 'parent'.

    Returns:
        list: List of unique category IDs from li tags with data-id attribute.
    """
    url = "https://vnexpress.net"
    category_ids_set = set()

    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        parent_ul = soup.find('ul', class_='parent')

        if not parent_ul:
            print("Error: Could not find <ul class='parent'> on homepage")
            return []

        for li in parent_ul.find_all('li'):
            data_id = li.get('data-id')
            if data_id:
                category_ids_set.add(data_id)

        print(f"Found {len(category_ids_set)} unique category IDs: {list(category_ids_set)}")
        return list(category_ids_set)

    except requests.RequestException as e:
        print(f"Error fetching category IDs from {url}: {e}")
        return []

def scrape_article_content(url):
    """
    Scrape title and content from a VnExpress article page. Content is concatenated from all <p> tags with class 'Normal'.

    Args:
        url (str): URL of the article page.

    Returns:
        dict: Dictionary containing title, full_text, and URL, or None if scraping fails.
    """
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Get article title
        title_tag = soup.find('h1', class_='title-detail')
        title = title_tag.get_text(strip=True) if title_tag else "No title found"

        # Get content from all <p> tags with class 'Normal'
        normal_tags = soup.find_all('p', class_='Normal')
        full_text = ' '.join(tag.get_text(strip=True) for tag in normal_tags) if normal_tags else "No content found"

        return {
            'url': url,
            'title': title,
            'full_text': full_text
        }

    except requests.RequestException as e:
        print(f"Error fetching article {url}: {e}")
        return None

def scrape_vnexpress_articles(start_date, end_date, output_file="vnexpress_articles.csv"):
    """
    Scrape article URLs, summaries, titles, and content from VnExpress for all categories
    based on date range. Content is concatenated from all <p> tags with class 'Normal'.
    Saves data to a CSV file with columns: url, title, summary, full_text.

    Args:
        start_date (datetime): Start date for scraping.
        end_date (datetime): End date for scraping.
        output_file (str): File to save the scraped data (CSV format).
        Returns:
        list: List of dictionaries containing unique article URLs, summaries, titles, and full_text.
    """
    base_url = "https://vnexpress.net/category/day/cateid/{}/fromdate/{}/todate/{}/allcate/{}"
    articles_data = []
    category_ids = get_category_ids()

    if not category_ids:
        print("No category IDs found, exiting.")
        return []

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    for category_id in category_ids:
        print(f"Scraping category ID: {category_id}")
        current_date = start_date

        while current_date <= end_date:
            day_start = int(current_date.timestamp())
            day_end = int((current_date + timedelta(days=1)).timestamp()) - 1
            url = base_url.format(category_id, day_start, day_end, category_id)

            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')
                article_items = soup.find_all('p', class_='description')

                if not article_items:
                    print(f"No articles found for category {category_id} on {current_date.strftime('%Y-%m-%d')}")

                for item in article_items:
                    link = item.find('a')
                    if link and link.get('href'):
                        href = link['href']
                        if href.startswith('https://vnexpress.net/') and href.endswith('.html'):
                            parsed_url = urlparse(href)
                            clean_url = urlunparse(parsed_url._replace(fragment=''))
                            summary = link.get_text(strip=True) if link else "No summary found"
                            articles_data.append({
                                'url': clean_url,
                                'summary': summary
                            })

                print(f"Scraped {len(article_items)} article links from {current_date.strftime('%Y-%m-%d')} for category {category_id}")

            except requests.RequestException as e:
                print(f"Error fetching {url}: {e}")

            current_date += timedelta(days=1)
            time.sleep(1)

    # Remove duplicate URLs
    unique_articles = []
    seen_urls = set()
    for article in articles_data:
        if article['url'] not in seen_urls:
            seen_urls.add(article['url'])
            unique_articles.append(article)

    # Scrape content from each unique article URL
    final_articles = []
    for article in unique_articles:
        print(f"Scraping content from {article['url']}")
        article_content = scrape_article_content(article['url'])
        if article_content:
            article_content['summary'] = article['summary']
            final_articles.append(article_content)
        time.sleep(1)

    # Save to CSV file
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'title', 'summary', 'full_text'])
            writer.writeheader()
            for article in final_articles:
                writer.writerow({
                    'url': article['url'],
                    'title': article['title'],
                    'summary': article['summary'],
                    'full_text': article['full_text']
                })
        print(f"Saved {len(final_articles)} articles to {output_file}")
    except Exception as e:
        print(f"Error saving to {output_file}: {e}")

    return final_articles

# Example usage
if __name__ == "__main__":
    # Use past dates to ensure articles exist
    start = datetime(2025, 6, 21)
    end = datetime(2025, 6, 30)

    articles_data = scrape_vnexpress_articles(start, end)
    print(f"Total unique articles scraped: {len(articles_data)}")
    for article in articles_data:
        print(f"URL: {article['url']}")
        print(f"Summary: {article['summary']}")
        print(f"Title: {article['title']}")
        print(f"Full Text: {article['full_text'][:100]}...")
        print("-" * 50)