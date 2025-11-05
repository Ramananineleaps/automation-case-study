import pandas as pd
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import time
import urllib3
import csv
from tabulate import tabulate
import argparse

# Set pandas display options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.max_colwidth', 100)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_internet_connection():
    """Check internet connection and proxy settings"""
    try:
        response = requests.get("https://api.github.com", timeout=5, verify=False)
        return response.status_code == 200
    except:
        return False

def format_table(df):
    """Format DataFrame for better display"""
    # Create a copy to avoid modifying the original
    display_df = df.copy()
    
    # Truncate long text
    display_df['title'] = display_df['title'].str.slice(0, 50) + '...'
    display_df['url'] = display_df['url'].str.slice(0, 40) + '...'
    
    # Convert to table format
    table = tabulate(
        display_df,
        headers='keys',
        tablefmt='grid',
        showindex=False,
        numalign='left',
        stralign='left'
    )
    return table

def extract_articles(url="https://news.ycombinator.com", max_retries=3):
    """Extract articles from Hacker News"""
    if not check_internet_connection():
        print("Network Error: Please check your connection and proxy settings")
        return None

    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            }
            
            print(f"\nAttempt {attempt + 1}/{max_retries}: Fetching articles...")
            
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            structured_articles = {
                'title': [],
                'author': [],
                'pub_date': [],
                'scraped_at': [],
                'url': []
            }
            
            for item in soup.select('.athing'):
                try:
                    title_element = item.select_one('.titleline > a')
                    if title_element:
                        title = title_element.text.strip()
                        link = title_element['href']
                        
                        subtext = item.find_next_sibling('tr')
                        time_element = subtext.select_one('.age') if subtext else None
                        author_element = subtext.select_one('.hnuser') if subtext else None
                        
                        structured_articles['title'].append(title)
                        structured_articles['author'].append(author_element.text if author_element else 'Unknown')
                        structured_articles['pub_date'].append(time_element['title'] if time_element else None)
                        structured_articles['scraped_at'].append(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                        structured_articles['url'].append(link)
                        
                except Exception as e:
                    print(f"Error parsing article: {str(e)}")
                    continue

            if any(structured_articles.values()):
                os.makedirs('data', exist_ok=True)
                df = pd.DataFrame(structured_articles)
                
                # Reorder columns
                column_order = ['title', 'author', 'pub_date', 'scraped_at', 'url']
                df = df[column_order]
                
                # Save to CSV
                df.to_csv("data/articles_raw.csv", 
                         index=False,
                         encoding='utf-8',
                         quoting=csv.QUOTE_ALL)
                
                return df
            else:
                print("No articles found.")
                return None

        except requests.RequestException as e:
            print(f"Network error on attempt {attempt + 1}: {str(e)}")
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {str(e)}")
        
        if attempt < max_retries - 1:
            print(f"Retrying in 5 seconds...")
            time.sleep(5)

    print("Error: All retry attempts failed")
    return None


def view_articles(df=None):
    """Display articles in a formatted table"""
    try:
        if df is None:
            if not os.path.exists('data/articles_raw.csv'):
                print("No articles found. Please extract articles first.")
                return
            df = pd.read_csv('data/articles_raw.csv')
        
        # Set display options for better table formatting
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.max_colwidth', 40)  # Limit column width
        
        # Format the table with proper alignment and borders
        table = tabulate(
            df,
            headers=['Title', 'Author', 'Published Date', 'Scraped At', 'URL'],
            tablefmt='fancy_grid',  # Uses Unicode box-drawing characters
            showindex=False,
            maxcolwidths=[40, 20, 25, 25, 40],  # Control column widths
            numalign='left',
            stralign='left'
        )
        
        print("\nArticles Database:")
        print("=" * 100)
        print(table)  # Print the formatted table
        print("=" * 100)
        print(f"\nTotal articles: {len(df)}")
        
    except Exception as e:
        print(f"Error displaying articles: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='News Article Scraper and Viewer')
    parser.add_argument('--extract-only', action='store_true', help='Only extract articles without viewing')
    parser.add_argument('--view-only', action='store_true', help='Only view existing articles')
    args = parser.parse_args()

    if args.view_only:
        view_articles()
    elif args.extract_only:
        df = extract_articles()
        if df is not None:
            print(f"\nExtracted {len(df)} articles to data/articles_raw.csv")
    else:
        # Default: extract and view
        df = extract_articles()
        if df is not None:
            view_articles(df)

if __name__ == "__main__":
    main()