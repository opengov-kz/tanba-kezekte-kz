import aiohttp
import asyncio
import pandas as pd
import random
import time
import logging
from datetime import datetime 
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(filename="errors.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

# Set up headers to mimic real user behavior
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

BASE_URL = "https://tanba.kezekte.kz/ru/reestr-tanba-public/animal/list?p="
BATCH_SIZE = 20  # Number of pages to process in one batch

parsing_started = datetime.now() 
print("Parsing started at: ", parsing_started)
logging.info("Started parsing", parsing_started)

async def fetch_page(session, url, page, retries=3):
    """Fetch a single page with retry logic."""
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status in [429, 500, 503]:  # Rate limited or server issue
                    wait_time = random.uniform(5, 15)
                    logging.warning(f"Rate limited on page {page}. Retrying in {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                elif attempt == retries - 1:  # If it's the last retry attempt
                    logging.warning(f"Final attempt failed for page {page}, waiting 30 seconds before skipping.")
                    await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Error fetching page {page} (Attempt {attempt+1}/{retries}): {e}")
        await asyncio.sleep(random.uniform(2, 10))  # Random delay before retry

    logging.error(f"Failed to fetch page {page} after {retries} attempts.")
    return None



async def get_total_pages(session):
    """Extracts the total number of pages dynamically."""
    html = await fetch_page(session, BASE_URL + "1", 1)
    if not html:
        logging.error("Failed to fetch first page. Cannot determine total pages.")
        return 1

    soup = BeautifulSoup(html, "html.parser")
    pagination = soup.find("ul", class_="pagination")

    if not pagination:
        logging.error("Pagination not found. Defaulting to 1 page.")
        return 1

    try:
        last_page_link = pagination.find_all("a", class_="page-link")[-2]
        total_pages = int(last_page_link.text.strip())
        print(f"✅ Found total pages: {total_pages}")  # DEBUG PRINT
        return total_pages
    except Exception as e:
        logging.error(f"Error extracting total pages: {e}")
        return 1


async def scrape_page(session, page, amount_of_batches):
    """Scrape data from a single page."""
    url = BASE_URL + str(page)
    html = await fetch_page(session, url, page)

    if not html:
        logging.error(f"Batch No.{amount_of_batches}. Failed to fetch page {page}. Skipping...")
        return [], None  # Skip if page fails

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": lambda x: x and x.startswith("guid-")})

    if not table:
        logging.warning(f"Batch No.{amount_of_batches}. No table found on page {page}. Check if the structure has changed!")
        return [], None

    titles = [th.text.strip() for th in table.find_all("th")]
    rows = table.find_all("tr")[1:]  # Skip first row (headers)
    data = [[td.text.strip() for td in row.find_all("td")] for row in rows]

    print(f"✅ Scraped {len(data)} records from page {page}")  
    return data, titles


async def scrape_batch(session, pages, amount_of_batches):
    """Scrape a batch of pages concurrently."""
    tasks = [scrape_page(session, page, amount_of_batches) for page in pages]
    results = await asyncio.gather(*tasks)

    all_data = []
    headers = None
    for data, cols in results:
        if data:
            all_data.extend(data)
        if cols and not headers:
            headers = cols
    
    return all_data, headers


async def scrape_all_pages(output_file="animal_records_2.csv"):
    """Scrape all pages using async requests in batches and save data."""
    async with aiohttp.ClientSession() as session:
        total_pages = await get_total_pages(session)
        all_data = []
        table_titles = None
        amount_of_batches = 1

        # Process pages in batches
        for start in range(1, total_pages + 1, BATCH_SIZE):
            batch = list(range(start, min(start + BATCH_SIZE, total_pages + 1)))
            print(f"🔄 Scraping batch number {amount_of_batches} {batch}...")

            data, headers = await scrape_batch(session, batch, amount_of_batches)

            if data:
                all_data.extend(data)
            if headers and not table_titles:
                table_titles = headers

            # Save after each batch
            if all_data:
                df = pd.DataFrame(all_data, columns=table_titles)
                df.to_csv(output_file, index=False, mode='a', header=not (start > 1), encoding="utf-8-sig")
                print(f"💾 Saved {len(all_data)} records to file.")
                all_data = []  # Reset memory

            amount_of_batches += 1

            # Randomized delay between batches
            await asyncio.sleep(random.uniform(3, 5))

    parsing_ended = datetime.now() 
    time_difference = parsing_started - parsing_ended
    print(f"Scraping started at: {parsing_started}\nScraping completed at: {parsing_ended}\nTotal execution time: {time_difference}\nData saved to {output_file}")


# Run the scraper
asyncio.run(scrape_all_pages("animal_records_2.csv"))
