import scrapy
from bs4 import BeautifulSoup
import logging

class MarkFactorySpider(scrapy.Spider):
    name = "mark_factory"
    allowed_domains = ["tanba.kezekte.kz"]
    start_urls = ["https://tanba.kezekte.kz/ru/reestr-tanba-public/mark-factory/list?p=1"]
    custom_settings = {
        "FEEDS": {"mark_factory.csv": {"format": "csv", "encoding": "utf-8-sig", "overwrite": True}},
        "LOG_LEVEL": "INFO",
    }
    def parse(self, response):
        print(response.request.headers)
        """Extracts total pages and schedules requests for each page."""
        soup = BeautifulSoup(response.text, "html.parser")
        pagination = soup.find("ul", class_="pagination")
        
        if pagination:
            try:
                last_page = int(pagination.find_all("a", class_="page-link")[-2].text.strip())
            except Exception:
                last_page = 1
        else:
            last_page = 1

        self.log(f"Total pages found: {last_page}", level=logging.INFO)
        for page in range(1, last_page + 1):
            yield scrapy.Request(
                url=f"https://tanba.kezekte.kz/ru/reestr-tanba-public/mark-factory/list?p={page}",
                callback=self.parse_page,
                meta={"page": page},
            )

    def parse_page(self, response):
        """Extracts data from a table on each page."""
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": lambda x: x and x.startswith("guid-")})
        
        if not table:
            self.log(f"No table found on page {response.meta['page']}", level=logging.WARNING)
            return
        

        headers = [th.text.strip() for th in table.find_all("th")]
        headers.insert(0, "Link")
        rows = table.find_all("tr")[1:]  # Skip headers
        links = []
        for row in rows:
            values = [td.text.strip() for td in row.find_all("td")]
            
            first_link = row.find("a", href=True)
            if first_link:
                full_link = response.urljoin(first_link["href"]) 
                print(f"Extracted Link: {full_link}") 
                values.insert(0, f"{full_link}") 
            yield dict(zip(headers, values))


    
