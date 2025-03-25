# pip install scrapy-user-agents
# DOWNLOADER_MIDDLEWARES = {
#     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
#     'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
# }

import scrapy
from bs4 import BeautifulSoup
import pandas as pd

class ServicesViewSpider(scrapy.Spider):
    name = "services_view"
    allowed_domains = ["tanba.kezekte.kz"]

    custom_settings = {
        "FEEDS": {"services_view.csv": {"format": "csv", "encoding": "utf-8-sig", "overwrite": True}},
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "CONCURRENT_REQUESTS_PER_IP": 2
    }

    def start_requests(self):
        """Read URLs from mark_factory.csv and create Scrapy requests."""
        try:
            df = pd.read_csv("services.csv", header=0)  # Read CSV with headers
            for url in df["Link"]:  # Use correct column name
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse, 
                    meta={"url": url}
                )
        except Exception as e:
            self.logger.error(f"Error reading mark_factory.csv: {e}")

    def parse(self, response):
        """Extract data from the detail page."""
        soup = BeautifulSoup(response.text, "html.parser")
        card_body = soup.find("div", class_="card-body")

        if not card_body:
            self.log(f"No card-body found on {response.url}", level=scrapy.log.WARNING)
            return

        services_tag = card_body.find_all("div", class_ = "col-md-3")
        services_name = [service.text.strip() for service in services_tag]

        desc_tag = card_body.find_all("div", class_ = "col-md-9")
        desc_name = [desc.text.strip().replace("\xa0", "") for desc in desc_tag]

        

        
        # Yield extracted data
        yield {
            "Link": response.url, 
            services_name[0]: desc_name[0], 
            services_name[1]: desc_name[1], 
            services_name[2]: desc_name[2], 
            services_name[3]: desc_name[3], 
            services_name[4]: desc_name[4],
            services_name[5]: desc_name[5],  
        }

        

