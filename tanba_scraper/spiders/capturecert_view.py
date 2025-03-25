# pip install scrapy-user-agents
# DOWNLOADER_MIDDLEWARES = {
#     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
#     'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
# }

import scrapy
from bs4 import BeautifulSoup
import pandas as pd

class CaptureCertViewSpider(scrapy.Spider):
    name = "capturecert_view"
    allowed_domains = ["tanba.kezekte.kz"]

    custom_settings = {
        "FEEDS": {"capturecert_view.csv": {"format": "csv", "encoding": "utf-8-sig", "overwrite": True}},
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "CONCURRENT_REQUESTS_PER_IP": 2
    }

    def start_requests(self):
        """Read URLs from mark_factory.csv and create Scrapy requests."""
        try:
            df = pd.read_csv("capturecert.csv", header=0)  # Read CSV with headers
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
        card_body = soup.find("div", {"class": "card-body"})
        
        if not card_body:
            self.log(f"No card-body found on {response.meta['url']}", level=scrapy.log.WARNING)
            return

        # Extract organization name
        org_name = card_body.find("h2").text.strip()

        desc_text = card_body.find("h4").text.strip()

        certId = card_body.find("h3").text.strip()

        cert_data_tag = card_body.find_all("p", class_ = "font-16")

        cert_data = [cert_data.text.strip() for cert_data in cert_data_tag]

        full_name = cert_data[0].split(",")
        issue_date = cert_data[1].split(",")
        validity_date = cert_data[2].split(",")


        yield {
            "Organization Name": org_name,
            "Description Text": desc_text,
            "Certificate ID": certId,
            "Full Name": full_name,
            "Issue Date": issue_date,
            "Validity Date": validity_date,
        }