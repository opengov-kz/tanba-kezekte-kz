import scrapy
from bs4 import BeautifulSoup
import pandas as pd

class MarkFactoryDetailsSpider(scrapy.Spider):
    name = "factory_view"
    allowed_domains = ["tanba.kezekte.kz"]

    custom_settings = {
        "FEEDS": {"factory_view.csv": {"format": "csv", "encoding": "utf-8-sig", "overwrite": True}},
        "LOG_LEVEL": "INFO",
    }

    def start_requests(self):
        """Read URLs from links.csv and create Scrapy requests."""
        try:
            df = pd.read_csv("mark_factory.csv")
            for url in df["url"]:
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse, 
                    meta={"url": url}
                )
        except Exception as e:
            self.log(f"Error reading links.csv: {e}", level=scrapy.log.ERROR)

    def parse(self, response):
        """Extract data from the detail page."""
        soup = BeautifulSoup(response.text, "html.parser")
        card_body = soup.find("div", {"class": "card-body"})
        
        if not card_body:
            self.log(f"No card-body found on {response.meta['url']}", level=scrapy.log.WARNING)
            return

        # Extract organization name
        org_name_tag = card_body.find("div", class_="text-center text-uppercase mt-3 font-18")
        org_name = org_name_tag.find("b").text.strip() if org_name_tag else "N/A"

        # Extract address
        address_tag = card_body.find("h5", string="Адрес")
        address_text = address_tag.find_next("address").text.strip() if address_tag else "N/A"

        # Extract contacts
        contacts_tag = card_body.find("h5", string="Контакты")
        contacts_text = contacts_tag.find_next("address").text.strip() if contacts_tag else "N/A"

        # Extract organization data
        org_data_tag = card_body.find("h5", string="Данные организации")
        org_info = org_data_tag.find_next("address").text.strip().split("\n") if org_data_tag else []
        org_info = [info.strip() for info in org_info if info.strip()]

        reg_date = org_info[0].split(":")[1].strip() if len(org_info) > 0 else "N/A"
        org_type = org_info[1].split(":")[1].strip() if len(org_info) > 1 else "N/A"
        bin_number = org_info[2].split(":")[1].strip() if len(org_info) > 2 else "N/A"

        # Extract table data
        tables = card_body.find_all("table", class_="table")
        for table in tables:
            table_name = table.find_previous("p").text.strip() if table.find_previous("p") else "N/A"
            table_rows = table.find_all("tr")[1:]

            for row in table_rows:
                columns = row.find_all("td")
                if len(columns) >= 2:
                    animal_type = columns[0].text.strip()
                    quantity = columns[1].text.strip()

                    yield {
                        "URL": response.meta["url"],
                        "Organization": org_name,
                        "Address": address_text,
                        "Contacts": contacts_text,
                        "Registration Date": reg_date,
                        "Type": org_type,
                        "BIN": bin_number,
                        "Table Name": table_name,
                        "Animal Type": animal_type,
                        "Quantity": quantity,
                    }
