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
    
    # def parse_url(self, response):
    #     self.log(f"Processing URL: {response.url}", level=logging.INFO)
    #     soup = BeautifulSoup(response.text, "html.parser")
    #     card_body = soup.find("div", {"class":"card-body"})
        
    #     if not card_body:
    #         self.log(f"No card-body found on url: {response.meta['url']}", level=logging.WARNING)
    #         return

    #     card_rows = card_body.find_all("div", class_="row")  
    #     if not card_rows:
    #         self.log(f"No rows found on url: {response.meta['url']}", level=logging.WARNING)
    #         return
        
    #     org_name_tag = card_body.find("div", class_="text-center text-uppercase mt-3 font-18")
    #     org_name = org_name_tag.find("b").text.strip()
        
    #     address = card_body.find("h5", string="Адрес")
    #     address_text = address.find_next("address").text.strip() 

    #     contacts = card_body.find("h5", string="Контакты")
    #     contacts_text = contacts.find_next("address").text.strip() 

    #     org_data = card_body.find("h5", string="Данные организации")
    #     org_info = org_data.find_next("address").text.strip().split("\n")
    #     org_info = [info.strip() for info in org_info if info.strip()]
    #     reg_date = org_info[0].split(":")
    #     type = org_info[1].split(":")
    #     bin = org_info[2].split(":")

    #     tables = card_body.find_all("table", class_ = "table")
        
    #     for table in tables:
    #         table_name = table.find_previous("p")
    #         table_rows = table.find_all("tr")[1:]
    #         for table_row in table_rows:
    #             columns = table_row.find_all("td")
    #             animal_type = columns[0].text.strip()
    #             quantity = columns[1].text.strip()

    #             yield {
    #                 "URL": response.meta["url"],
    #                 "Organization": org_name,
    #                 "Address": address_text,
    #                 "Contacts": contacts_text,
    #                 "Registration Date": reg_date,
    #                 "Type": type,
    #                 "BIN": bin,
    #                 "Table Name": table_name,
    #                 "Animal Type": animal_type,
    #                 "Quantity": quantity,
    #             }

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

        rows = table.find_all("tr")[1:]  # Skip headers
        links = []
        for row in rows:
            values = [td.text.strip() for td in row.find_all("td")]
            
            # Extract first link in row
            first_link = row.find("a", href=True)
            if first_link:
                link_text = first_link.text.strip() or "Открыть"
                full_link = response.urljoin(first_link["href"]) 
                print(f"Extracted Link: {link_text}: {full_link}") 
                # yield scrapy.Request(
                #     url=f"{full_link}",
                #     callback=self.parse_url,
                #     meta={"url": full_link},
                # )
                values.insert(0, f"{link_text}: {full_link}") 
            yield dict(zip(headers, values))


    
