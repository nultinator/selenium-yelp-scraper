import os
import csv
import json
import logging
from time import sleep
from urllib.parse import urlencode
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from dataclasses import dataclass, field, fields, asdict

OPTIONS = webdriver.ChromeOptions()
OPTIONS.add_argument("--headless")

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, page_number, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.yelp.com/search?find_desc={formatted_keyword}&find_loc={location}&start={page_number*10}"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        driver = webdriver.Chrome(options=OPTIONS)
        try:
            driver.get(url)
            logger.info(f"Fetched {url}")
                
            ## Extract Data            
            div_cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-ia-card']")


            for div_card in div_cards:

                card_text = div_card.text
                sponsored = card_text[0].isdigit() == False
                ranking = None

                img = div_card.find_element(By.CSS_SELECTOR, "img")
                title = img.get_attribute("alt")
                

                if not sponsored:
                    rank_string = card_text.replace(title, "").split(".")
                    if len(rank_string) > 0:
                        ranking = int(rank_string[0])

                rating = 0.0
                has_rating = driver.find_elements(By.CSS_SELECTOR, "div span[data-font-weight='semibold']")
                if len(has_rating[0].text) > 0:
                    if has_rating.text[0].isdigit():
                        has_rating = float(rating[0].text)
                    
                review_count = 0

                if "review" in card_text:
                    review_count = card_text.split("(")[1].split(")")[0].split(" ")

                a_element = div_card.find_element(By.CSS_SELECTOR, "a")
                link = a_element.get_attribute("href").replace("https://proxy.scrapeops.io", "")
                yelp_url = f"https://www.yelp.com{link}"

                search_data = {
                    "name": title,
                    "sponsored": sponsored,
                    "stars": stars,
                    "rank": ranking,
                    "review_count": review_count,
                    "url": yelp_url
                }
                print(search_data)

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")
        

def start_scrape(keyword, pages, location, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, page, retries=retries)



if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 4
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["restaurants"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        start_scrape(keyword, PAGES, LOCATION, retries=MAX_RETRIES)

    logger.info(f"Crawl complete.")