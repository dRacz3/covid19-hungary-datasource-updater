import bs4
from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime

def get_new_values():
    koronagov = requests.get("https://koronavirus.gov.hu/").text
    gov_site = BeautifulSoup(koronagov, 'html.parser')
    data_cards = gov_site.find_all("", id = lambda x : x and x.startswith('api-'))
    collected_data = {}
    for card in data_cards:
        card : bs4.element.Tag
        collected_data[card.get('id').replace('-','_')] = int(card.text.replace(' ', ''))
    collected_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return collected_data



if __name__ == "__main__":
    get_new_values()