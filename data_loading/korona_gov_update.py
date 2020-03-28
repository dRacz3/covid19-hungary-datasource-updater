from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime

def get_new_values():
    koronagov = requests.get("https://koronavirus.gov.hu/").text
    gov_site = BeautifulSoup(koronagov)
    data_cards = gov_site.find_all("", class_="diagram-a")
    collected_data = {}
    collected_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M")
    for card in data_cards:
        label = card.find_all('', class_='label')[0].text
        value = json.loads(card.find_all('', class_='number')[0].text.replace(' ',''))
        collected_data[label] = value
    return collected_data