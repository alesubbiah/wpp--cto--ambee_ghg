###### WPP AMBEE GHG API TEST ######

from gzip import BadGzipFile
from wsgiref import headers
import requests
import os
import json
from dotenv import find_dotenv, load_dotenv
import logging
import pandas as pd
import schedule
import time


__author__ = 'alesubbiah'

# Load dotenv for API key
load_dotenv(find_dotenv())
API_KEY = os.getenv('key')

# Logging config
logging.basicConfig(
    format='[%(asctime)s %(levelname)s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def get_ghg_city_data(city: str):
    """
    Get data from Ambee GHG API for a specific city using the by-place endpoint.

    Args: 
        city: Name of the city of interest as a string. The API will use the city centre to give latitude and longitude values of the city to the API.

    """
    url = "https://api.ambeedata.com/ghg/latest/by-place"
    logging.info(f"Calling Ambee GHG API for data on {city}")
    querystring = {"place":city}
    headers = {
        'x-api-key': API_KEY,
        'Content-type': "application/json"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    logging.info(response.text)
    df = _convert_to_df(response)
    df['date'] = pd.to_datetime('today').normalize()
    df['city'] = city
    logging.info(f"Logged data for {pd.to_datetime('today')}")
    return df

def get_ghg_geospatial(lat, lng):
    url = "https://api.ambeedata.com/ghg/latest/by-lat-lng"
    querystring = {"lat":lat, "lng":lng}
    headers = {
        'x-api-key': API_KEY,
        'Content-type': "application/json"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    logging.info(response.text)
    json_acceptable_string = response.text.replace("'", "\"")
    d = json.loads(json_acceptable_string)
    # dataset = ws.datasets['dataset-name']
    data = d['data']
    return pd.DataFrame(data[0]).T

def _convert_to_df(response):
    json_acceptable_string = response.text.replace("'", "\"")
    d = json.loads(json_acceptable_string)
    # dataset = ws.datasets['dataset-name']
    data = d['data']
    data = pd.DataFrame(data[0]).T
    logging.info('Converted response to DataFrame...')
    return data

def fetch_data(csv_path, city):
    get_ghg_city_data(city).to_csv(csv_path, mode='a', header=False)


cities = ["Los Angeles", "Caracas", "New York", "London", "Mumbai", "Singapore", "Cape Town", "Sydney"]

# Get initial data into .csv
for city in cities: 
    get_ghg_city_data(city).to_csv("ambee_ghg.csv", mode='a', header=False)

# Fetch data every day
for city in cities: 
    schedule.every().day.at("10:30").do(fetch_data, csv_path='ambee_ghg.csv', city=city)

while True:
    schedule.run_pending()
    time.sleep(1)

