# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 18:48:07 2018

@author: mark
"""

from bs4 import BeautifulSoup
#from to_temp_file import to_temp_file
import pandas as pd
import time
#import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait # to do
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
import sqlite3
import logging
import re
import numpy as np

from selenium.webdriver.support import expected_conditions as EC #to do
from selenium.webdriver.common.by import By #to do

from my_logger import my_logger
from load_app_data import load_app_data
from datetime import datetime, date, timedelta

from GenericFlightScraper import GenericFlightScraper

"""
examples:
https://www.momondo.co.uk/flight-search/IEV-CPH/2018-07-25/2018-08-30
# one way
https://www.momondo.co.uk/flight-search/LON-SPU/2018-08-15?sort=price_a
## return
https://www.momondo.co.uk/flight-search/BER-BCN/2018-08-15/2018-08-30?sort=price_a
"""

class MomondoScraper(GenericFlightScraper):

    base_urls = [r'https://www.momondo.co.uk/',
                 ]


    def one_way(self,u, depart_airport, airport, depart_date, user_agent):
        url = r'{}{}-{}/{}?sort=price_a&fs=stops=0'.format(u,
                depart_airport, airport, depart_date,)
        logging.debug('Url created: {}'.format(url))
        MomondoOneUrl(self, url, user_agent)
    
    def return_flight(self, durations, todays_date, day_delta,
                      u,depart_airport, airport, depart_date, user_agent):
        for duration in durations:
            ret_date = todays_date + timedelta(day_delta) + timedelta(duration)
            ret_url =r'{}{}-{}/{}/{}?sort=price_a&fs=stops=0'.format(u,
                depart_airport, airport, depart_date, ret_date)
            logging.debug('Url created: {}'.format(ret_url))
            MomondoOneUrl(self, ret_url, user_agent)

class MomondoOneUrl(object):

    def __init__(self, ms, url, user_agent):
        self.url = url
        self.user_agent = user_agent
        
        driver = ms.driver
        logging.debug('Fetching url: {}'.format(url))
        driver.get(url)
        driver.implicitly_wait(30) # seconds
        time.sleep(4) # to fix
        try:
            html = driver.find_element_by_xpath('')
        except:
            pass

if __name__ == "__main__":
    my_logger(loglevel='debug', print_to_console_level='debug')
    ms = MomondoScraper('LON','TXL', user_agent='', driver_type = 'Chrome')