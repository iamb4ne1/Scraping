# -*- coding: utf-8 -*-
"""
Created on Mon May 29 09:16:59 2017

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


"""

TO DO: Add weekend breaks 
Momondo url: http://www.momondo.co.uk/flightsearch/?Search=true&TripType=1&SegNo=1&SO0=BCN&SD0=LON&SDP0=25-08-2017&AD=1&TK=ECO&DO=false&NA=false&currency=GBP
    https://www.momondo.co.uk/flightsearch/?Search=true&TripType=2&SegNo=2&SO0=BCN&SD0=LON&SDP0=25-10-2017&SO1=LON&SD1=BCN&SDP1=26-10-2017&AD=1&TK=ECO&DO=false&NA=false&currency=GBP
https://www.momondo.co.uk/flightsearch/?Search=true&TripType=4&SegNo=3&SO0=BCN&SD0=LON&SDP0=25-10-2017&SO1=LON&SD1=TLL&SDP1=26-10-2017&SO2=TLL&SD2=DEL&SDP2=27-10-2017&AD=1&TK=ECO&DO=false&NA=false&currency=GBP

TK=ECO      # economy class ticket
DO=true     # direct flights only
NA=false    # use nearby airports
currency=GBP # currency


"""



class KayakScraper(object):

    def __init__(self, base_urls, airports, depart_airports,
                 user_agent, future_timedelta = 1,
                 days_ahead=90, debug=False, one_way=True, return_durations=[7,14]
                 ):

        self.count_of_successfull_loads = 0
        
        if type(user_agent) != type('str'):
            raise TypeError, "user_agent must be a string"

        webdriver.DesiredCapabilities.PHANTOMJS['phantomjs.page.settings.userAgent'] = user_agent
        driver = self.driver = webdriver.PhantomJS(r'C:\Users\mat4m_000\Downloads\phantomjs-2.1.1-windows\phantomjs-2.1.1-windows\bin\phantomjs.exe')
            
        if debug:
            self.debug_mode( driver, base_urls, airports, depart_airports, 
                 user_agent,)
        else:
            for day_delta in xrange(3, days_ahead, 4):
                todays_date = date.today() + timedelta(future_timedelta) #so, 1 is actually tomorrows date
                logging.debug('Start date for search is {}'.format(todays_date))
                date_to_search = datetime.strftime(todays_date + timedelta(day_delta) , '%Y-%m-%d')
    
                for u in base_urls:
                    for airport in airports:
                        for depart_airport in depart_airports:
                            if airport != depart_airport:
                                if one_way:
                                    self.one_way(u, depart_airport, airport, date_to_search, user_agent)
                                if return_durations:
                                    self.return_flight(return_durations, todays_date, day_delta,
                                                       u,depart_airport, airport,date_to_search, user_agent)


    def one_way(self,u, depart_airport, airport, date_to_search, user_agent):
        url = r'{}/{}-{}/{}-flexible?fs=stops=0'.format(
                u, depart_airport, airport, date_to_search)
        logging.debug('Url created: {}'.format(url))
        KayakOneUrl(self, url, user_agent)
    
    def return_flight(self, durations, todays_date, day_delta,
                      u,depart_airport, airport, date_to_search, user_agent):
        for duration in durations:
            ret_date = todays_date + timedelta(day_delta) + timedelta(duration)
            ret_url = r'{}/{}-{}/{}-flexible/{}-flexible?fs=stops=0'.format(
                    u,depart_airport, airport,date_to_search, ret_date, )
            logging.debug('Url created: {}'.format(ret_url))
            KayakOneUrl(self, ret_url, user_agent)
    
    def weekend_break(self):
        raise NotImplementedError


    def debug_mode(self, driver, base_urls, airports, depart_airports, 
                 user_agent,):
        todays_date = date.today() + timedelta(1) #so, actually its tomorrows date
        date_to_search = datetime.strftime(todays_date + timedelta(7) , '%Y-%m-%d')
    
        url = r'{}/{}-{}/{}-flexible?fs=stops=0'.format(
                                base_urls[0], 
                                depart_airports[0], 
                                airports[0], 
                                date_to_search)

        KayakOneUrl(self, url, user_agent)




class KayakOneUrl(object):

    def __init__(self, ks, url, user_agent):
        self.url = url
        self.user_agent = user_agent
        
        driver = ks.driver
        logging.debug('Fetching url: {}'.format(url))
        driver.get(url)
        driver.implicitly_wait(30) # seconds
        time.sleep(4) # to fix
        try:
            html = driver.find_element_by_xpath('//*[@class="Common-Results-ProgressBar theme-phoenix Hidden"]').get_attribute("innerHTML")
            logging.info(html)
            if '100%' not in html:
                logging.warning('page not loaded. url {}'.format(url))
                time.sleep(20)
    
            else:
                logging.info('The page apears to be completely loaded because 100% was found')
        except NoSuchElementException:
            logging.warning('No loading bar found!')
            time.sleep(4)

        logging.info('Page loaded: {}'.format(driver.title.encode('utf8', 'replace')))
        url = driver.current_url
        logging.info('Url loaded: {}'.format(url))
        pos = url.find('flights',)
        depart_airport = url[pos+8:pos+8+3]    
        destination_airport = url[pos+8+4:pos+8+7]
        depart_airport, destination_airport, base_url = parse_url(url)
        results = []
        access_datetime = datetime.now()        
        price_table = driver.find_elements_by_class_name('col-cell')
        for price_element in price_table:
            depart_date, return_date, price, price_as_int = self.parse_elem(price_element)
            data_elem = (depart_airport,destination_airport,
                         depart_date,return_date,price, price_as_int,
                         access_datetime, user_agent, base_url,
                         )
            results.append(data_elem)
        df = pd.DataFrame(results, columns = ['Depart', 'Destination',
                    'depart_date','return_date','price', 'price_as_int',
                    'access_datetime', 'user_agent', 'base_url'
                    ])
        kayak_data_to_sql(df, depart_airport)
        ks.count_of_successfull_loads += 1
        logging.info('One iteration complete. url: {}, user_agent: {}'.format(url, user_agent))

    def parse_elem(self, elem):
        try:
            soup = BeautifulSoup(elem.get_attribute('innerHTML'))
            soup2 = soup.find('a')
            try:
                date_x_string = soup2['data-x-filter-code']
            except TypeError:
                logging.exception(soup.prettify())
                date_x_string = ''
            try:
                date_y_string = soup2['data-y-filter-code']
            except TypeError:
                logging.exception(soup.prettify())
                date_y_string = ''
    
            logging.debug('date_x_string: {}'.format(date_x_string))
            logging.debug('date_y_string: {}'.format(date_y_string))
            
            try:
                if date_y_string == '': #its just one way
                    depart_date = datetime.strptime(date_x_string,'%Y%m%d')
                    return_date = None
    
                else: # there is a return journey
                    depart_date = datetime.strptime(date_y_string,'%Y%m%d')
                    return_date = datetime.strptime(date_x_string,'%Y%m%d')
            except:
                return_date = None
                depart_date = None
                logging.exception(soup)
    
            price = soup2.text
            if price == '--':
                price_as_int = np.nan
            else:
                price_as_int = int(re.sub('[^0-9]','',price))
            return depart_date, return_date, price, price_as_int
#        except StaleElementReferenceException:  ## why would this happen?
#            self.scrape_one_url(url, user_agent)
        except:
            logging.exception('')
            try:
                logging.warning(soup.prettify())
            except:
                pass
            return None, None, None, None

        



def kayak_data_to_sql(df, table_name = 'Kayak_data'):
    conn = sqlite3.connect(load_app_data()+r'\kayak_data.db')
    df.to_sql(table_name,conn,if_exists='append',index=False)
    conn.close()
    

def parse_url(url):
    pos = url.find('flights',)
    depart_airport = url[pos+8:pos+8+3]    
    destination_airport = url[pos+8+4:pos+8+7]
    base_url = url[8:pos]
    return depart_airport, destination_airport, base_url

user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
               ## Doesnt work because it looks like the html returned is very different
## user agent doesnt seem to make any difference (because there was a bug and the UA wasnt actually changing)
#               'Mozilla/5.0 (Linux; Android 5.1.1; SAMSUNG SM-A9000 Build/LMY47V) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/4.0 Chrome/44.0.2403.133 Mobile Safari/537.36',
               ]

base_urls = ['https://www.kayak.co.uk/flights',
             ]
"""
'https://www.kayak.it/flights',
            'https://www.fi.kayak.com/flights',
            'https://www.kayak.es/flights',
            'https://www.kayak.no/flights',
            'https://www.kayak.nl/flights',
            'https://www.kayak.pl/flights',
            'https://www.kayak.com/flights',
            'https://www.kayak.ie/flights',
            'https://www.kayak.de/flights',
            'https://www.kayak.com.my/flights',
            'https://www.kayak.com.tr/flights',
            'https://www.kayak.ae/flights',
            'https://www.kayak.fr/flights',
            'https://www.ca.kayak.com/flights',
            'https://www.kayak.com.ar/flights',
            'https://www.kayak.com.pe/flights',
            'https://www.gr.kayak.com/flights',
            'https://www.kayak.co.jp/flights',
            'https://www.kayak.cl/flights',
            'https://www.kayak.com.au/flights',
            'https://www.kayak.pt/flights',
            'https://www.kayak.ch/flights',
            'https://www.kayak.co.th/flights',
            'https://www.kayak.com.br/flights',
            'https://www.kayak.com.co/flights',
            'https://www.kayak.se/flights',
            'https://www.kayak.com.hk/flights',
            'https://www.kayak.co.id/flights',
            'https://www.tw.kayak.com/flights',
            'https://www.kayak.dk/flights',
            'https://www.nz.kayak.com/flights',
            'https://www.kayak.ru/flights',
            'https://www.kayak.sg/flights',
            'https://www.kayak.co.in/flights',
            ]
"""

depart_airports = ['LON',
## usually a waste of time looking for direct flights from other departure 
## airports in the UK because there arent any
#                   'EMA',
#                   'MAN',
#                   'BHX',
                   'TLL',
                   'FAO',
                   ]


airports = ['IST', #Istanbul
            'RIX', #Riga
            'CPH', #Copenhagen
            'BUD', #Budapest
            'ZAG', #Zagreb
            'PRG', #Prague
            'LAS', #Las Vegas
            'NYC', #New York
            'TLL', #Tallinn
            'IEV', #Kiev
            'WAW', #Warsaw
            'DEL', #Delhi
            'BCN', #Barcelona
            'BER', #Berlin
            'MUC', #Munich
            'CDG', #Paris CDG
            'FAO', #Faro Portugal
            'LIS', #Lisbon Portugal 
            'OPO', #Porto Portugal
            'FRA', #Frankfurt
            ]

"""
            'TIA',
            'EVN',
            'GRZ',
            'INN',
            'KLU',
            'LNZ',
            'SZG',
            'VIE',
            'GYD',
            'MSQ',
            'ANR',
            'BRU',
            'CRL',
            'LGG',
            'OST',
            'SJJ',
            'TZL',
            'BOJ',
            'SOF',
            'VAR',
            'DBV',
            'PUY',
            'SPU',
            'ZAD',
            'ZAG',
            'LCA',
            'PFO',
            'BRQ',
            'PRG',
            'AAL',
            'AAR',
            'BLL',
            'CPH',
            'TLL',
            'HEL',
            'OUL',
            'RVN',
            'TMP',
            'TKU',
            'VAA',
            'AJA',
            'BSL', 
            'MLH', 
            'EAP',
            'BIA',
            'EGC',
            'BIQ',
            'BOD',
            'BES',
            'FSC',
            'LIL',
            'LYS',
            'MRS',
            'MPL',
            'NTE',
            'NCE',
            'BVA',
            'CDG',
            'ORY',
            'SXB',
            'TLN',
            'TLS',
            'TBS',
            'FMM',
            'BER',
            'SXF',
            'TXL',
            'BRE',
            'CGN',
            'DTM',
            'DRS',
            'DUS',
            'FRA',
            'FDH',
            'HHN',
            'HAM',
            'HAJ',
            'FKB',
            'LEJ',
            'MUC',
            'FMO',
            'NUE',
            'PAD',
            'STR',
            'NRN',
            'ATH',
            'CHQ',
            'CFU',
            'HER',
            'KGS',
            'RHO',
            'SKG',
            'BUD',
            'DEB',
            'KEF',
            'ORK',
            'DUB',
            'NOC',
            'KIR',
            'SNN',
            'AHO',
            'AOI',
            'BRI',
            'BGY',
            'BLQ',
            'BDS',
            'CAG',
            'CTA',
            'CIY',
            'FLR',
            'GOA',
            'SUF',
            'LIN',
            'MXP',
            'NAP',
            'OLB',
            'PMO',
            'PEG',
            'PSR',
            'PSA',
            'CIA',
            'FCO',
            'TPS',
            'TSF',
            'TRN',
            'VCE',
            'VRN',
            'ALA',
            'TSE',
            'RIX',
            'KUN',
            'VNO',
            'LUX',
            'SKP',
            'MLA',
            'KIV',
            'TGD',
            'TIV',
            'AMS',
            'EIN',
            'GRQ',
            'MST',
            'RTM',
            'AES',
            'BGO',
            'BOO',
            'HAU',
            'KRS',
            'RYG',
            'OSL',
            'TRF',
            'SVG',
            'TOS',
            'TRD',
            'GDN',
            'KTW',
            'KRK',
            'WMI',
            'POZ',
            'WAW',
            'WRO',
            'FAO',
            'LIS',
            'FNC',
            'PDL',
            'OPO',
            'OTP',
            'CLJ',
            'TSR',
            'SVX',
            'DME',
            'SVO',
            'VKO',
            'LED',
            'AER',
            'BEG',
            'PRN',
            'BTS',
            'LJU',
            'ALC',
            'LEI',
            'BCN',
            'BIO',
            'FUE',
            'GRO',
            'LPA',
            'IBZ',
            'XRY',
            'ACE',
            'MAD',
            'AGP',
            'PMI',
            'MAH',
            'MJV',
            'REU',
            'SDR',
            'SCQ',
            'SVQ',
            'TFN',
            'TFS',
            'VLC',
            'ZAZ',
            'GOT',
            'MMX',
            'ARN',
            'BMA',
            'NYO',
            'VST',
            'BRN',
            'GVA',
            'LUG',
            'ZRH',
            'ADA',
            'ESB',
            'AYT',
            'DLM',
            'IST',
            'SAW',
            'ADB',
            'BJV',
            'TZX',
            'KBP',
            'IEV',
            'ODS',
            ]
"""


if __name__ == "__main__":
    my_logger(loglevel='info', print_to_console_level='info')
    if 1:
        airports = ['TLL',
                    ]
        depart_airports = ['BCN', 'CDG'
                           ]
    ks = KayakScraper(base_urls, airports, depart_airports, 
                 user_agents[0], debug=False, future_timedelta = 2, days_ahead=90)