# -*- coding: utf-8 -*-
"""
Created on Wed May 14 13:18:06 2014

@author: mark
"""

from bs4 import BeautifulSoup

import urllib2
import mechanize
import sqlite3
import logging

from urlparse import urljoin
from datetime import date, timedelta, datetime
from matplotlib.finance import quotes_historical_yahoo_ohlc

from load_app_data import load_app_data
from classify_time_series import classify_time_series
from yticker import yticker
from DataTable import DataTable
from pandas import Series
from setproxy import setproxy
## need to get fund names as dictionary keys and match the two dictionaries on 
## entries with the same key

"""

TO DO: look into using https://www.theaic.co.uk/aic/analyse-investment-companies
"""


def main():
    fd = FundDiscounts()
    fd.all_to_sql()
    return fd
    
def fund_discounts():
    FundDiscounts().all_to_sql()

class FundDiscounts(object):
    def __init__(self):
        logging.debug('Initialising the FundDiscounts')
        setproxy()
        self.db_filepath = load_app_data() + '\\investment_trust_data.db'   
        yield_url = 'http://www.trustnet.com/Investments/Perf.aspx?ctr=QS&univ=T&Pf_AssetClass=A:EQUI&Pf_sortedColumn=yield&Pf_sortedDirection=DESC'
        disc_url = 'http://www.trustnet.com/Investments/Perf.aspx?ctr=QS&univ=T&Pf_AssetClass=A:EQUI&Pf_sortedColumn=Discount&Pf_sortedDirection=ASC'
        urls = [yield_url, disc_url]
        names = ['yields', 'discounts']
        self.url_dict = dict(zip(names, urls))
        insert_date = date.today()
        self.df_dict = {}
        
        for url, name in zip(urls,names):
            df = None
            try:
                hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
                req = urllib2.Request(url, headers=hdr)
                html = urllib2.urlopen(req).read()
            except urllib2.HTTPError:
                logging.exception('')
                logging.warning(url)
                html = ''
            soup = BeautifulSoup(html)
            
            table_soup = soup.find("table", {"id": "fundslist"})
            
            headers = ['rowno', 'plus', 'None', 'fund_name', 'group', 'None2', 'price', 'Prem/Disc', 'NAV',
                    'NDY', 'FE risk score', '1y perf', '3y perf', '5y perf', '1y NAV', '3y NAV', '5y NAV', 'None3']
        
            dt = DataTable(table_soup, headers=headers)
            df = dt.to_dataframe(remove_cols=['plus', 'None','None2', 'None3', 'href_plus', 'href_None3', 'href_None', 'href_group'], hrefs=True) # hrefs=True not working
            df['insert_date'] = Series(insert_date, index=df.index)
            self.df_dict[name] = df
    
    
    def all_to_sql(self):
        for name, df in self.df_dict.iteritems():
            self.df_to_sql(name, df)
    
    def df_to_sql(self, name, df):
        connection=sqlite3.connect(self.db_filepath)
        try:
        ## NOTE THAT I HAD TO ALTER THE PANDAS CODE TO GET THIS TO WORK!!
            df.to_sql(name=name, con=connection, if_exists='append', index=False)
        except:
            logging.exception('')
        finally:
            connection.close()

    def generate_tickers(self, name, df):
        base_url = self.url_dict[name]
#        for href in df['href_fund_name']:
        for index, row in df.iterrows():
            url = urljoin(base_url, row['href_fund_name'])
            html = urllib2.urlopen(url).read()
            soup =BeautifulSoup(html)
            ticker = soup.find('a',{'class':'js_hits','title':'company announcements from Investegate'})['href'].split('=')[1]
            yield row['fund_name'], ticker
            
    def tickers_to_sql(self, name, df):
        # to do
        for fund_name, ticker in self.generate_tickers(name, df):
            self.ticker_to_sql(fund_name, ticker)
    
        


    def ticker_to_sql(self, fund_name, ticker):
        connection = sqlite3.connect(self.db_filepath)
        sql = """insert into 'trust_data' (
        	'Trust_name',
          'ticker' )
            values ( ?, ? ) """    
        try:
            connection.cursor().execute(sql, [fund_name, ticker])
            connection.commit()
        except Exception:
            logging.exception('')
        finally:
            connection.close()


    def generate_holdings_table(self, name, df):
        base_url = self.url_dict[name]
        for index, row in df.iterrows():
            url = urljoin(base_url, row['href_fund_name'])
            html = urllib2.urlopen(url).read()
            soup =BeautifulSoup(html)
            table_soup = soup.find('table',{'class':'top_holding'})
            top_holding_datatable = DataTable(table_soup)
            top_holding_df = top_holding_datatable.to_dataframe()
            table_soup2 = soup.find('div',{'class':'panel_widget fund_breakdowns'}).table
            top_holding_datatable2 = DataTable(table_soup2)
            top_holding_df2 = top_holding_datatable2.to_dataframe()
            yield top_holding_df, top_holding_df2



def create_name_ticker_table():
    sql = """CREATE TABLE if not exists 'name_ticker' (
    	'Trust_name'	TEXT,
    	'Ticker'	TEXT,
     PRIMARY KEY(Trust_name)
    )
    """
    connection.cursor().execute(sql)
    connection.commit()

def save_to_db2(Trust_name, ticker):
    filepath = load_app_data() + '\\trust_discounts.db'
    connection = sqlite3.connect(filepath)    
    sql = """update 'trust_data' 
    set 'Ticker' = ? 
    where Trust_name = ?
    """
    try:
        connection.cursor().execute(sql, (ticker,Trust_name))
        connection.commit() 
    except Exception as ex:
        print ex
    connection.close()


def update_trend_directions():
    filepath = load_app_data() + '\\trust_discounts.db'
    connection = sqlite3.connect(filepath)    

    select_sql = ''' select ticker from trust_data '''
    update_sql = ''' update 'trust_data' 
    set trend_direction = ? 
    where ticker = ? '''
    for row in connection.cursor().execute(select_sql):
        ticker = row[0]
        date1 = date.today() - timedelta(weeks=52)    
        date2 = date.today()
        try:
            quotes = quotes_historical_yahoo_ohlc(yticker(ticker), date1, date2, asobject=True)
            direction_int, direction_str, one_yr_change = classify_time_series(quotes)
            print direction_int
            connection.cursor().execute(update_sql, (direction_int,ticker))
            connection.commit()             
        except Exception as ex:
            print ex
            print direction_int
        

    
if __name__ == "__main__":
    fd = main()
