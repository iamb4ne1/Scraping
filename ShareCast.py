# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 18:20:52 2020

@author: mark
"""

"""
Digital Look is now ShareCast
"""

import urllib.request as request
import pandas as pd
import logging
import sqlite3 
import datetime

from yfinance import Ticker as yticker
from bs4 import BeautifulSoup
from re import sub

from load_app_data import load_app_data
from my_logger import my_logger
from to_temp_file import to_temp_file
from get_soup_from_url import get_soup_from_url

def main():
#    scn, scc = get_market_cap()
    scn = update_db_urls()
    return scn, None


def get_market_cap():
    ticker = 'TSCO'
    my_logger()
    scn = ShareCastNavigator()
    scn.get_soup_from_ticker(ticker) # maybe this should be within the get_co_name function
    co_name = scn.get_company_name(scn.soup)
    scc = ShareCastCompany(scn, ticker, url=None ,company_name=co_name, )
    scc.main_page_data()
    return scn, scc

def director_dealings():
    ticker = 'NFC'
    my_logger()
    scn = ShareCastNavigator()
    scc = ShareCastCompany(scn, ticker=ticker)
    scc.get_director_dealings()
    return scn, scc
    
def get_fundamentals_and_forecasts():
    ticker = 'TSCO'
    my_logger()
    scn = ShareCastNavigator()
    scn.get_soup_from_ticker(ticker) # maybe this should be within the get_co_name function
    co_name = scn.get_company_name(scn.soup)
    scc = ShareCastCompany(scn, ticker, url=None ,company_name=co_name, )
    fundamentals, forecasts = scc.get_key_fundamentals_and_forecasts()

    scc.write_df_to_sql(fundamentals, table_name='fundamentals')
    scc.write_df_to_sql(forecasts, table_name='forecasts')

    return scn, scc

def update_db_urls():
    my_logger(loglevel='debug')
    scn = ShareCastNavigator()
    a_z_urls = scn.get_links_from_a_z()
    scn.get_company_urls_from_A_Z_site(a_z_urls,)
    #to_temp_file(url_dict)
    for key, letter_vals in scn.url_dict.items():
        for url, name, ticker in letter_vals:
            if yticker(ticker+'.L').history(period='1y').empty:
                logging.info('ticker {} not saved'.format(ticker))
            else:
                scn.update_db_url(ticker, url,)
    return scn

class ShareCastNavigator(object):
    def __init__(self, dbfile=None):
        if dbfile is None:
            self.dbfile = load_app_data() + '\\DLdata.db'
        else:
            self.dbfile = dbfile
        logging.debug('Database file being used is {}'.format(self.dbfile))

        self.html = ''
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",}
        self.soup = ''
        self.current_url = ''

    def get_soup_from_ticker(self, ticker,):
        url = self.get_company_url_from_db(ticker,)
        soup, _ = self.get_soup_from_url(url, return_html=False)
        return soup


    def get_soup_from_url(self, url, return_html=False):
        soup, html = get_soup_from_url(url, return_html=return_html, headers=self.headers)
        self.soup = soup
        self.html = html
        return soup, html
    
    def get_links_from_a_z(self):
        root_a_z = 'https://www.sharecast.com/companyAToZ/index.html'
        req = request.Request(root_a_z, headers=self.headers)
        self.html = html = request.urlopen(req).read()
        self.soup = soup = BeautifulSoup(html, features="lxml")
        

        #A_hrefs = [tag['href'] for tag in scn.soup.findAll('a', title=True)]

        all_links = [tag.get('href') for tag in soup.findAll('a',href=True)]

        a_z_string = 'https://www.sharecast.com/companyAToZ/LSE'
        self.a_z_urls = a_z_urls = [url for url in all_links if a_z_string in url]
        logging.debug('Found {} A to Z urls'.format(len(a_z_urls)))
        return a_z_urls

    def get_company_urls_from_A_Z_site(self, a_z_urls, limit=-1):
        self.url_dict = url_dict = {}
        for url in a_z_urls[0:limit]:
            self.current_url = url
            letter = url.split('/')[-2]
            self.current_url = url
            self.soup, self.html = soup, html = get_soup_from_url(
                    url, return_html = True, headers=self.headers)
            company_urls = []
            for tr in soup.find_all('tr'):
                tag = tr.find('a')
                if tag is not None:
                    href = tag['href']
                    name= tag.string
                    tag2 = tr.find('td', {'class':"text-right"})
                    if tag2 is not None:
                        ticker = tag2.string
                    else:
                        ticker = ''
                    company_urls.append( (href, name, ticker.strip()) )
                # end if
            logging.debug('found {} company urls for letter {}'.format(len(company_urls), letter))
            url_dict[letter] = company_urls
        return url_dict



    def get_company_url_from_db(self, ticker,):
        """ Pulls the latest url from the db """
        try:
            connection = sqlite3.connect(self.dbfile)   
            cursor = connection.cursor()
            URL = cursor.execute(""" Select URL from DL_URL where Ticker = ? and URL is not NULL""", [ticker,]).fetchone()
            if URL is not None:
                url = URL[0]
                return url
            else:
                logging.warning("Could not find company page URL for {}".format(ticker))
                return None
        except Exception:
            logging.exception('')
            #logging.exception("In get_current_dl_data " + ex)
            return None
        finally:
            connection.close()



    def update_db_url(self, ticker, url):
        logging.debug('updating db for ticker {}'.format(ticker))

        try:
            connection = sqlite3.connect(self.dbfile)   
            cursor = connection.cursor()
            URL = cursor.execute(""" Select URL from DL_URL where Ticker = ? """, [ticker]).fetchone()
            if URL is not None:
                sql = """ update DL_URL set URL = ? where Ticker = ? """ # [url, ticker]
                cursor.execute(sql, [url, ticker])

            else:
                logging.debug('adding ticker {} to the db'.format(ticker))
                sql = """ insert into DL_URL(Ticker, URL) values (?, ?)"""
                cursor.execute(sql, [ticker, url])
            connection.commit()
        except Exception:
            logging.exception('')
            return None
        finally:
            connection.close()


    def get_company_name(self, soup=None,):
        if soup is None:
            logging.info('No soup value passed to get_company_name, using the last-accessed soup for this object')
            soup = self.soup
        try:
            company_name = soup.find('div' ,{'class':"company_name"}).find('h1').get_text().strip()
        except:
            logging.warning('Unable to get company name in {}'.format(soup.prettify()))
            company_name = None
        return company_name

    
    def get_ticker(self, soup=None):
        if soup is None:
            soup = self.soup
        ticker = soup.find('div' ,{'class':"company_name"}).find('h1').get_text().split()[-1].strip()
        if ticker is None:
            logging.warning('ticker not found on page {}'.format(self.current_url))
            return None
        else:
            return ticker

        

class ShareCastCompany(object):
    def __init__(self, scn, ticker=None, url=None, company_name=None,):
        self.scn = scn
        
        if url is None and ticker is None:
            logging.warning('Not able to initialise ShareCastCompany without a url or a ticker')
            return None

        if url is None:
            logging.debug('Initialising SCC, getting URL for ticker {} from the database'.format(ticker))
            url = self.scn.get_company_url_from_db(ticker)
            if url is None:
                logging.warning('URL not found in db for {}'.format(ticker))
                ## fetch url from the website here?
            
        self.url = url
        
        if ticker is None:
            logging.warning('No ticker has been set to initialise current company, using the ticker from current page {}'.format(scn.current_url))
            self.soup, html = soup, html = get_soup_from_url(self.url, return_html = False, headers=self.scn.headers)

            ticker = scn.get_ticker(scn.soup)
        self.ticker = ticker

        if company_name is None and self.url is not None:
            logging.warning('No name has been set to initialise current company, using the name from current page {}'.format(scn.current_url))
            self.soup, html = soup, html = get_soup_from_url(self.url, return_html = False, headers=self.scn.headers)

            company_name = scn.get_company_name(soup)
        else:
            logging.warning('Something went wrong getting company name')
            company_name = None
        self.company_name = company_name
        

        
    def write_df_to_sql(self,df,table_name):
        ## add company name and insert_date
        df['company_name'] = self.company_name
        df['ticker'] = self.ticker
        df['insert_date'] = datetime.date.today()
        for colname in df.columns:
            if type(colname) is type('str'):
                if '(' in colname:
                    new_colname = colname.split('(')[0]
                    currency_size = colname.split('(')[-1].replace(')', '')
                    df.rename(columns={colname:new_colname}, inplace=True)
                    df['{} currency/size'.format(new_colname)] = currency_size
        """
        FYI this works:
        scc.fundamentals.rename(lambda x:x.split('(')[0], axis='columns')
        """
        
        try:
            connection = sqlite3.connect(self.scn.dbfile)   
            #cursor = connection.cursor()
            df.to_sql(table_name, con=connection, if_exists='append', index=False, )
        except Exception:
            logging.exception('')
            return None
        finally:
            connection.close()



    def get_key_fundamentals_and_forecasts(self, ):
        url = self.scn.get_company_url_from_db(self.ticker)
        self.url = url
        if url is None:
            logging.warning('Could not get url for {}'.format(self.ticker))
        self.soup, self.html = soup, html = get_soup_from_url(url, return_html = True, headers=self.scn.headers)
        self.df_list = pd.read_html(html)
        fundamentals = self.df_list[3]
        forecasts = self.df_list[4]
        """ Other data tables are available """

        return fundamentals, forecasts

    def get_director_dealings(self, ):
        #wheres the best place to keep the company url?
        dd_url = self.url+ '/director-deals'
        self.soup, self.html = soup, html = get_soup_from_url(dd_url, return_html = True, headers=self.scn.headers)
        try:
            self.dd_list =dd_list = pd.read_html(html)
        except:
            logging.exception('')
            to_temp_file(soup.prettify())
            self.dd_list= dd_list = None
        return dd_list[0:2]

            
    def get_url_from_db(self):
        """already taken care of in SCN"""
        self.scn.get_current_url_from_db_url

    def main_page_data(self):
        try:
            ## same as get_key_fundamentals_and_forecasts
            url = self.scn.get_company_url_from_db(self.ticker)
            self.url = url
            if url is None:
                logging.warning('Could not get url for {}'.format(self.ticker))
            self.soup, self.html = soup, html = get_soup_from_url(url, return_html = True, headers=self.scn.headers)
            self.df_list = pd.read_html(html)
            self.fundamentals = self.df_list[3]
            self.forecasts = self.df_list[4]
            """ Other data tables are available """
    
    
            ## market cap
            market_cap_size = soup.find_all('li', {'class':"market-channel-headline"})[-2].get_text()
            self.market_cap_text = market_cap_text = soup.find_all('li', {'class':"b500"})[-1].get_text()
            try:
                self.pre_tax_forecast = pre_tax_forecast = self.forecasts[self.forecasts.columns[2]][0]
            except IndexError:
                logging.exception(url)
            
        except:
            logging.exception('in {}'.format(url))
        
"""
class DirectorDealings(object):
    def __init__(self, scc):
        self.soup = ''
        self.scc = scc
        
    def get_director_dealings(self, ):
        #wheres the best place to keep the company url?
        url = scc.url+ '/director-deals'
        self.soup = soup = get_soup_from_url(url, return_html = False, headers=self.scc.scn.headers)
        try:
            #buys:
            buy_table = self.soup.find('table',{'id':'directorDealings_1'})
            self.buys_df = DataTable(buy_table).to_dataframe()
            self.buys_df["Volume/Price"] = self.buys_df["Volume/Price"].map(lambda x: sub('[\s]','',x))
            
            #sells
            sell_table = self.soup.find('table',{'id':'directorDealings_2'})
            self.sells_df = DataTable(sell_table).to_dataframe()
            self.sells_df["Volume/Price"] = self.sells_df["Volume/Price"].map(lambda x: sub('[\s]','',x))
            
            #dict
            self.dd_dict = {'buys':self.buys_df, 'sells':self.sells_df}
            
            self.df = concat([self.buys_df,self.sells_df])
        except:
            logging.exception('')
            self.buys_df = None
            self.sells_df = None
            self.df = None
        
    def get_ticker_dealings(self, ticker):
        ## doesnt return both buys and sells, if applicable
        try:
            if self.buys_df is not None: ## need to investigate why it would be None
                new_columns = self.buys_df.columns.values
                new_columns = [sub(r"[^\w]", '', s) for s in new_columns]
                self.buys_df.columns = new_columns
                buys = self.buys_df.loc[self.buys_df['Ticker'] == ticker]
            if not buys.empty:
                return buys
            if self.sells_df is not None: ## need to investigate why it would be None
                new_columns = self.sells_df.columns.values
                new_columns = [sub(r"[^\w]", '', s) for s in new_columns]
                self.sells_df.columns = new_columns
                sells = self.sells_df.loc[self.sells_df['Ticker'] == ticker]
            if not sells.empty:
                return sells
            return None
        except:
            logging.exception(self.buys_df.keys())
            return None
        
    def to_sqlite(self, conn):
        create_director_dealings_table(conn)
        self.buys_df.to_sql('director_dealings', conn)
        self.sells_df.to_sql('director_dealings', conn)
"""

def create_director_dealings_table(connection):
    sql = """CREATE TABLE if not exists 'director_dealings' (
	'date'	TEXT,
	'Company_name'	TEXT,
	'Ticker'	TEXT,
	'Director'	TEXT,
	'Volume/price'	TEXT,
	'trade_value'	REAL,
	PRIMARY KEY(date,Company_name,Ticker,Director,trade_value)
     ) """
    connection.cursor().execute(sql)
    connection.commit()



""" FROM DL Crawler """


def getStockTicker(soup):
    try:
        name_box_soup = soup.find("div", {"id": "researchnav"})
        if name_box_soup is not None:
            name_box = name_box_soup.find("h1").string #seems to have switched from h5 to h1
            company_name = name_box.strip()
            ticker = sub('[()]','',company_name.split()[-1])
            logging.info("Found ticker "+ticker)
            return company_name, ticker
        else:
            logging.warning('No name found in the DigitalLook HTML')
            return None, None
    except Exception as ex:
        logging.exception('')
        return None, None

def getMCap(soup):
    try:
        MCap_box = soup.find("div", {"class": "mid-right"})(text=True)
        try:
            if "Market Cap" in MCap_box[2]:
                MCap_num = MCap_box[5]

        except IndexError:
            logging.warning("Market Cap not available")
            MCap_num = None
        try:
#            currency_box= soup.findAll("ul", {"class": "companyData"})[1](text=True)
            if 1: #"Currency" in currency_box[1]:
                MCap_currency = sub(r'[^£]','',MCap_box[5] )
            else:
                MCap_currency = None
        except IndexError:
            logging.info( "MCap currency not available")
            MCap_currency = None
        return MCap_num, MCap_currency
#        else:
#            print "MCap not found"
           ##TO DO: Add some kind of "Get_MCap_from_yahoo" function
#            return None
    except Exception as e:
        logging.exception('')
        return None, None

def parse_financials_tables(table_soup):
#        table_soup = soup.findAll("table", {'class':'table table-responsive table-hover table-bordered cator'})[0]
    try:
        cells = {}
        for rowno, row in enumerate(table_soup.findAll("tr")):
            if row.findAll('th'):
                headers = [r(text=True) for r in row.findAll('th')]
                currency = headers[2][0].split()[-1]
            if row.findAll('td'):
                cells[rowno] = [r(text=True) for r in row.findAll("td")]
        return headers, cells, currency          
    except Exception as ex:
        logging.exception('')
        return None, None, None

#
#def get_historic_data_save_to_db(soup, ticker, dbfile):
#    try:
#        hist_table = soup.findAll("table", {'class':'table table-responsive table-hover table-bordered cator'})[0]
#        headers, hist_data, currency = parse_financials_tables(hist_table)
#        if hist_data is not None:
#            for key, data_row in hist_data.iteritems():
#                DLhist_data = (ticker,) + tuple(d[0] for d in data_row) + (currency,) 
#                DL_hist_db_insert(DLhist_data, dbfile)
#        else:
#            logging.warning('hist_data is None')
#    except Exception as ex:
#        logging.exception('')

def getStockForecasts(soup, ticker):
    try:
        forecast_table = soup.findAll("table", {'class':'table table-responsive table-hover table-bordered cator'})[1]
        headers, forecast_data, currency = parse_financials_tables(forecast_table)
        if forecast_data:
            Date1 = forecast_data[1][0][0]
            Rev1 = (forecast_data[1][1][0])
            FCast1 = (forecast_data[1][2][0])
            DL_EPS = (forecast_data[1][3][0])
            DL_PE = (forecast_data[1][4][0])
            DL_PEG = (forecast_data[1][5][0])
            try:
                FCast2 = (forecast_data[2][2][0])
            except KeyError:
                logging.info("No second year forecasts available")
                FCast2 = None
        else:
            FCast1 = None
            FCast2 = None
        return FCast1, FCast2
    except Exception as ex:
        logging.exception('')
        return None, None

               
def calcPE(MCap, FCast1):
    try:
        if FCast1 is not None:
            if FCast1 != 0.0:
                #print "forecast 1: ", forecast_data[1][2]
                #print "MCap: ", MCap
                PE = MCap/FCast1
                return PE
    except Exception:
        logging.exception('')
        return None

def calc_growth(FCast1, FCast2):
    try:
        if FCast1 != 0.0:
            growth = (FCast2 - FCast1) / FCast1
            return growth
        else:
            return None
    except Exception:
        logging.exception('')
        return None            
    
    
if __name__ == "__main__":
    pass
    out = main()
    scn, scc = out