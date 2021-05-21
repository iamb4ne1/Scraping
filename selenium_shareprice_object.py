# -*- coding: utf-8 -*-
"""
Created on Thu Jan 01 17:21:51 2015

@author: WellsM
"""


import re
import sys
import logging 
import Queue
import threading

from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from datetime import datetime, time as dtime
from matplotlib.pyplot import pause

from load_app_data import load_app_data
from selenium_shareprice_single_ticker import SeleniumSharepriceSingleTicker
from my_logger import my_logger
from stops_checker import StopsChecker
from setproxy import setproxy
from string_to_num import string_to_float
from CurrentHolding import CurrentHolding
#from tkinter_control_panel import TkUIThreadedTask
from morningstar_risers_fallers import morningstar_risers_fallers
#from BestWorstMomentum import BestWorstMomentum
from load_technical_stocks import load_technical_stocks
from DirectorDealings import DirectorDealings
from initialize_webdriver import initialize, log_in
from SeleniumADVFN import SeleniumADVFN


def main():
    SharepriceCrawler(debug=False)
    
class SharepriceCrawlerBasicInfo(object):
    ## currently the same as SHarepriceCrawler init. would it be easier to have 
    ## a seperate class? or a config file?
    def __init__(self, debug=False, silent_mode=False, gui_q=None, 
                 threaded_q=None,  top=None, event=threading.Event(), 
                 logger_on=False ):
        if not logger_on:
            my_logger('SharepriceCrawler', path=load_app_data(), loglevel='debug', print_to_console_level='debug' )
        try:
            logging.info('Initialising Shareprice Crawler')
            self.db_filepath = load_app_data() + '\\Shareprice_data.db' 
            self.trades_db = load_app_data() + '\\Google_trades.db' 
            self.ticker_data_dict = {}
            self.ignore_dict = {}  
            self.quotes_dict = {}
            self.high_alert_dict = {}
            self.most_recent_news_datetime_dict = {}
            self.current_holdings_dict = {}
            self.stbrowser = None
            self.pause_event= event
            self.debug = debug 
            self.wait_for_level = True
            self.urllib_opener = setproxy()
            self.selenium_shareprice_single_ticker_dict = {}
            self.ticker_dict = {}
            self.paused = False
            self.safe_exit_flag = False
 
            self.top = None
            
            ## if they are none maybe leave them as none and dont send gui jobs to a queue?
            if gui_q == None:
                self.gui_q = Queue.PriorityQueue()
            else:
                self.gui_q = gui_q

            if threaded_q == None:
                self.threaded_q = Queue.PriorityQueue()
            else:
                self.threaded_q = threaded_q
            
            if len(sys.argv) > 1:
                print "Silent mode set to ", sys.argv[1]
                logging.info("Silent mode set to"+ str(sys.argv[1]))
                self.silent_mode = sys.argv[1]
            else:
                self.silent_mode = silent_mode 
            self.stops_checker = StopsChecker(self)
            try:
                self.DD = DirectorDealings()
            except: #seem to be getting a 407 error for this module
                logging.exception('')
                self.DD = {}

            self.sadvfn = SeleniumADVFN(self.db_filepath)

            ## initialise browser here?
        except Exception:
            logging.exception('')
            raw_input("Press any key to continue...")


class SharepriceCrawler(object):
    def __init__(self, debug=False, silent_mode=False, gui_q=None, 
                 threaded_q=None,  top=None, event=threading.Event(), 
                 logger_on=False ):
        if not logger_on:
            my_logger('SharepriceCrawler', path=load_app_data() )
        try:
            logging.info('Initialising Shareprice Crawler')
            self.db_filepath = load_app_data() + '\\Shareprice_data.db' 
            self.trades_db = load_app_data() + '\\Google_trades.db' 
            self.ticker_data_dict = {}
            self.ignore_dict = {}  
            self.quotes_dict = {}
            self.high_alert_dict = {}
            self.most_recent_news_datetime_dict = {}
            self.current_holdings_dict = {}
            self.stbrowser = None
            self.pause_event= event
            self.debug = debug 
            self.wait_for_level = True
            self.urllib_opener = setproxy()
            self.selenium_shareprice_single_ticker_dict = {}
            self.ticker_dict = {}
            self.paused = False
            self.safe_exit_flag = False
 
            self.top = None
            
            ## if they are none maybe leave them as none and dont send gui jobs to a queue?
            if gui_q == None:
                self.gui_q = Queue.PriorityQueue()
            else:
                self.gui_q = gui_q

            if threaded_q == None:
                self.threaded_q = Queue.PriorityQueue()
            else:
                self.threaded_q = threaded_q
            
            if len(sys.argv) > 1:
                print "Silent mode set to ", sys.argv[1]
                logging.info("Silent mode set to"+ str(sys.argv[1]))
                self.silent_mode = sys.argv[1]
            else:
                self.silent_mode = silent_mode 
            self.stops_checker = StopsChecker(self)
            try:
                self.DD = DirectorDealings()
            except: #seem to be getting a 407 error for this module
                logging.exception('')
                self.DD = {}
                
           
       
            
            self.browser = initialize()
            self.browser = log_in(self.browser)
            
            self.sadvfn = SeleniumADVFN(self.db_filepath)
    
         
            while debug:
                logging.info("In debug mode")
                self.db_filepath = load_app_data() + '\\Shareprice_data_TEST.db' 
                self.crawl_single_ticker( 0 )
                pause(30)
                
        
            while (not debug) and (not self.safe_exit_flag):
                try:
                    time_now = datetime.now().time()
                    if time_now > dtime(8) and time_now < dtime(17, 30):
                        self.crawl_shareprice_portfolio( 'trades', 0, ) 
                        self.crawl_shareprice_portfolio( 'trades2', 0, ) 
                        self.crawl_shareprice_portfolio( 'Buy_list', 1, )     
                        self.crawl_shareprice_portfolio( 'Buy_list2', 1, )   
                        self.crawl_shareprice_portfolio( 'Sell_list', -1,)
                        silent_mode_indicator = self.silent_mode
                        self.silent_mode = True
                        self.crawl_technical_stocks()
                        self.silent_mode = silent_mode_indicator
                    else:

                        logging.warning('Time is outside of trading hours, exiting the program')
                        self.browser.quit()
                        sys.exit()
                except NoSuchElementException:
                    print "NoSuchElementException. Attempting to re-log in."
                    logging.warning("NoSuchElementException. Attempting to re-log in.")
                    try:
                        self.browser = log_in(self.browser)
                    except:
                        pause(10)
                    continue
                
            logging.info("Safe exit flag has been set. Exiting the program.")
        except Exception as ex:
            logging.exception('')
            print ex
            raw_input("Press any key to continue...")



    def navigate_to_stock_page(self, ticker):
        self.browser.get('http://www.shareprice.co.uk/' + ticker)
        
    def crawl_single_ticker(self, portfolio_type_flag):
        url = 'http://www.shareprice.co.uk/TSCO'
        self.browser.get(url+'#ui-tabs-2')
        pause(1)
        ticker = self.get_ticker_and_check_page(url)
        self.handle_single_ticker(ticker, portfolio_type_flag)

    def crawl_shareprice_portfolio(self, portfolio, portfolio_type_flag):
        if not self.pause_event.is_set():
            self.browser.get('http://www.shareprice.co.uk/portfolio')
            pause(4)
            elem=self.browser.find_element_by_id('spPortfolioList')
            elem.send_keys(portfolio + Keys.RETURN)
            pause(4)
            link_list = self.parse_portfolio_page()
            for link in link_list:
                if link.has_attr('href'):
                    print str(datetime.today()), link['href'], str(portfolio_type_flag)
                    logging.info(unicode(link['href'])+ " Portfolio type: "+unicode(portfolio_type_flag))
                    self.browser.get(link['href']+'#ui-tabs-2')
                    pause(3)
                    ticker = self.get_ticker_and_check_page(link['href'])
                    if ticker is not None:
                        self.handle_single_ticker(ticker, portfolio_type_flag)
        else:
            logging.debug('Pause is clicked (event is set) so skipping the crawl of shareprice portfolio')
        
    def crawl_technical_stocks(self):
        try:
            for ticker, portfolio_type_flag in load_technical_stocks('best_worst_momentum'):
                self.browser.get('http://www.shareprice.co.uk/'+ticker)
                pause(3)
                msg = self.browser.current_url+ " Portfolio type: "+str(portfolio_type_flag) + ' from BEST/WORST MOMENTUM'
                logging.info(msg)
                print str(datetime.today()) + msg 
                ticker = self.get_ticker_and_check_page(self.browser.current_url)
                if ticker is not None:
                    self.handle_single_ticker(ticker, portfolio_type_flag)
                    
            for ticker, portfolio_type_flag in load_technical_stocks('steppers'):
                self.browser.get('http://www.shareprice.co.uk/'+ticker)
                pause(3)
                msg = self.browser.current_url+ " Portfolio type: "+str(portfolio_type_flag) + ' from STEPPERS'
                logging.info(msg)
                print str(datetime.today()) + msg 
                ticker = self.get_ticker_and_check_page(self.browser.current_url)
                if ticker is not None:
                    self.handle_single_ticker(ticker, portfolio_type_flag)
                    
            for ticker, portfolio_type_flag in morningstar_risers_fallers():
                self.browser.get('http://www.shareprice.co.uk/'+ticker)
                pause(3)
                msg = self.browser.current_url+ " Portfolio type: "+str(portfolio_type_flag) + ' from MORNINGSTAR RISERS/FALLERS'
                logging.info(msg)
                print str(datetime.today()) + msg 
                ticker = self.get_ticker_and_check_page(self.browser.current_url)
                if ticker is not None:
                    self.handle_single_ticker(ticker, portfolio_type_flag)
        except:
            logging.exception('')

                
    def get_ticker_and_check_page(self, url):
        try:
            ticker = url.split('/')[3]
            scrapeticker = self.browser.find_element_by_class_name('ticker').get_attribute('innerHTML')
            if scrapeticker == "XIV": 
                return None
            if scrapeticker not in ticker:
                logging.warning("Ticker and webpage are misaligned. URL Ticker: "+ unicode(ticker)+" Webpage ticker: "+ unicode(scrapeticker))
                print "Ticker and webpage are misaligned."
                print "URL Ticker:", ticker
                print "Webpage ticker:", scrapeticker
                return None

            return ticker
        except Exception:
            logging.exception("In get_ticker_and_check_page")
            
    def handle_single_ticker(self, ticker, portfolio_type_flag):
        """ No longer uses the queue """
        if ticker not in self.ticker_dict:
            self.ticker_dict[ticker] = SeleniumSharepriceSingleTicker(self, ticker, portfolio_type_flag)
        else:
            self.ticker_dict[ticker].crawl()


    def parse_portfolio_page(self):
        portfolio_soup = BeautifulSoup(self.browser.find_element_by_id('streamingTable_shareprice_portfolio').get_attribute('innerHTML'))
        link_list = portfolio_soup.findAll('a',href=re.compile('^http://www.shareprice.co.uk'))
        try:
            for table_row in portfolio_soup.find_all('tr'):
                try:
                    ticker_href =  table_row.find('a',href=True)
                    if ticker_href is not None:
                        ticker = ticker_href['href'].split('/')[3]
                    cost_elem = table_row.find('td',{'class':'symbol-calc-pvalue'})
                    if cost_elem is not None:
                        cost = string_to_float(cost_elem.string)
                        if cost > 0.0:
                            self.current_holdings_dict[ticker] = CurrentHolding(ticker, cost, self)
                except Exception as ex:
                    print "Exception caught in parse_portfolio_page", ex
                    logging.exception('')

        except Exception as ex:
            print ex
            logging.exception('')    
        return link_list

    def exit_(self):
        self.browser.quit()

#class GuiHandler:
## make these ss_crawler methods or create new GuiHandler class?
   
    def gui_handler(self, ss_event):
        """ handle calls to the queue to pass messages back and forth to
        the gui if using the control panel """
        if self.threaded_q is None:
            ss_event.message_box_logic('message missing') # show messagebox here
        else:
            self.threaded_q.put((ss_event.msg, 10, 'msgbox'), )
            pause(2)
            while self.gui_q.empty(): 
                pause(1)
            if self.gui_q is None:
                pass 
            else:
                self.process_gui_queue(ss_event)
        
    def process_gui_queue(self, ss_event):
        while not self.gui_q.empty():
            (val, priority, description) = self.gui_q.get()
            logging.info( 'Processing job: {}'.format(description) )
            if description == 'orderbook':
                self.show_order_book()
                while self.gui_q.empty():
                    pause(1)
                self.process_queue(self.gui_q)
            elif description == 'msgbox_return':
                self.msgbox_return(val)
            elif description == 'exit':
                self.exit_()
    
    def show_order_book(self, ss_event):
        raise NotImplementedError
        
if __name__ == "__main__":
    main()
