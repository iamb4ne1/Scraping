# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 16:41:12 2015

@author: wellsm
"""

import re
import time
import win32api
import numpy as np
import sqlite3
import logging

from bs4 import BeautifulSoup
from datetime import datetime, time as dtime
from matplotlib.pyplot import pause

from lineno import lineno
from string_to_num import string_to_float

### TO DO: Take out all functions that shouldnt be called inside the class
## TO DO: Turn the event into a dictionary giving info on what the even is and which side of the order book

class Level2(object):
    def __init__(self, ssst):
#        logger = logging.getLogger(__name__)
        logging.info('Initialising Level2')
        self.ticker = ssst.ticker
        self.shareprice_crawler = ssst.shareprice_crawler
        self.ssst = ssst
        self.old_bid = None
        self.old_offer = None
        self.event = False
        self.formatted_bids = None
        self.formatted_asks = None
        self.event = False
## IF this doesnt work first time then it causes errors every time
        try:
            loaded = self.get_level2_data_and_save_to_db(self.shareprice_crawler, self.ticker)
            if loaded:
                self.ssst.level2_success = True
                if self.formatted_bids is not None and self.formatted_asks is not None:
                    self.classify_and_check_one_sided_book(ssst)  
        except Exception as ex:
            logging.exception("In Level2 __init__")
            print "In level2 init", ex
        
### Get and save L2 data
    def get_level2_data_and_save_to_db(self, shareprice_crawler, ticker):
        try:
            #level 2 info
            shareprice_crawler.browser.find_element_by_link_text("Level 2").click()
            pause(2)
            time_now = datetime.now().time()
            if time_now > dtime(8) and time_now < dtime(16,30):
                elem=shareprice_crawler.browser.find_element_by_id('level2-panel')
                if self.check_l2_loaded(elem):
                    loaded = True
                    soup = BeautifulSoup(elem.get_attribute('innerHTML'))
            
                    bids = soup.find('tbody', {'class':"bid stream-field-bid"})
                    asks = soup.find('tbody', {'class':"ask stream-field-ask"})
            
                    self.formatted_bids =  [bid(text=True) for bid in bids.findAll("tr")]
                    self.formatted_asks =  [ask(text=True) for ask in asks.findAll("tr")]
                   
                    if self.formatted_bids is not None:
                        self.l2_data_insert_sqlite( self.formatted_bids, ticker, 1)
            
                    if self.formatted_asks is not None:
                        self.l2_data_insert_sqlite( self.formatted_asks, ticker, 0)
                        
                    best_bid = soup.find('tbody', {'class':"stream-field-sbid"})
                    best_ask = soup.find('tbody', {'class':"stream-field-sask"})            
                    try:
                        self.best_bid_vol = string_to_float(best_bid(text=True)[1])
                        self.best_ask_vol = string_to_float(best_ask(text=True)[1])
                        ## NEED BETTER ERROR HANDLING HERE
                        self.best_bid_price = string_to_float(best_bid(text=True)[2])
                        self.best_ask_price = string_to_float(best_ask(text=True)[0])
                        if self.shareprice_crawler.debug:
                            print self.best_bid_price, self.best_ask_price
                            logging.debug('best_bid_price:'+unicode(self.best_bid_price))
                            logging.debug('best_ask_price:'+unicode(self.best_ask_price))
                    except Exception as ex:
                        logging.exception("In get_level2_data")
                        print "In get_level2_data", lineno(), ex
                        print best_bid
                        print best_ask
    #                return self.best_bid_price , best_ask_price
                else:
                    loaded = False
            else:
                logging.info('Did not collect L2 data as time is outside trading hours')
                loaded = False
            return loaded
        except Exception as ex:
            print 'In get_level2_data', lineno(), ex

    def check_l2_loaded(self, elem):
        try:
            attempt = 0
            while attempt < 10:
                soup = BeautifulSoup(elem.get_attribute('innerHTML'))
                is_loading = soup.find(text=re.compile("Loading..."))
                attempt += 1
                if is_loading is None:
                    return True
                else:
#                    print "Page is still loading..."
                    pause(2)
            print "Page not loaded but check loaded timed out"
            logging.warning("Page not loaded but check loaded timed out")
            self.formatted_bids = None
            self.formatted_asks = None
            return False
    
        except Exception as ex:
            print "In check_l2_loaded,", lineno(), ex
            logging.exception("In check_l2_loaded")
            return None
    
            
    def l2_data_insert_sqlite(self, data_in, ticker, bid_flag):
        try:    
            connection = sqlite3.connect(self.shareprice_crawler.db_filepath)
            self.create_trade_db_table(connection)
            insert_date = datetime.now().replace(microsecond=0)
            if bid_flag:
                for data in data_in:
                    try:
                        data = (ticker, 1, data[0], data[1] ,data[2], data[3])
                        SQL, data_tup = self.create_l2_SQL_sqlite(data, insert_date)
                        connection.cursor().execute(SQL, data_tup)
                    except:
                        data = (ticker, 1, data[0], 'None', data[1] ,data[2])
                        SQL, data_tup = self.create_l2_SQL_sqlite(data, insert_date)
                        connection.cursor().execute(SQL, data_tup)
                    finally:
                        connection.commit()
            else: #must be offers
                for data in data_in:
                    try:
                        data = (ticker, 0, data[3], data[2], data[1], data[0])
                        SQL, data_tup = self.create_l2_SQL_sqlite(data, insert_date)
                        connection.cursor().execute(SQL, data_tup)
                    except:
                        data = (ticker, 0, data[2], 'None', data[1], data[0])
                        SQL, data_tup = self.create_l2_SQL_sqlite(data, insert_date)
                        connection.cursor().execute(SQL, data_tup)
                    finally:
                        connection.commit()
        except Exception as ex:
            logging.exception('In l2_data_insert_sqlite:')
            print 'In l2_data_insert_sqlite:', lineno(), ex
            print data_in

    
    def create_l2_SQL_sqlite(self, data, insert_date):
        try:
            SQL = """
            insert into 
            l2_data (Ticker, Bid_flag, Insert_date, Order_time, Market_maker, Size, Price)
            values (?, ?, ?, ?, ?, ?,? )"""
            data_tup = (data[0], data[1], insert_date, data[2], data[3], data[4], data[5])
            return SQL, data_tup
        except Exception as e:
            logging.exception('In create_l2_SQL_sqlite')
            print 'In create_l2_SQL_sqlite', lineno(), e


    def create_trade_db_table(self, connection):
        connection.cursor().execute("""
        CREATE TABLE IF NOT EXISTS l2_data
            (ID INTEGER PRIMARY KEY, Ticker text, Bid_flag INTEGER, Insert_date text, Order_time Text, Market_maker Text, Size REAL, Price REAL)
             """)
        connection.commit() 
### End get and save L2 data
        
### Check change in big and offer
    def check_change_in_bid_and_offer(self, ticker):
        self.check_change_in_bid_or_offer(ticker, 'bid')
        self.check_change_in_bid_or_offer(ticker, 'ask')
        
    def check_change_in_bid_or_offer(self, ticker, book_side):
        try:
            if self.old_bid is not None and self.old_offer is not None:
                if self.old_bid != 0.0 and self.old_offer!= 0.0 and self.best_bid_price != 0.0 and self.best_ask_price!=0.0:
                    if book_side == 'bid':
                        bid_change = (self.best_bid_price - self.old_bid)/self.old_bid
                        if abs(bid_change) > 0.025 :
                            msg= ticker + ': large change in bid\nfrom '+str(self.old_bid)+' to '+str(self.best_bid_price)
                            print 'best bid:', self.best_bid_price
                            print 'old bid:', self.old_bid
                            self.shareprice_crawler.browser.find_element_by_id('level2-panel').click()
                            print msg
                            logging.info(msg)
                            if not self.shareprice_crawler.silent_mode:
                                win32api.MessageBox(0, msg , 'Alert!', 0x00001000)
                    else: #book_side == 'ask':
                        ask_change = (self.best_ask_price - self.old_offer)/self.old_offer
                        if abs(ask_change) > 0.025:
                            msg= ticker + ': large change in offer\nfrom '+str(self.old_offer)+' to '+ str(self.best_ask_price)
                            print 'best offer:', self.best_ask_price
                            print 'old offer:', self.old_offer               
                            self.shareprice_crawler.browser.find_element_by_id('level2-panel').click()
                            print msg
                            logging.info(msg)
                            if not self.shareprice_crawler.silent_mode:
                                win32api.MessageBox(0, msg , 'Alert!', 0x00001000)
            self.update_old_bids_and_offers()
        except Exception as ex:
            logging.exception('In check_change_in_bid_or_offer')
            print 'In check_change_in_bid_or_offer', lineno(), ex
###  END check change in big and offer

    def update_old_bids_and_offers(self):
        self.old_bid = self.best_bid_price
        self.old_offer = self.best_ask_price
        return True
            
### check for one sided order book and technical level
    def classify_and_check_one_sided_book(self, ssst):
        try:
            if ssst.ignore:
                pass # ticker ignore flag is switched on
            else:
                if self.is_SETS():
                    ## TO DO: WHAT HAPPENS NEXT AFTER STRONG BIDS?? -record all L2 data and best bid, keep track of where price goes after.
                    ## keep a note of how wide the spread is
                    # also look at other bids further down the ladder                
                    self.check_one_sided_book(self.ticker, ssst.portfolio_type_flag, 6, 8)            
                else:   # not is_SETS == is MM only
                    self.check_one_sided_book(self.ticker, ssst.portfolio_type_flag, 4, 4)

        except Exception as ex:
            logging.exception('In check_for_one_sided_order_book')
            print 'In check_for_one_sided_order_book', lineno(), ex

    def is_SETS(self):
        """ Uses L2 data to check whether the stock is a SETS stock. If False then its probably MM only """
        try:
            if self.formatted_bids == [] or self.formatted_asks == []:
                print "No bids or asks!"
                return None
            else:
                sets = 0
                for row in self.formatted_bids:
                    if len(row) <= 3:
                        sets += 1
                for row in self.formatted_asks:
                    if len(row) <= 3:
                        sets += 1
                if sets > 2:
                    return True
                elif sets > 0:
                    print "WARNING!: in is_SETS number of SETS participants is ambiguous!"
                    return True
                else:   # There are no sets participants - must be MM only
                    return False
        except Exception as ex:
            logging.exception('In is_SETS')
            print 'In is_SETS', lineno(), ex
            return None
    
    def check_one_sided_book(self, ticker, flag, size_multiplier, mm_multiplier):
        """ Highlights if the size on one side of the book is significantly larger
            than on the other side. Now does both size-based and number of MMs-based classification. """
        try:
            ## First check for any big orders
            self.highlight_any_big_orders(self.formatted_bids, self.formatted_asks)
            
            ## Next assess the number of MMs and the size of the orders on the best bid
            ### TO DO: Colapse 4 into one function
            no_bid_mms = self.calc_no_MMs_at_best(self.formatted_bids)
            no_offer_mms = self.calc_no_MMs_at_best(self.formatted_asks)        
            if 1:#flag >= 0:                
                if self.best_bid_vol > size_multiplier*self.best_ask_vol and self.best_ask_vol > 100 and no_bid_mms > 2:
                    self.msg = ticker+': strong bids\nBest bid: '+ str(self.best_bid_price)
                    logging.info(self.msg)
                    self.event = 1
                elif (no_bid_mms >= no_offer_mms*mm_multiplier):
                    self.msg = ticker+': Large No MMs on bid\nBest bid: '+ str(self.best_bid_price)
                    logging.info(self.msg)                    
                    self.event = 1
            if 1:#flag <= 0:
                if self.best_ask_vol> size_multiplier*self.best_bid_vol and self.best_bid_vol > 100 and no_offer_mms > 2:
                    self.msg= ticker + ': strong offer\nBest offer: '+ str(self.best_ask_price)
                    logging.info(self.msg)
                    self.event = -1
                elif (no_offer_mms >= no_bid_mms*mm_multiplier):
                    self.msg= ticker + ': Large No MMs on offer\nBest offer: '+ str(self.best_ask_price)
                    logging.info(self.msg)
                    self.event = -1
        except Exception as ex:
            logging.exception("In check_one_sided_book,")
            print "In check_one_sided_book,", lineno(), ex

    def highlight_any_big_orders(self, formatted_bids, formatted_asks):
        try:
            logging.info("Checking for large orders")
            bid_sizes = np.array([string_to_float(formatted_bid[-2]) for formatted_bid in formatted_bids], dtype= '<f8')
            ask_sizes = np.array([string_to_float(formatted_ask[1]) for formatted_ask in formatted_asks], dtype= '<f8')
            if self.shareprice_crawler.debug:
                print bid_sizes
                print ask_sizes
            average_bid_size = bid_sizes.mean()
            average_ask_size = ask_sizes.mean()
            average_size = np.concatenate((bid_sizes, ask_sizes), axis=1).mean()
            if np.any(average_bid_size > 6*average_size):
                print "* large big found!"
                self.large_order = True
            if np.any(average_ask_size > 6*average_size):
                print "* large offer found!"
                self.large_order = True
        except Exception as ex:
            logging.exception("In highlight_any_big_orders")
            print "In highlight_any_big_orders", ex

    def calc_no_MMs_at_best(self, formatted_orders):
        try:
            ## This logic could be improved
            try:    #bid side
                best_prices = [float(formatted_order[-1]) for formatted_order in formatted_orders]
            except: #offer side
                best_prices = [float(formatted_order[0]) for formatted_order in formatted_orders]
            best_price = best_prices[0]
            if self.shareprice_crawler.debug:
                print "Best prices:", best_prices
                print "Best price:", best_price
            no_MMs = [best_prices.count(price) for price in best_prices if price == best_price][0]
            return no_MMs
        except Exception as ex:
            logging.exception("in no_MMs,")
            print "in no_MMs,", lineno(), ex

    def bid_ask_volume_imbalance(self):
        try:
            volume_imbalance = self.best_bid_vol - self.best_ask_vol
            return volume_imbalance
        except:
            logging.exception('')
            
    def calc_spread(self):
        return self.best_ask_price - self.best_bid_price