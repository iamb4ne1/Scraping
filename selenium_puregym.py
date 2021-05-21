# -*- coding: utf-8 -*-
"""
Created on Sun Jan 04 14:50:37 2015

@author: WellsM
"""

import time
import re
import logging
import calendar
import pandas as pd

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from datetime import datetime, time as dtime
from bs4 import BeautifulSoup
from load_app_data import load_app_data

from my_logger import my_logger
   

class SeleniumPuregym(object):
    
    book_strings = ['pure pump' ,
                'pure legs' ,
                'pure tabata' ,
                'bodyweight' ,
                'pure intensity' ,
                'pure power' ,
                'pure fat burn' ,
#                'pure blast' ,
                'pure box fit',
                ]
    
    
    def __init__(self, debug=False):
        self.earliest_start_time = dtime(18,30)
        self.latest_start_time = dtime(20)
        self.browser = None
        self.booked_classes_csv = r'{}\booked puregym classes.csv'.format(load_app_data())
        
        if debug:
            loglevel='debug'
            
        else:
            loglevel='info'
        my_logger('SeleniumPuregym', loglevel=loglevel)

    
    def main(self):
        try:
            browser = self.initialize()
            self.log_in()
        
            browser.set_window_position(0, 0) #NOTE: 0,0 might fail on some systems
            browser.set_window_size(1200, 800)
        
            page_loaded = False
            while not page_loaded:
                try:
                    browser.find_element_by_xpath('//*[@id="timetable-view-switcher"]/dt[2]/span').click()
                    page_loaded = True
                except:
                    print "Page not loaded in time."
                    browser = self.log_in(browser)
                    time.sleep(10)    
    
            last_day = browser.find_element_by_xpath('//*[@id="main"]/div[3]/div/div[9]/h2').get_attribute("innerHTML")
            print "The last day available is ", last_day
            logging.info("The last day available is {}".format(last_day))
            try_to_book_classes = True
            while try_to_book_classes:
                webelement_list = browser.find_elements_by_css_selector('.three.columns.tt_block')
                try_to_book_classes = self.class_booker(webelement_list)
                time.sleep(10)
            print 'Done!'
        except Exception:
            logging.exception('')
#        raw_input('Press any key to continue...')


     
    def initialize(self,):
        chromedriver_path = r'C:\Users\WellsM\Downloads\chromedriver_win32\chromedriver.exe'
        self.browser = browser = webdriver.Chrome(chromedriver_path)  # Optional argument, if not specified will search path.
        return browser
        
    def log_in(self,):
        try:
            browser = self.browser
            logging.info('Logging in')
            browser.get('http://www.puregym.com/members/bookings/')
            time.sleep(2)
            elem=browser.find_element_by_id('edit-email')
            elem.send_keys("e.clewlow@gmail.com")
            elem=browser.find_element_by_id('edit-pincode')
            elem.send_keys("22160685" + Keys.RETURN)
            time.sleep(5)
            
        except Exception:
            logging.exception('log in')

                
 
    def class_booker(self, webelement_list):
        booked_classes = pd.read_csv(self.booked_classes_csv)
        for we in webelement_list:
            try:
                pgc = PuregymClass(we, self)
                for book_string in self.book_strings:

                    if (book_string.upper() in pgc.class_name and 'BOOK' in pgc.class_book_string 
                            and pgc.class_time >= self.earliest_start_time 
                            and pgc.class_time <= self.latest_start_time):  
                        msg= '{} class found!'.format(pgc.class_name)
                        print msg
                        logging.info(msg)
                        if [pgc.class_name, pgc.date] in booked_classes.values.tolist():
                            logging.warning("class {} on {} has already been booked!".format(pgc.class_name, pgc.date ))
                        else:
                            success = pgc.book()
                            if success:
                                pgc.to_csv()
#                    # end if
#                # end for
            except NoSuchElementException:
                pass
            except Exception:
                logging.exception('')
            # end try
        # end for
        return False

    def exit_browser(self):
        logging.debug('Closing the browser.')
        self.browser.quit()



class PuregymClass(object):
    
    def __init__(self, we, spg):
        self.spg = spg
        self.we = we
        self.class_name =  we.find_element_by_tag_name('h1').get_attribute("innerHTML").upper()
        self.class_time = parse_time(we.find_element_by_tag_name('h2').get_attribute("innerHTML"))
        self.class_book_string = we.find_element_by_css_selector('.use-ajax.secondary.button.ajax-processed').get_attribute("innerHTML").upper()
        self.class_book_elem1 = we.find_element_by_css_selector('.icon-info')
        self.class_book_elem2 = we.find_element_by_css_selector('.use-ajax.secondary.button.ajax-processed')
        self.booked = False
        self.date = self.we.get_attribute('data-date')

    def get_date(self):
        raise NotImplementedError


    def book(self, ):
        tries = 0
        while tries < 5:
            try:
                scroll_element_into_view(self.spg.browser, self.class_book_elem1)
                self.class_book_elem1.click() # opens the 'popup' ?
                time.sleep(10)
                scroll_element_into_view(self.spg.browser, self.class_book_elem2)
                self.class_book_elem2.click() #books the class
                tries += 1


                ## print weekday                                
                dt = datetime.strptime(self.date, '%Y-%m-%d')
                self.day = calendar.day_name[dt.weekday()]
                self.time = self.we.get_attribute('data-time')
                print self.day
                ##

                msg= 'Booked class {} Date: {} {} Time: {}'.format(self.class_name,self.day, self.date, self.time )
                print msg
                logging.info(msg)

                time.sleep(10)
                self.booked = True
                return True
            except Exception:
                logging.exception('')
                time.sleep(10)
            # end try
                
            try:
                scroll_element_into_view(self.spg.browser, self.we.find_element_by_css_selector('.weekly-close'))
                self.we.find_element_by_css_selector('.weekly-close').click()    # closes the 'popup'?
                time.sleep(10)
            except:
                logging.exception('')
        # end while
                
    def to_series(self):
        s = pd.Series([self.class_name, self.date], index=['class_name', 'date'])
        return s

    def to_csv(self):
        try:
            s = self.to_series()
            s.to_frame('').T.to_csv(self.spg.booked_classes_csv, mode='a', header=False, index=False)
        except:
            logging.exception('')
        
#    def from_csv(self):
#        df = pd.read_csv('booked puregym classes.csv',)
#        return df
        

def scroll_element_into_view(driver, element):
    """Scroll element into view"""
    y = element.location['y']
    driver.execute_script('window.scrollTo(0, {0})'.format(y))
    
    
def parse_time(a_string):
    strings = a_string.split()
    clean_string = re.sub(r'[^0-9.:]','',strings[0])
    return datetime.strptime(clean_string, '%H:%M').time()

def load_booked_classes():
    df = pd.read_csv(r'{}\booked puregym classes.csv'.format(load_app_data()),)
    return df
            
        
        
if __name__ == "__main__":
    spg = SeleniumPuregym()
    spg.main()
    spg.exit_browser()