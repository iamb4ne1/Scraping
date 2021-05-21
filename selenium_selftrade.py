# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 18:55:34 2014

@author: WellsM
"""

import win32api
import threading

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from time import sleep
from bs4 import BeautifulSoup
from re import sub
from tkinter_messagebox import mbox

from lineno import lineno


def initialize():
    stbrowser = webdriver.Firefox()
    stbrowser.implicitly_wait(10)
    stbrowser.get('https://selftrade.co.uk/login?')
    intial_window_handle = stbrowser.window_handles
    elem=stbrowser.find_element_by_id('login')
    elem.send_keys(PASSWORD)
    elem.send_keys(Keys.RETURN)
    sleep(5)
    new_window_handle = [x for x in stbrowser.window_handles if x != intial_window_handle[0]]
    trade_window_handle = new_window_handle[0]
    stbrowser.switch_to_window(trade_window_handle)
    
    stbrowser.switch_to.frame(stbrowser.find_element_by_name('comhome'))
    stbrowser.switch_to.frame(stbrowser.find_element_by_name('usecase'))
    stbrowser.find_element_by_name('loginid')
    Select(stbrowser.find_element_by_name("dayOfBirth")).select_by_value(DAY)
    Select(stbrowser.find_element_by_name("monthOfBirth")).select_by_value(MONTH)
    Select(stbrowser.find_element_by_name("yearOfBirth")).select_by_value(YEAR)
    
    
    ## authenticate using keypad here ##
    win32api.MessageBox(0, 'Please authenticate using the keypad' , 'Action', 0x00001000) 
    sleep(5)
    return stbrowser, trade_window_handle


def fetch_depth_data(stbrowser, ticker, order_type=0, quantity='111'):
    
    loaded = False
    while not loaded:
        try:
            try:  
                stbrowser.switch_to_default_content()
            except Exception as ex:
                print "In selenium_selftrade, fetch_depth_data", lineno() , ex
                
            try:
                stbrowser.switch_to.frame(stbrowser.find_element_by_name('comhome'))
                stbrowser.switch_to.frame(stbrowser.find_element_by_name('navleft'))
                loaded = True
            except Exception as ex:
                "In selenium_selftrade, fetch_depth_data", lineno() ,ex
        except NoSuchElementException:
            print "Page not loaded. Waiting 5 secs..."
            sleep(5)
        except Exception as ex:
            print "In selenium_selftrade, fetch_depth_data", ex
            stbrowser, trade_window_handle = initialize() 

    try:
        stbrowser.find_element_by_link_text('Trade').click()
    except:
        stbrowser.find_element_by_link_text('Dealing').click()
        sleep(2)
        stbrowser.find_element_by_link_text('Trade').click()
        
    try:  
        stbrowser.switch_to_default_content()
        sleep(1)
    except Exception as e:
        print e
        
    try: 
        stbrowser.switch_to.frame(stbrowser.find_element_by_name('comhome'))
        stbrowser.switch_to.frame(stbrowser.find_element_by_name('usecase'))
    except Exception as e:
        pass

    try:
        sleep(3)
        stbrowser.find_elements_by_name('isBuy')[order_type].click()
        sleep(1)
        stbrowser.find_element_by_name('Epic').send_keys(ticker)
        sleep(1)
        stbrowser.find_elements_by_name('isBuy')[order_type].click()
        sleep(1)
        stbrowser.find_element_by_name('goNext').click()
        sleep(1)
    except Exception as e:
        print e
        
    try:
        stbrowser.find_element_by_name('Quantity').send_keys(quantity)
        sleep(1)
    except Exception as e:
        print e

    try:        
        stbrowser.find_element_by_name('goGetQuote').click()
        sleep(1)
    except Exception as e:
        print e
        
    try:
        stbrowser.switch_to.frame(stbrowser.find_element_by_name('mainframemain'))
        sleep(1)
    except Exception as e:
        print e

    try:    
        depth_data = BeautifulSoup(stbrowser.find_element_by_xpath('//*[@id="container1"]/form/table[3]/tbody/tr[4]/td[5]').get_attribute('innerHTML')).findAll('td',{'align':'right'})
        # to do: remove \xa0 char
        bid_price = sub('\xa0', '', depth_data[1](text=True)[0])
        offer_price = sub('\xa0', '', depth_data[3](text=True)[0])
        max_size = sub('\xa0', '', depth_data[5](text=True)[0])
        
        if order_type == 0:
            print "OFFER DETAILS: "
            print 'Offer price:', offer_price
            print 'Max size:', max_size
        elif order_type == 1:
            print "BID DETAILS: "
            print 'Bid price:', bid_price
            print 'Max size:', max_size

## Add functionality to save to sqlite
    except Exception as e:
        print e
    return stbrowser

#def raw_input_with_timeout(prompt, timeout=300.0):
#    timer = threading.Timer(timeout, thread.interrupt_main)
#    astring = None
#    try:
#        timer.start()
#        astring = raw_input(prompt)
#    except KeyboardInterrupt:
#        stbrowser.refresh()
#    timer.cancel()
#    return astring

def wait_for_event(e):
    
    while e.wait():
        sleep(200)
        if e.is_set():
            stbrowser.refresh()
            print '\nI just refreshed the browser.'     


if __name__ == "__main__":
    stbrowser, trade_window_handle = initialize()
    e = threading.Event()
    t = threading.Thread(name='your_mum', 
                     target=wait_for_event,
                     args=(e,))
    t.start()
    while True:
        e.set()
        ticker = raw_input('Enter ticker: ').upper()
        e.clear()
        msg = 'Do you want to poll SelfTrade?'
        if ticker is not None:
            result = mbox(msg, b1='Continue', b2='Ignore', b3='SelfTrade', frame=True, t=False, entry=False)
            if result == True:
                pass
            elif result == False:
                pass
            elif result == None:
                print "Fetching ST data for", ticker
                stbrowser = fetch_depth_data(stbrowser, ticker, 0, '111')
                sleep(3)
                stbrowser = fetch_depth_data(stbrowser, ticker, 1, '111')
            else:
                print 'Something went wrong'
            
            
