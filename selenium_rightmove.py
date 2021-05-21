# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 07:56:41 2014

@author: Mark
"""

import logging

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import numpy as np
from re import sub

from my_logger import my_logger


def main(browser=None):
    my_logger()
    
    if browser == None:
        browser = webdriver.Firefox()
        browser.implicitly_wait(10)
    browser.get('http://www.rightmove.com')
    
    town_name = "pokesdown"
    
    print town_name
    av_rent = get_average_rental(browser, town_name)
    print 'average rent:' + unicode(av_rent)
    av_price = get_average_purchase_price(browser, town_name)
    print 'average price:'+ unicode(av_price)
    print 'yield:'+ unicode(av_rent*12/av_price)
    return browser

def get_average_purchase_price(browser, town_name):
    try:
        browser.find_element_by_link_text('For Sale').click()
        elem = browser.find_element_by_id('searchLocation')  # Find the search box
        elem.send_keys(town_name + ' station' + Keys.RETURN)
        Select(browser.find_element_by_id("radius")).select_by_value('0.5')
        Select(browser.find_element_by_id("displayPropertyType")).select_by_value('flats')
        Select(browser.find_element_by_id("minBedrooms")).select_by_value('1')
        Select(browser.find_element_by_id("maxBedrooms")).select_by_value('1')
#        Select(browser.find_element_by_id("retirement")).select_by_value('false')
#        Select(browser.find_element_by_id("partBuyPartRent")).select_by_value('false')
        
        browser.find_element_by_id("submit").click()
        print 'No properties for sale:', browser.find_element_by_id('resultcount').get_attribute('innerHTML')
        Select(browser.find_element_by_id("sortselect")).select_by_value('1')
        
        no_pages=float(BeautifulSoup(browser.find_element_by_id('pagenavigation').get_attribute('innerHTML'))(text=True)[0][-1])
        if no_pages > 1:
            mid_page_no = str(int(round(no_pages/2,0)))
            
            browser.find_element_by_link_text(mid_page_no).click()
            
        prices = browser.find_elements_by_class_name('price-new')
        
        return np.nanmean(np.array([float_or_numpy_nan(sub('[^0-9.]','',BeautifulSoup(price.get_attribute('innerHTML'))(text=True)[0])) for price in prices]))
        
    except Exception as ex:
        print ex
        logging.exception('')

def float_or_numpy_nan(in_string):
    try:
        return float(in_string)
    except ValueError:
        return np.NaN

def get_average_rental(browser, town_name):
    try:
        browser.find_element_by_link_text('To Rent').click()
        elem = browser.find_element_by_id('searchLocation')  # Find the search box
        elem.send_keys(town_name + ' station' + Keys.RETURN)   
        Select(browser.find_element_by_id("radius")).select_by_value('0.5')
        Select(browser.find_element_by_id("displayPropertyType")).select_by_value('flats')
        Select(browser.find_element_by_id("minBedrooms")).select_by_value('1')
        Select(browser.find_element_by_id("maxBedrooms")).select_by_value('1')
    #    Select(browser.find_element_by_id("retirement")).select_by_value('false')
        browser.find_element_by_id("submit").click()
        print 'No properties for rent:', browser.find_element_by_id('resultcount').get_attribute('innerHTML')
        
        Select(browser.find_element_by_id("numberOfPropertiesPerPage")).select_by_value('50')
        
        rents = browser.find_elements_by_class_name('price-new')
        
        return np.nanmean(np.array([float_or_numpy_nan(sub('[^0-9.]','',BeautifulSoup(rent.get_attribute('innerHTML'))(text=True)[2])) for rent in rents]))

    except Exception as ex:
        print ex
        logging.exception('')


if __name__ == "__main__":
    browser = main()