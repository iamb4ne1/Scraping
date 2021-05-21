# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 23:33:51 2019

@author: mark
"""

from __future__ import print_function


import logging
from my_logger import my_logger

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

from time import sleep
from bs4 import BeautifulSoup

from to_temp_file import to_temp_file
from load_app_data import load_app_data
from FB_settings import USERNAME, PASSWORD, FB_ID

from datetime import datetime
from selenium.common.exceptions import StaleElementReferenceException


try:
    import urllib
except: #python3
    import urllib.request
import re

from selenium.webdriver.common.keys import Keys

def main():
    fapi = FacebookAPI()
    browser = fapi.browser
    fapi.login()
    sleep(2)
    fapi.navigate_to_photos()
    sleep(1)
    photonum=1
    
    while 0:
        try:
            fapi.save_current_photo(photonum)
        except StaleElementReferenceException:
            fapi.save_current_photo(photonum)
            
        fapi.navigage_to_next_photo()
        photonum+=1
    return browser
    

class FacebookAPI(object):
    def __init__(self):
        self.browser = browser = webdriver.Chrome()
        self.base_save_location = r'F:\My Pictures\Photos of me off facebook'
        browser.get(r'https://facebook.com/')
        sleep(2)
    
    def login(self):
        browser = self.browser
        password=PASSWORD
        email = USERNAME
        
        facebook_email_element = browser.find_element_by_id("email")
        facebook_email_element.send_keys(email)
        facebook_password_element = browser.find_element_by_id("pass")
        facebook_password_element.send_keys(password)
        browser.find_element_by_id('loginbutton').click()

    def navigate_to_photos(self):
        url = r"""https://www.facebook.com/{}/photos?lst=197808211%3A197808211%3A1548200263&source_ref=pb_friends_tl""".format(FB_ID)
        self.browser.get(url)
        sleep(3)
        self.browser.find_element_by_xpath("""//*[@id="pic_2254066587945560"]/div/i""").click()
        

    def save_current_photo(self, photo_num):
        elem = self.browser.find_element_by_xpath("""//*[@id="photos_snowlift"]/div[2]/div/div[1]/div[1]/div[1]/div[1]/div[3]/img""")
        src = elem.get_attribute('src')
        description = elem.get_attribute('alt')
        urllib.urlretrieve(src, r"{}\{} {}.jpg".format(self.base_save_location, photo_num, re.sub(r"Image may contain: ", "", description, ).encode('utf8', 'replace')))
        #urllib.request.urlretrieve(src, "local-filename.jpg")        


    def navigage_to_next_photo(self):
        next = self.browser.find_element_by_xpath("""//*[@id="photos_snowlift"]/div[2]/div/div[1]/div[1]/div[1]/a[2]""")
        next.click()
        
if __name__ == "__main__":
    browser = main()
