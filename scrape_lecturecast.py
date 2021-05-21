# -*- coding: utf-8 -*-
"""
Created on Mon Dec 24 21:40:44 2018

@author: mark
"""

from __future__ import print_function


import logging
from my_logger import my_logger

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException


browser = webdriver.Chrome()

browser.get(r'https://moodle-1819.ucl.ac.uk/mod/lti/view.php?id=786923')


"""** login now **"""

driver = browser
iframe = driver.find_element_by_xpath("//iframe[@id='contentframe']")
driver.switch_to.frame(iframe)
media_files = browser.find_elements_by_class_name("menu-opener")


for media in media_files:
    media.get_attribute('innerHTML')


# doesnt work   
#media_files[0].click()

def get_video_source():
    videos = browser.find_elements_by_xpath("//video")
    
    for video in videos:
        src = video.get_attribute("src")
        print(src)