from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
 
import datetime
import requests 
import pandas as pd
import numpy as np 
import time
import openpyxl
import xml.etree.ElementTree as ET
from urllib3.exceptions import InsecureRequestWarning
import sys
if sys.version_info[0] == 3:
    from urllib.request import urlopen
else:
    from urllib import urlopen

# 크롬 드라이버 자동 업데이트
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup as bs
from lxml.html import fromstring

#Warning msg 없앰
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#브라우저 꺼짐 방지 
chrome_options = Options()
#Options.add_argument("headless")
chrome_options.add_experimental_option("detach", True)
# 불필요한 에러 메시지 없애기
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
# Chrome WebDriver 초기화
driver = webdriver.Chrome(options=chrome_options)

url = input('url 입력: ')
driver.get(url)

#페이지 스크롤
def doScrollDown(whileSeconds):
    start = datetime.datetime.now()
    end = start + datetime.timedelta(seconds=whileSeconds)
    while True:
        driver.execute_script('window.scrollTo(0,document.body.scrollHeight);')
        time.sleep(1)
        if datetime.datetime.now() > end:
            break

try:
    #CNTS를 가져올 List 초기화
    CNTS_key = []

    #찾으려는 페이지 끝 번호 가져오는데 / 있어서 자르고 형변환 시킴, 두자리수까지 대응
    TotalCount = int(driver.find_element(By.CLASS_NAME,'TotalCount').text[-2:])
    action = driver.find_element(By.CSS_SELECTOR, 'body')
    
    print('총 페이지 수: ', TotalCount)
    
    #페이지 수 맞춰야하니 1부터 시작
    for i in range(1,999):
        
        #img src로 CNTS 코드 추출
        btn_CNTS = driver.find_elements(By.CSS_SELECTOR,'div > div.textBox > div.resultTitle > p > a')
       
        #컬렉션 내 CNTS 코드 추출해서 List 저장
        for k in btn_CNTS:
            tmp_key = k.get_attribute('onclick')
            #url 뒤에서 16자리만 끌어오면 됨
            val1 = tmp_key.split("'")
            val2 = val1[3]
            print(val2)


        #N초간 스크롤
        doScrollDown(3)
        print('i 루프 [',i,']회차 돎')

        #마지막 페이지에는 '다음 페이지' 버튼이 없음
        if i < TotalCount:
            #다음 페이지 버튼을 찾음
            next_btn = driver.find_element(By.CSS_SELECTOR, 'p > a.btn-paging.next')
            next_btn.click()
            print(i,'회차, 다음 버튼 클릭함')
            time.sleep(2)
        
        #마지막 페이지까지 수집하고 루프 끝냄
        if i == TotalCount:
            print("크롤링 끝")
            break
finally:
    driver.quit()