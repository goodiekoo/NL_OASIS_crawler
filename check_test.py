from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from urllib3.exceptions import InsecureRequestWarning
from urllib.request import urlretrieve
from uuid import uuid4
 
import datetime
import requests 
import pandas as pd 
import time
import os
import xml.etree.ElementTree as ET
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

url = input("url 입력 | OASIS 더보기+수집일 정렬 누른 상태에서 unit 100 권장 |")

#'https://www.nl.go.kr/oasis/contents/O4010000.do?page=1&pageUnit=100&schM=search_list&schType=disa&schTab=list&schIsFa=ISU-000000000379&facetKind=01&ordFld=regDay&ordBy=desc'
#xml 파싱용

CDRW_url = 'https://www.nl.go.kr/oasis/common/mods_view_xml.do?contentsId='
savePath = r'/Users/user/Pictures/OASIS/'
#썸네일, 임시로 전역 변수
ThumbnailList = list()
#컬렉션 썸네일 필터용
word = "FILE-"

#페이지 열고 
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

#CNTS 값 토대로 CDRW 가져오기 
def searchCDRW(KeyList):
    #CNTS를 key로 CDRW 값을 가져올 List 초기화
    CDRW_siteUrl = list()
    CDRW_keys = list()
    #네임스페이스 prefix 써서 오류 방지 xml:mods
    namespace_map = {"mods": "http://www.loc.gov/mods/v3"}
    #네임스페이스로 가져올 XPATH
    xpath_expression = ".//mods:recordIdentifier"
    
    #xml 사이트 url 합체
    for z in range(0,len(KeyList)):
        CDRW_siteUrl.append(CDRW_url+KeyList[z])
    
    print("CDRW url 합체 완료")

    #CDRW 찾기
    for y in range(0,len(CDRW_siteUrl)):
        response = requests.get(CDRW_siteUrl[y], verify=False)
        xtree = ET.fromstring(response.content)
        record_identifier_element = xtree.find(xpath_expression, namespaces=namespace_map)
        
        # Check if the element is found before accessing its text content
        if record_identifier_element is not None:
            value = record_identifier_element.text
            CDRW_keys.append(value)
        else:
            print("!CDRW를 못찾음!")
    
    print("CDRW 값 크롤링 완료")
    return CDRW_keys

def searchCNTS(TotalCnt, CNTSkeyList):
    
    for i in range(1,999):
        
        #타이틀 onclick에서 CNTS 값 가져옴
        title_CNTS = driver.find_elements(By.CSS_SELECTOR, 'div > div.textBox > div.resultTitle > p > a')
        #썸네일 가져옴
        img_CNTS = driver.find_elements(By.CSS_SELECTOR,'div > div.imgBox > img')
        doScrollDown(5)

        #컬렉션 내 CNTS 코드 추출해서 List 저장
        for k in title_CNTS:
            #onclick 자체를 긁어옴
            tmp_key = k.get_attribute('onclick')
            # ' 로 문자열 나눔
            val1 = tmp_key.split("'")
            #CNTS 값이 있는 3번 리스트만 빼옴
            CNTSkeyList.append(val1[3])
        
        #썸네일 수집 
        for x in img_CNTS:
            tmp_src = x.get_attribute('src')
            ThumbnailList.append(tmp_src)
        #ListImg = searchImg(ListImg)       
        
        #N초간 스크롤
        print('i 루프 [',i,']회차 돎')

        #마지막 페이지에는 '다음 페이지' 버튼이 없음
        if i < TotalCnt:
            #다음 페이지 버튼을 찾음
            next_btn = driver.find_element(By.CSS_SELECTOR, "p > a.btn-paging.next")
            next_btn.click()
            print(i,'회차, 다음 페이지 이동')
            time.sleep(3)

        #1page 밖에 없으면 예외처리 
        if TotalCnt == 1:
            break

        #마지막 페이지까지 수집하고 루프 끝냄
        if i == TotalCnt:
            print("CNTS 크롤링 끝")
            break

    return CNTSkeyList


#썸네일 다운로드
def downloadThumbnail(ImgList,CNTSkeyList):
    print("썸네일 네이밍 및 다운로드 중")
    i = 0
    for link in ImgList:
        urlretrieve(link,savePath+f'{CNTSkeyList[i]}.jpg')
        i += 1
        time.sleep(0.3)



try: 
    element = WebDriverWait(driver, 5).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR,'#paging_btn_go_page'))
    )
    #CNTS를 가져올 List 초기화
    CNTS_key = list()

    #찾으려는 페이지 끝 번호 가져오는데 / 있어서 자르고 형변환 시킴, 두자리수까지 대응
    TotalCount = int(driver.find_element(By.CLASS_NAME,'TotalCount').text[-2:])
    action = driver.find_element(By.CSS_SELECTOR, 'body')
    
    print('총 페이지 수: ', TotalCount)
    CNTS_key = searchCNTS(TotalCount, CNTS_key)
    


except TimeoutException:
    print('해당 페이지 정보 없음')   
    

finally:
    print("CNTS 데이터 수집 끝나고 처리중")

    #9 중복값 제거
    CNTS_final = list(dict.fromkeys(CNTS_key))
    #컬렉션 썸네일 제거
    #if final not in FileThumnail: del final[0]
    
    print("CNTS 원본 갯수 :",len(CNTS_key))

    #CDRW 찾는 함수, CDWR 리스트로 받고  
    CDRW_final = searchCDRW(CNTS_final)
    
    #뉴스때문에 중복제거 
    #CDRW_final = list(dict.fromkeys(CDRW_final))
    print("CDRW 중복 제거(CNTS랑 동일함) : ",len(CDRW_final),'\n')

    #컬렉션 썸네일 중복제거 (Python comprehension 사용)
    ThumbnailList = [item for item in ThumbnailList if word not in item]

    #썸네일 다운로드
    downloadThumbnail(ThumbnailList,CNTS_final)

    
    #리스트를 엑셀저장
    #final_df = pd.DataFrame(final, columns=['CNTS'])
    final_df = pd.DataFrame({'CNTS': CNTS_final, 'CDRW': CDRW_final})

    #index는 1부터 시작
    final_df.index = final_df.index + 1

    #모든 열을 행으로 이동
    pd.melt(final_df)

    #CSV로 저장
    final_df.to_csv(f'CNTS_df.csv', mode='w', encoding='utf-8-sig',header=True, index=True)
    print('csv 저장완료')
    #print(*sorted(final))
    
    #브라우저 종료
    driver.quit()