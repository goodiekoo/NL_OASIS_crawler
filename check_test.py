from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
# 크롬 드라이버 자동 업데이트
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from chromedriver_autoinstaller import install
from PIL import Image
from io import BytesIO
from selenium.webdriver.support import expected_conditions as EC
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
from lxml.html import fromstring
from concurrent.futures import ThreadPoolExecutor, wait 
from concurrent.futures import as_completed, TimeoutError
import ssl 
import logging
import datetime
import requests
import pandas as pd 
import time
import os
import xml.etree.ElementTree as ET

#Warning msg 없앰
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#ssl 인증 해제
ssl._create_default_https_context = ssl._create_unverified_context
#log 세팅
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#requests 속도 향상
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)

#크롤링 할 url, 최종버전에서는 입력받음
url = 'https://www.nl.go.kr/oasis/contents/O2010000.do?page=1&pageUnit=1000&schM=search_list&schType=disa&schTab=list&schIsFa=ISU-000000000376&facetKind=01'

#xml 파싱용
CDRW_url = 'https://www.nl.go.kr/oasis/common/mods_view_xml.do?contentsId='
#cnts_df 저장경로
savePath = r'/Users/user/Pictures/OASIS/'
#컬렉션 썸네일 필터용 키워드 (중복)
word = "FILE-"

#로딩 최적화 CLASS
class SeleniumDriver:
    def __init__(self):
        self.driver = None
    
    def setup(self, options=None):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        chrome_options.page_load_strategy = 'none'

        if options:
            for opt in options:
                chrome_options.add_argument(opt)
        
        install()
        chromedriver_path = install(cwd=True)
        self.driver = webdriver.Chrome(service=ChromeService(chromedriver_path), options=chrome_options)

    def teardown(self):
        if self.driver:
            self.driver.quit()

#페이지 스크롤
def doScrollDown(driver, whileSeconds, sleep_duration=1):
    start = datetime.datetime.now()
    end = start + datetime.timedelta(seconds=whileSeconds)
    
    while datetime.datetime.now() <= end:
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        time.sleep(sleep_duration)

    #스크롤 끝난거 확인
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.TAG_NAME, 'footer'))
        )
    except TimeoutException:
        print("스크롤 시간초과. 일부 데이터가 누락되었을지도 모름")

def fetchCDRWkey(site_url):
    #네임스페이스 prefix 써서 XPATH 오류 방지 xml:mods
    namespace_map = {"mods": "http://www.loc.gov/mods/v3"}
    xpath_expression = ".//mods:recordIdentifier"
    try:
        #웹 XML 불러오는거라 재활용 불가능...
        with session.get(site_url, verify=False) as response:
            response.raise_for_status()
            xtree = ET.fromstring(response.content)
            record_identifier_element = xtree.find(xpath_expression, namespaces=namespace_map)

            if record_identifier_element is not None:
                return record_identifier_element.text
            else:
                print(f"해당 XML에서 CDRW을 찾을 수 없음 : {site_url}")
                return None
    except (requests.RequestException, ET.ParseError) as e :
        print(f"{site_url} 에서 오류 {e} 발생 ")
        return None

#CNTS 값 토대로 CDRW 가져오기 
def searchCDRW(KeyList):
    #CNTS를 key로 CDRW 값을 가져올 List 초기화
    CDRW_keys = list()

    #xml 사이트 url 합체
    #CDRW_siteUrl = [CDRW_url + key for key in KeyList]
    #lambda-map으로 바꿈
    CDRW_siteUrl = list(map(lambda key: CDRW_url + key, KeyList))

    #ThreadPool병렬처리
    print("CDRW url 작업 완료, xml 크롤링 시작")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = list(map(lambda site_url: executor.submit(fetchCDRWkey, site_url),CDRW_siteUrl))
        #futures = [executor.submit(fetchCDRWkey, site_url) for site_url in CDRW_siteUrl]
    
    #결과 받아오기
    CDRW_keys= [future.result() for future in as_completed(futures) if future.result() is not None]
    print("CDRW 값 크롤링 완료")
    return CDRW_keys

#CNTS 수집
def OASISCrawler(driver, CNTSkeyList):
    driver.get(url)
    
    #이동버튼 나올때까지 최대 20초 대기 (보통 맨 나중에 나옴)
    WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR,'#paging_btn_go_page'))
    )
    
    #찾으려는 페이지 끝 번호 가져오는데 / 있어서 자르고 형변환 시킴, 두자리수까지 대응
    TotalCount = int(driver.find_element(By.CLASS_NAME,'TotalCount').text[-2:])
    print('총 페이지 수: ', TotalCount)


    for i in range(1, TotalCount+1):
        
        print(f"Step1. CNTS 크롤링 시작 - 페이지 {i}")
        #타이틀 onclick에서 CNTS 값 가져옴
        title_CNTS = driver.find_elements(By.CSS_SELECTOR, 'div > div.textBox > div.resultTitle > p > a')
        #썸네일 가져옴
        img_CNTS = driver.find_elements(By.CSS_SELECTOR,'div > div.imgBox > img')
        
        #배치처리
        tmp_keys = [k.get_attribute('onclick').split("'")[3]for k in title_CNTS]
        CNTSkeyList.extend(tmp_keys)

        tmp_srcs = [x.get_attribute('src') for x in img_CNTS]
        ThumbnailList.extend(tmp_srcs)
        
        #N초간 스크롤
        doScrollDown(driver, 5)
        print(f'현재 {i} 페이지 스크롤 완료')

        #마지막 페이지까지 수집하고 루프 종료
        if i == TotalCount:
            print("마지막 페이지까지 CNTS 크롤링 끝.")
            break

        #다음 페이지 버튼을 찾음
        next_btn = driver.find_element(By.CSS_SELECTOR, "p > a.btn-paging.next")
        next_btn.click()
        print(f'{i}회차, 다음 페이지 이동')
        time.sleep(5)

    return CNTSkeyList

#썸네일 리사이징 + CNTS 네이밍
def download_and_process_img(args):
    link, cnts_key = args
    try:
        with session.get(link, timeout=10) as res:
            res.raise_for_status()  # Check if the request was successful
            img_content = BytesIO(res.content)
            img = Image.open(img_content)
    
            # PNG인 경우 처리 
            if img.mode == 'RGBA':
                img = img.convert('RGB')
        
            resized_img = img.resize(target_size)
            resized_img.save(savePath + f'{cnts_key}.jpg')
            print(f"썸네일 {cnts_key} 다운 및 리사이징 성공 \n")

    except requests.RequestException as req_exc:
        print(f"Request exception occurred for {cnts_key}: {req_exc}")
    except Exception as e:
        print(f"다운로드, 변환, 리사이징 오류 발생 {cnts_key}: {e}")

#썸네일 다운로드
def downloadThumbnail(ImgList,CNTSkeyList):
    global target_size
    target_size = (140, 95) 
    print("Step3. 썸네일 CNTS 네이밍 및 다운로드 중")
    #폴더가 없다면 생성 예외처리
    if not os.path.exists(savePath):
        os.mkdir(savePath)
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        args_list = zip(ImgList, CNTSkeyList)
        executor.map(lambda args: download_and_process_img(args), args_list)

#main
def main():
    try:
        driver = SeleniumDriver()
        driver.setup()
    
        #썸네일은 전역으로 일단 처리
        global ThumbnailList
        ThumbnailList = list()
        CNTS_key = list() 
        CNTS_final = list() 
        CDRW_final = list()

        with ThreadPoolExecutor(max_workers=3) as executor:
            #Step1. [MainThread] CNTS, Thumbnail src(global) 수집 
            CNTS_key = executor.submit(OASISCrawler, driver.driver, CNTS_key)

            try:
                wait([CNTS_key],timeout=3)
                # CNTS 값 받아오고 3초 대기
                CNTS_final = CNTS_key.result()
                # Step2. [SubThread] (1) CDRW 찾기 
                tmp = executor.submit(searchCDRW,CNTS_final) 
                wait([tmp], timeout=3)
                # CDRW 값 받아오고 3초 대기
                CDRW_final = tmp.result()
                #컬렉션 썸네일 중복제거 (Python comprehension 사용)
                ThumbnailList = [item for item in ThumbnailList if word not in item]
                #Step2. [SubThread] (2) 썸네일 CNTS 네이밍 및 다운로드 (시간 소요 커서 map+lambda)
                executor.map(lambda args: downloadThumbnail(*args), zip([ThumbnailList],[CNTS_final]))

            except TimeoutError:
                logger.error("시간 초과. 스레드가 예상 시간 내에 처리되지 않음")

    except Exception as e:
        logger.exception(f"오류 발생: {e}")
    
    finally:
        logger.info(f"CNTS: {len(CNTS_final)}, CDRW: {len(CDRW_final)}")
    
    #Step3. (2-1) CNTS, CDRW 결과 엑셀 다운로드
    final_df = pd.DataFrame({'CNTS': CNTS_final, 'CDRW': CDRW_final})
    final_df.index = final_df.index + 1
    pd.melt(final_df)
    final_df.to_csv(f'CNTS_df.csv', mode='w', encoding='utf-8-sig',header=True, index=True)
    print('Step4. csv까지 저장완료. 끝.')
    
    driver.teardown()
    
#main 먼저 실행
if __name__ == '__main__':

    start_time = time.time() 
    main()
    end_time = time.time()	
    duration = end_time - start_time
    print(f'프로그램 실행 시간: {duration}')