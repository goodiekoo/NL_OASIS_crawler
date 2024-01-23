from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
#from selenium.webdriver.chrome.options import Options
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
# 크롬 드라이버 자동 업데이트
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
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
from filecmp import cmp
import ssl 
import logging
import datetime
import requests
import pandas as pd 
import time
import os
import xml.etree.ElementTree as ET
import io
import shutil
import aiofiles
import asyncio
import numpy as np

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

#xml 파싱용
CDRW_url = 'https://www.nl.go.kr/oasis/common/mods_view_xml.do?contentsId='
#cnts_df 저장경로 (없을경우 새로 생성)
savePath = r'./CrawlingResults'

#많은 파일 열 때 리미트 제한
semaphore = asyncio.Semaphore(100)

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
    CDRW_siteUrl = list(map(lambda key: CDRW_url + key, KeyList))

    #ThreadPool병렬처리
    print("CDRW url 작업 완료, xml 크롤링 시작")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = list(map(lambda site_url: executor.submit(fetchCDRWkey, site_url),CDRW_siteUrl))
    
    #결과 받아오기
    CDRW_keys= [future.result() for future in as_completed(futures) if future.result() is not None]
    print("CDRW 값 크롤링 완료")
    return CDRW_keys

#CNTS 수집
def OASISCrawler(url, driver, CNTSkeyList):
    driver.get(url)
    
    #이동버튼 나올때까지 최대 10초 대기 (보통 맨 나중에 나옴)
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR,'#paging_btn_go_page'))
    )
    
    TotalCount = int(driver.find_element(By.CLASS_NAME, 'TotalCount').get_attribute('data-total_page'))
    print('총 페이지 수: ', TotalCount)


    for i in range(1, TotalCount+1):
        print(f"Step1. CNTS 크롤링 시작 - 페이지 {i}")

        #타이틀 onclick에서 CNTS 값 가져옴
        title_CNTS = [a.get_attribute('onclick').split("'")[3] for a in driver.find_elements(By.CSS_SELECTOR, 'div > div.textBox > div.resultTitle > p > a')]
        #썸네일 가져옴
        img_CNTS = [img.get_attribute('src') for img in driver.find_elements(By.CSS_SELECTOR, 'div > div.imgBox > img')]

        CNTSkeyList.extend(title_CNTS)
        ThumbnailList.extend(img_CNTS)
        
        #N초간 스크롤
        doScrollDown(driver, 5)
        print(f'현재 {i} 페이지 스크롤 완료')

        #마지막 페이지까지 수집하고 루프 종료
        if i == TotalCount:
            print("마지막 페이지까지 CNTS 크롤링 끝.")
            break

        #다음 페이지 버튼을 찾음
        driver.find_element(By.CSS_SELECTOR, "p > a.btn-paging.next").click()
        print(f'{i}회차, 다음 페이지 이동')
        time.sleep(5)

    return CNTSkeyList

#썸네일 리사이징 + CNTS 네이밍
def DownloadandProcessImg(args):
    link, cnts_key = args
    try:
        with session.get(link, timeout=3) as res:
            res.raise_for_status()
            img_content = BytesIO(res.content)
            img = Image.open(img_content)
    
            # PNG인 경우 처리 
            if img.mode == 'RGBA':
                img = img.convert('RGB')
        
            resized_img = img.resize(target_size)
            resized_img.save(os.path.join(savePath, f'{cnts_key}.jpg'))
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
        executor.map(lambda args: DownloadandProcessImg(args), args_list)

#Cosine 유사도로 썸네일 오류 검출
def CosineSimilarity(img1, img2):
    arr1 = np.array(img1)
    arr2 = np.array(img2)
    #print(f'arr1.shape: {arr1.shape}, arr2.shape: {arr2.shape}')  
    assert arr1.shape == arr2.shape

    h, w, c = arr1.shape
    len_vec = h * w * c
    vec1 = arr1.reshape(len_vec, ) / 255.
    vec2 = arr2.reshape(len_vec, ) / 255.

    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)

    if norm_vec1 == 0 or norm_vec2 == 0:
        cosine_similarity =0 
    else:
        cosine_similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
    
    return cosine_similarity

#썸네일 위치 불러오기
async def LoadThumbnailPath(directory):
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.jpg')]

#썸네일 이미지 불러오기
async def LoadThumbnailImg(thumbnail_paths):
    return await asyncio.gather(*[AsyncLoadThumbnail(path) for path in thumbnail_paths])

#비동기로 썸네일 불러오기
async def AsyncLoadThumbnail(path):
    try: 
        async with semaphore:
            async with aiofiles.open(path,'rb') as file:
                imgData = await file.read()
                imgarray = np.array(Image.open(io.BytesIO(imgData)))
                return imgarray
    except Exception as e:
        print(f"썸네일 로딩중 오류 발생 : {e}")

async def CheckingExeptionsofThumbnail():
    #썸네일 오류만 한 폴더에 몰아넣기
    file_to = r'./CrawlingResults/Result'
    #썸네일 디렉토리
    dir = r'./CrawlingResults'
    #필터 기준 NA: 썸네일 없음, NEWS: 뉴스 썸네일
    ImgNA_path = r'./NA.jpg'
    ImgNEWS_path = r'./NEWS.jpg'

    #썸네일 파일이 있는 디렉토리 수집
    imgPath = await LoadThumbnailPath(dir)
    
    #썸네일 파일명 수집
    imgName = list(map(os.path.basename, imgPath))
    #이미지 비교를 위해 NumPyArray로 변환
    imgArray = await LoadThumbnailImg(imgPath)
    #imgArray = list(map(lambda path: np.array(Image.open(path)),imgPath))

    #검출용 파일 NA: 썸네일 없음 | NEWS: 뉴스 썸네일 | Dup: 중복 의심
    NA_array = np.array(Image.open(ImgNA_path))
    NEWS_array = np.array(Image.open(ImgNEWS_path))
    #중복 검출용은 순서 섞어버림
    Dup_array = imgArray

    #NA
    global NA_score
    NA_score = [filename if CosineSimilarity(NA_array, img) >= 1 else None for img, filename in zip(imgArray, imgName)]
    #NA_count = sum(score is not None for score in NA_score)
    
    #NEWS
    global NEWS_score
    NEWS_score = [filename if CosineSimilarity(NEWS_array, img) >= 1 else None for img, filename in zip(imgArray, imgName)]
    #NEWS_count = sum(score is not None for score in NEWS_score)

    #Dup
    #global Dup_score
    #Dup_score = list()
    #Dup_score = [filename1 if not cmp(filename1, filename2) and CosineSimilarity(img1, img2) >= 1 else None for (img1, filename1) in zip(imgArray, imgName) for (img2, filename2) in zip(Dup_array, imgName)]
    #for i, (img1, filename1) in enumerate(zip(imgArray, imgName)):
    #    for j, (img2, filename2) in enumerate(zip(Dup_array, imgName)):
        # Skip processing if the filenames are the same or if comparing the same image
    #        if filename1 == filename2:
    #            Dup_score.append(None)
    #            continue

        # Check for similarity and append to Dup_score
    #        if CosineSimilarity(img1, img2) >= 1:
    #            Dup_score.append(filename1)
    #        else:
    #            Dup_score.append(None)
    
    #Result 폴더가 없으면 만들고
    if not os.path.exists(file_to):
        os.mkdir(file_to)
    
    #Result에다가 복사
    for (na, news) in zip(NA_score, NEWS_score):
        try:
            for item in (na, news):
                if item is not None and os.path.exists(dir):
                    destination = os.path.join(file_to, os.path.basename(item))
                    print("Copying:", item)
                    shutil.copyfile(os.path.join(dir, item), destination)

        except Exception as e:
            print("파일 복사 에러:", e)
    

#main
def main():
    try:
        #컬렉션 썸네일 필터용 키워드 (중복)
        word = "FILE-"
        raw_url = input("반드시 콘텐츠 목록에서 '더보기' 누른후 url 입력 : ")
        #컬렉션 넘버만 추출해서 복사
        url = f"https://www.nl.go.kr/oasis/contents/O2010000.do?page=1&pageUnit=1000&schM=search_list&schType=disa&schTab=list&{raw_url.split('&')[5]}&facetKind=01"
        print(url)
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
            CNTS_key = executor.submit(OASISCrawler, url, driver.driver, CNTS_key)

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
        asyncio.run(CheckingExeptionsofThumbnail())        
        
    except Exception as e:
        logger.exception(f"오류 발생!: {e}")
    
    finally:
        logger.info(f"CNTS: {len(CNTS_final)}, CDRW: {len(CDRW_final)}")
    
    #Step3. (2-1) CNTS, CDRW 결과 엑셀 다운로드
    final_df = pd.DataFrame({'CNTS': CNTS_final, 'CDRW': CDRW_final, 'N/A': NA_score, 'NEWS': NEWS_score})
    final_df.index = final_df.index + 1
    pd.melt(final_df)
    final_df.to_csv(os.path.join(savePath,f'NL_CrawlingResult.csv'), mode='w', encoding='utf-8-sig',header=True, index=True)
    print('Step4. csv까지 저장완료. 끝.')
    
    driver.teardown()
    
#main 먼저 실행
if __name__ == '__main__':

    start_time = time.time() 
    main()
    end_time = time.time()	
    duration = end_time - start_time
    print(f'프로그램 실행 시간: {duration}')