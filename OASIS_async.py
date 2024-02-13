from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
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
import io
import shutil
import aiofiles
import aiohttp
import asyncio
import numpy as np

#Warning msg 없앰
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#ssl 인증 해제
ssl._create_default_https_context = ssl._create_unverified_context
#log 세팅
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#async 많은 파일 열 때 리미트 제한
semaphore = asyncio.Semaphore(100)

#Selenium 로딩 최적화 CLASS
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

#CNTS 크롤링 과정에서 페이지 스크롤
def DoScrollDown(driver, whileSeconds, sleep_duration=1):
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

#OASIS Url 입력받아서 처리
async def FetchUrls():
    try:
        print("##### 국립중앙도서관 OASIS 웹사이트 유효성 검사 ##### ")
        collectionKey = "schIsFa=ISU-"
        rawUrl = input("\n!!!반드시 *콘텐츠 목록*에서 *더보기* 누른 후 url 입력(ctrl+c 후 우클릭)!!!: \n")
    
        if rawUrl.find(collectionKey) != -1:
            collectionCode = rawUrl.split('&')[5]
            goUrl = f"https://www.nl.go.kr/oasis/contents/O2010000.do?page=1&pageUnit=1000&schM=search_list&schType=disa&schTab=list&{collectionCode}&facetKind=01"
            return goUrl

        else:
            print("!!!오류!!! OASIS *콘텐츠 목록*에서 *더보기* 누른 후 가져온 url이 맞나요? 프로그램을 다시 시작해주세요.")
            await asyncio.sleep(5)
            exit(0)                
    
    except Exception as e:
        logger.exception(f"오류 발생!: {e}")

#CNTS, Thumbnail 다운로드 src 키 크롤링
async def FetchCNTS(url):
    #Selenium 준비
    driver = SeleniumDriver()
    driver.setup()
    driver.driver.get(url)
    CNTSkeyList = list()
    
    #이동버튼 나올때까지 최대 5초 대기 (보통 맨 나중에 나옴)
    WebDriverWait(driver.driver, 3).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR,'#paging_btn_go_page'))
    )
    
    TotalCount = int(driver.driver.find_element(By.CLASS_NAME, 'TotalCount').get_attribute('data-total_page'))
    print('총 페이지 수: ', TotalCount)


    for i in range(1, TotalCount+1):
        print(f"{i}/{TotalCount} pages 내 CNTS 크롤링 중...")

        #타이틀 onclick에서 CNTS 값 가져옴
        title_CNTS = [a.get_attribute('onclick').split("'")[3] for a in driver.driver.find_elements(By.CSS_SELECTOR, 'div > div.textBox > div.resultTitle > p > a')]
        #썸네일 가져옴
        img_CNTS = [img.get_attribute('src') for img in driver.driver.find_elements(By.CSS_SELECTOR, 'div > div.imgBox > img')]

        CNTSkeyList.extend(title_CNTS)
        Thumbnail_List.extend(img_CNTS)
        
        #N초간 스크롤
        DoScrollDown(driver.driver, 2)

        #마지막 페이지까지 수집하고 루프 종료
        if i == TotalCount:
            print(f"CNTS: {len(CNTSkeyList)}, 썸네일 src: {len(Thumbnail_List)} 종 크롤링 완료. 브라우저가 닫힙니다.")
            await asyncio.sleep(1)
            driver.teardown()
            break

        #다음 페이지 버튼을 찾음
        driver.driver.find_element(By.CSS_SELECTOR, "p > a.btn-paging.next").click()
        print(f'{i}/{TotalCount} 페이지 완료, 다음 페이지로 이동합니다...')
        time.sleep(2)

    return CNTSkeyList

#웹 xml CDRW 키 추출
async def fetchCDRWkey(session, site_url):
    #네임스페이스 prefix 써서 XPATH 오류 방지 xml:mods
    namespace_map = {"mods": "http://www.loc.gov/mods/v3"}
    xpath_expression = ".//mods:recordIdentifier"
    try:
        #웹 XML 불러오는거라 재활용 불가능...
        async with session.get(site_url) as response:
            response.raise_for_status()
            CDRW_content = await response.text()
            xtree = ET.fromstring(CDRW_content)
            record_identifier_element = xtree.find(xpath_expression, namespaces=namespace_map)

            if record_identifier_element is not None:
                return record_identifier_element.text
            else:
                print(f"!!!오류!!!해당 XML에서 CDRW을 찾을 수 없습니다: {site_url}")
                return ""
            
    except (requests.RequestException, ET.ParseError) as e :
        print(f"!!!오류발생!!!: {e}")
        return ""

#CNTS 값 토대로 CDRW 웹 xml url 생성 처리 
async def searchCDRW(KeyList, CDRWUrl):
    
    #대기
    await asyncio.sleep(1)
    try:
        print("개별 xml마다 CDRW 추출을 위해 url 생성을 시작합니다...")
        #CNTS를 key로 CDRW 값을 가져올 List 초기화
        CDRW_keys = list()
        CDRW_siteUrl = list(map(lambda key: CDRWUrl + key, KeyList))
        print(f"CDRW url {len(CDRW_siteUrl)} 개 작업 완료, CDRW 키를 추출합니다...")

        async with aiohttp.ClientSession() as session:
            futures = [asyncio.ensure_future(fetchCDRWkey(session, site_url)) for site_url in CDRW_siteUrl]
            #CDRW 결과값 받아옴
            CDRW_keys = await asyncio.gather(*futures)
        
        print(f"CDRW: {len(CDRW_keys)}, 크롤링 완료")
        return CDRW_keys
  
    except Exception as e:
        logger.exception(f"오류 발생!: {e}")

#썸네일 BytesIO 변환 및 리사이징
async def FetchThumbnail(session, src_urls, target_size): 
    try:
        resized_img = list()
        async with session.get(src_urls) as res:
            res.raise_for_status()
            img_content = BytesIO(await res.read())
            img = Image.open(img_content)
    
            # PNG인 경우 처리 
            if img.mode == 'RGBA':
                img = img.convert('RGB')

            resized_img = img.resize(target_size)
            return resized_img

    except Exception as e:
        print(f"!!!오류발생!!!: 썸네일 {e}")

#src키로 썸네일(140*95) 다운로드(파일명 CNTS)
async def DownloadThumbnail(savePath, ImgList, CNTSkeyList):
    target_size = (140, 95)
    cnt = 0
    #대기
    await asyncio.sleep(1) 
    try:
        print(f"컬렉션 썸네일 제거 후 총: {len(ImgList)}, CNTS 네이밍 및 다운로드 중...")
        ImgDownloadList = list()

        #폴더 없으면 생성
        if not os.path.exists(savePath):
            os.mkdir(savePath)

        async with aiohttp.ClientSession() as session:
            futures = [
                FetchThumbnail(session, srcUrls, target_size)
                for srcUrls in ImgList
            ]

            #이미지 결과값 받아옴
            ImgDownloadList = await asyncio.gather(*futures)
        
        #다운로드
        for file, name in zip(ImgDownloadList, CNTSkeyList):
            try:
                file.save(os.path.join(savePath, f'{name}.jpg'))
                cnt += 1
                print(f"{cnt}번 썸네일: {name} 다운로드 완료. \n")
            except Exception as e:
                print(f"!!!오류발생!!!: {name}: {e}")

    except Exception as e:
        print(f"!!!오류발생!!!: {e}")

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

async def MoveErrorResult(savePath):
    #썸네일 오류만 한 폴더에 몰아넣기
    file_to = r'./ThumbnailsErrorResult'
    #Result 폴더가 없으면 만들고
    if not os.path.exists(file_to):
        os.makedirs(file_to)
    
    #Result에다가 복사
    for (na, news) in zip(NA_score, NEWS_score):
            for item in (na, news):
                if item is not None and os.path.exists(file_to):
                    destination = os.path.join(file_to, os.path.basename(item))
                    print("썸네일 이동중:", item)
                    shutil.copyfile(os.path.join(savePath, item), destination)

async def CalcThumbnailScoreN(imgA, imgB, imgName):
    try:
        result = list()
            #비교 조건 array가 하나만 있을 경우
        for img, filename in zip(imgB, imgName):
            if CosineSimilarity(imgA, img) >= 1.0:
                result.append(filename)
            else:
                result.append(None)
        return result
    
    except Exception as e:
        print(f"!!!N/A, NEWS 오류 검색중 오류발생!!! {e}")

#중복 검사 오류 있음
async def CalcThumbnailScoreDup(imgA, imgB, imgName):
    try:
        result = list()
        highscore = list()
        for img1, filename1 in zip(imgB, imgName):
            #match_found = False
            for img2, filename2 in zip(imgA, imgName):
                #이름 동일한 중복 이미지일 경우 스킵
                if filename1 == filename2:
                    continue
                    
                if CosineSimilarity(img1, img2) >= 1.0:
                    highscore.append(filename2)
                    #match_found = True
                    break

                #if not match_found:
                    #result.append(None)

            result.append(highscore)
            highscore.clear()
        return result
    except Exception as e:
        print(f"!!!오류발생!!! {e}")

        
async def GetSimilarityScore(imgPath, numpyArr):
        await asyncio.sleep(1)
        try:
            
            #필터 기준 NA: 썸네일 없음, NEWS: 뉴스 썸네일
            ImgNA_path = r'./NA.jpg'
            ImgNEWS_path = r'./NEWS.jpg'
            #검출용 파일 NA: 썸네일 없음 | NEWS: 뉴스 썸네일 | Dup: 중복 의심
            
            global NA_array
            NA_array = np.array(Image.open(ImgNA_path))
            global NEWS_array
            NEWS_array = np.array(Image.open(ImgNEWS_path))
            
            #썸네일 파일명 수집
            imgName = list(map(os.path.basename, imgPath))
            #중복 검출용
            #Dup_array = numpyArr
            
            print("썸네일 오류 검사 시작")
            TaskScores = await asyncio.gather(
                CalcThumbnailScoreN(NA_array,numpyArr,imgName),
                CalcThumbnailScoreN(NEWS_array,numpyArr,imgName)
                #,CalcThumbnailScoreDup(Dup_array,numpyArr,imgName)
            )

            await asyncio.sleep(1)

            global NA_score #NA
            NA_score = TaskScores[0]
            global NEWS_score #NEWS
            NEWS_score = TaskScores[1]

            await asyncio.sleep(1)
            await MoveErrorResult(savePath)

            #for na, news in zip(NA_score, NEWS_score):
            #    tmp = 
            #    if i or j ==
            #global Dup_score #Dup
            #Dup_array = NA_score + NEWS_score

            #print(NA_score)
            #print(NEWS_score)
            #print("##### 중복 썸네일 검사 결과 #####")
            #print(Dup_score)
            print(f"NA: {len(NA_score)} | NEWS: {len(NEWS_score)}" ) #| {len(Dup_score)}")

        except Exception as e:
            print(f"!!!오류발생!!!:{e}")        
    

async def CheckingExeptionsofThumbnail(savePath):
    try:
        imgPath = await LoadThumbnailPath(savePath)
        print("썸네일오류: 디렉토리 수집 완료")
        
        imgArray = await LoadThumbnailImg(imgPath)
        print("썸네일오류: NUMpyArry 변환완료")

        await asyncio.sleep(1)

        await GetSimilarityScore(imgPath, imgArray)
        
    except Exception as e:
        print(f"!!!오류발생!!!:{e}")
    
    #finally:
    #    await MoveErrorResult(savePath)

async def main():
    try:
        #컬렉션 썸네일 필터 키워드
        word = "FILE-"
        #cnts_df 저장경로 (없을경우 새로 생성)
        global savePath
        savePath = r'./CrawlingResults'
        #CDRW xml 파싱용 url
        CDRW_url = 'https://www.nl.go.kr/oasis/common/mods_view_xml.do?contentsId='

        #썸네일은 전역으로 일단 처리
        global Thumbnail_List
        Thumbnail_List = list()
        CNTS_List = list() 
        CDRW_List = list()

        #url 가져옴
        Task1_FetchUrl = asyncio.create_task(FetchUrls()) 
        await Task1_FetchUrl
        url = Task1_FetchUrl.result()
        
        await asyncio.sleep(1)

        #CNTS 가져옴
        Task2_FetchCNTS = asyncio.create_task(FetchCNTS(url))
        await Task2_FetchCNTS
        CNTS_List = Task2_FetchCNTS.result()
        
        #컬렉션 썸네일 중복제거 (Python comprehension 사용)
        Thumbnail_List = [item for item in Thumbnail_List if word not in item]

        #https://www.nl.go.kr/oasis/contents/O2010000.do?page=1&pageUnit=10&schM=search_list&schType=disa&schTab=list&schIsFa=ISU-000000000388&facetKind=01

        Task3_CDRWandThumbnails = await asyncio.gather(
           DownloadThumbnail(savePath, Thumbnail_List, CNTS_List), #[0]
           searchCDRW(CNTS_List,CDRW_url) #.result() 안써도 됨 [1] 
        )        
        Task4_CheckE = asyncio.create_task(CheckingExeptionsofThumbnail(savePath))
        
        await asyncio.sleep(1)

        CDRW_List = Task3_CDRWandThumbnails[1]
        await Task4_CheckE

        final_df = pd.DataFrame({'CNTS': CNTS_List, 'CDRW': CDRW_List, 'N/A': NA_score, 'NEWS': NEWS_score})
        final_df.index = final_df.index + 1
        pd.melt(final_df)
        final_df.to_csv(os.path.join(savePath,f'NL_CrawlingResult.csv'), mode='w', encoding='utf-8-sig',header=True, index=True)
        print('.csv까지 저장완료. 크롤링 끝.')

        logger.info(f"CNTS: {len(CNTS_List)}, CDRW: {len(CDRW_List)}")


    except Exception as e:
        logger.exception(f"오류 발생!: {e}")

        
if __name__ == '__main__':
    start_time = time.time() 
    asyncio.run(main())
    print(f'프로그램 실행 시간: {time.time()-start_time}')