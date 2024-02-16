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
from lxml.html import fromstring
from rich.console import Console
from rich.progress import Progress
from rich.panel import Panel
from rich.theme import Theme
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

#ssl 인증 해제
ssl._create_default_https_context = ssl._create_unverified_context
#log 세팅
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#async 많은 파일 열 때 리미트 제한
global semaphore
semaphore = asyncio.Semaphore(100)

#CLI용 rich
custom_theme = Theme({
    "info": "dim cyan",
    "m": "magenta",
    "danger": "bold red",
    "msg":"bold yellow underline on red",
    "sys":"bold green"
})

console = Console(theme=custom_theme)

#파일 부를 때 오류 방지 (절대경로)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

#Selenium 로딩 최적화 CLASS
class SeleniumDriver:
    def __init__(self):
        self.driver = None
    
    def setup(self, options=None):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument('--lang=ko_KR.UTF-8')
        
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
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'footer'))
        )
    except TimeoutException:
        console.print("스크롤 시간초과. 일부 데이터가 누락되었을지도 모름", style="danger")

#OASIS Url 입력받아서 처리
async def FetchUrls():
    try:
        console.clear(home=True)
        console.print(Panel("[sys]주제·이슈 컬렉션, 재난 아카이브 점검 크롤러[/sys]", title="국립중앙도서관-OASIS", title_align="right"))
        collectionKey = "schIsFa=ISU-"
        rawUrl = console.input("[msg] ⭣ ⭣ ⭣ 반드시 *콘텐츠 목록*에서 *더보기* 클릭 후 url 입력 (ctrl+c 후 우클릭) ⭣ ⭣ ⭣ \n")
    
        if rawUrl.find(collectionKey) != -1:
            global collectionCode
            collectionCode = rawUrl.split('&')[5]
            goUrl = f"https://www.nl.go.kr/oasis/contents/O2010000.do?page=1&pageUnit=1000&schM=search_list&schType=disa&schTab=list&{collectionCode}&facetKind=01"
            return goUrl

        else:
            console.print("!!!오류!!! OASIS *콘텐츠 목록*에서 *더보기* 누른 후 가져온 url이 맞나요? 프로그램을 재시작해주세요.", style="danger")
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
    #콜렉션 명 가져옴
    global CollectionTitle
    CollectionTitle = driver.driver.find_element(By.CLASS_NAME, 'emp1').text
    TotalCount = int(driver.driver.find_element(By.CLASS_NAME, 'TotalCount').get_attribute('data-total_page'))

    for i in range(1, TotalCount+1):
        #타이틀 onclick에서 CNTS 값 가져옴
        title_CNTS = [a.get_attribute('onclick').split("'")[3] for a in driver.driver.find_elements(By.CSS_SELECTOR, 'div > div.textBox > div.resultTitle > p > a')]
        #썸네일 가져옴
        img_CNTS = [img.get_attribute('src') for img in driver.driver.find_elements(By.CSS_SELECTOR, 'div > div.imgBox > img')]

        CNTSkeyList.extend(title_CNTS)
        Thumbnail_List.extend(img_CNTS)
        
        #N초간 스크롤
        with console.status(f"[info]<{CollectionTitle}> {i}/{TotalCount} pages 내 CNTS 크롤링 중...[/info]", spinner="point"):
            DoScrollDown(driver.driver, 2)

        #마지막 페이지까지 수집하고 루프 종료
        if i == TotalCount:
            console.print(f"[sys]<{CollectionTitle}>의 CNTS: {len(CNTSkeyList)}, 썸네일: {len(Thumbnail_List)} 크롤링 완료. 브라우저가 닫힙니다.[/sys]")
            await asyncio.sleep(1)
            driver.teardown()
            break

        #다음 페이지 버튼을 찾음
        driver.driver.find_element(By.CSS_SELECTOR, "p > a.btn-paging.next").click()
        console.print(f'[sys]<{CollectionTitle}> {i}/{TotalCount} pages 작업 완료, 다음 페이지로 이동합니다.[/sys]')        
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
                console.print(f"!!!오류!!!해당 XML에서 CDRW을 찾을 수 없습니다: {site_url}", style="danger")
                return ""
            
    except (requests.RequestException, ET.ParseError) as e :
        console.print(f"!!!오류발생!!!: {e}", style="danger")
        return ""

#CNTS 값 토대로 CDRW 웹 xml url 생성 처리 
async def searchCDRW(KeyList, CDRWUrl):
    try:
        #대기
        await asyncio.sleep(1)
        #console.print("[info]CDRW 추출작업을 시작합니다.[/info]")
        with console.status(f"[info]<{CollectionTitle}> CDRW 크롤링 중...[/info]", spinner="point"):
            #CNTS를 key로 CDRW 값을 가져올 List 초기화
            CDRW_keys = list()
            CDRW_siteUrl = list(map(lambda key: CDRWUrl + key, KeyList))
        
            async with aiohttp.ClientSession() as session:
                futures = [asyncio.ensure_future(fetchCDRWkey(session, site_url)) for site_url in CDRW_siteUrl]
                #CDRW 결과값 받아옴
                CDRW_keys = await asyncio.gather(*futures)
        
        console.print(f"[sys]<{CollectionTitle}> CDRW: {len(CDRW_keys)}, 크롤링 완료.[/sys]")
        return CDRW_keys
  
    except Exception as e:
        logger.exception(f"!!!오류 발생!!!: {e}")

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
        console.print(f"!!!오류발생!!!: 썸네일 {e}", style="danger")

#src키로 썸네일(140*95) 다운로드(파일명 CNTS)
async def DownloadThumbnail(savePath, ImgList, CNTSkeyList):
    #방화벽 문제로 썸네일 다운로드 실패한게 있는지 확인
    global is_failed_to_download_Thumbnails 
    is_failed_to_download_Thumbnails = False
    
    target_size = (140, 95)
    cnt = 0
    failed_downloads = set()
    try:
        #대기
        await asyncio.sleep(1)
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

        with Progress() as progress:
            task = progress.add_task("[cyan]썸네일 다운로드: ",  total=len(CNTSkeyList), start=False)
            #다운로드
            for file, name in zip(ImgDownloadList, CNTSkeyList):
                try:
                    progress.update(task, advance=1, message=f"[cyan][{cnt}]번 썸네일: {name} 다운로드 중...")
                    if file is not None:
                        file.save(os.path.join(savePath, f'{name}.jpg'))             
                    else:
                        is_failed_to_download_Thumbnails = True
                        if name not in failed_downloads:
                            failed_downloads.add(name)
                            console.log(f"!!!오류발생!!!: {name}: 다운로드 실패", style="danger")
                except Exception as e:
                    is_failed_to_download_Thumbnails = True
                    if name not in failed_downloads:
                        failed_downloads.add(name)
                        console.log(f"!!!오류발생!!!: {name}: {e}", style="danger")
    except Exception as e:
        console.print(f"!!!오류발생!!!: {e}",style="danger")
    
    finally:
        progress.stop()

        if failed_downloads:
            console.print(f"다음 썸네일 다운로드에 실패했습니다: {list(failed_downloads)}", style="danger")

    

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
        console.print(f"!!!오류!!! 썸네일 로딩 불가 : {e}", style="danger")

async def MoveErrorResult(savePath):
    #썸네일 오류만 한 폴더에 몰아넣기
    file_to = r'./ThumbnailErrors'
    #Result 폴더가 없으면 만들고
    if not os.path.exists(file_to):
        os.makedirs(file_to)
    
    #Result에다가 복사
    for (na, news) in zip(NA_score, NEWS_score):
            for item in (na, news):
                if item is not None and os.path.exists(file_to):
                    destination = os.path.join(file_to, os.path.basename(item))
                    console.print(f"오류 썸네일: {item} 이동",style="m")
                    shutil.copyfile(os.path.join(savePath, item), destination)

async def CalcThumbnailScoreN(imgA, imgB, imgName):
    try:
        result = list(map(lambda x: x[1] if CosineSimilarity(imgA, x[0]) >= 1.0 else None, zip(imgB, imgName)))
        return result
    
    except Exception as e:
        console.print(f"!!!N/A, NEWS 오류 검색중 오류발생!!! {e}", style="danger")

#중복 검사 오류 있음
async def CalcThumbnailScoreDup(imgA, imgB, imgName):
    try:
        result = list()
        for img1, filename1 in zip(imgB, imgName):
            dup_found = any(filename1 == filename2 for filename2 in imgName)
            if not dup_found:
                result.append(None)
            else:
                score = [filename2 for img2, filename2 in zip(imgA, imgName) if CosineSimilarity(img1, img2) >= 1.0]
                result.append(len(score))
        return result
    except Exception as e:
        console.print(f"!!!오류발생!!! {e}",style="danger")
        
    
async def CheckingExeptionsofThumbnail(savePath):
    try:
        #task_description = f"<{CollectionTitle}>의 썸네일 오류 검사중..."
        #with Progress() as progress:
        #    task = progress.add_task("[cyan]{task_description}", total=100, start=False)
        with console.status(f"[m]<{CollectionTitle}>의 썸네일 오류 검사중...", spinner="point"):
            imgPath = await LoadThumbnailPath(savePath)
            #print("오류 썸네일 검출: 썸네일 파일 경로 수집 완료.")
        
            imgArray = await LoadThumbnailImg(imgPath)
            #print("썸네일오류: NUMpyArry 변환완료")

            await asyncio.sleep(1)

            await GetSimilarityScore(imgPath, imgArray)
        
    except Exception as e:
        console.print(f"!!!오류발생!!!:{e}",style="danger") 

async def GetSimilarityScore(imgPath, numpyArr):
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
            Dup_array = list(reversed(numpyArr))

            TaskScores = await asyncio.gather(
                        CalcThumbnailScoreN(NA_array,numpyArr,imgName),
                        CalcThumbnailScoreN(NEWS_array,numpyArr,imgName),
                        CalcThumbnailScoreDup(Dup_array,numpyArr,imgName)
                    )

            await asyncio.sleep(1)

            global NA_score #NA
            NA_score = TaskScores[0]
            global NEWS_score #NEWS
            NEWS_score = TaskScores[1]
            global Dup_score
            Dup_score = TaskScores[2]   

            await MoveErrorResult(savePath)

            console.print(f"[sys]검사 완료 - NA: {len(NA_score)} | NEWS: {len(NEWS_score)} | Dup: {len(Dup_score)}[/sys]")

        except Exception as e:
            console.print(f"!!!오류발생!!!:{e}", style="danger")        

async def SavetoResults(CNTS_List, CDRW_List):
        final_df = pd.DataFrame({
                'CNTS': CNTS_List, 'CDRW': CDRW_List, 
                'N/A': NA_score, 
                'NEWS': NEWS_score, 
                'Dup': Dup_score
            })
        final_df.index = final_df.index + 1
        pd.melt(final_df)

        def draw_color_MAX(x,color):
            color = color = f'background-color:{color}'
            is_max = x > 5
            return [color if b else '' for b in is_max] 
        
        #final_df.info()

        #.xlsx 저장
        excel_file_path = os.path.join(savePath, 'NL_CrawlingResult.xlsx')
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            final_df.style.apply(
                draw_color_MAX, color='#ff9090',subset=['Dup'],axis=0).to_excel(
                    writer, index=True, header=True)
        
        #final_df.to_excel(os.path.join(savePath,f'NL_CrawlingResult.xlsx'), index=True, header=True, encoding='utf-8-sig')
        #logger.info(f"CNTS: {len(CNTS_List)}, CDRW: {len(CDRW_List)}")


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

        Task3_CDRWandThumbnails = await asyncio.gather(
           DownloadThumbnail(savePath, Thumbnail_List, CNTS_List), #[0]
           searchCDRW(CNTS_List,CDRW_url) #.result() 안써도 됨 [1] 
        )        
        
        await asyncio.sleep(1)
        CDRW_List = Task3_CDRWandThumbnails[1]

        #모든 썸네일을 다운로드하는데 성공했을 경우, 엑셀까지 저장
        if is_failed_to_download_Thumbnails is not True:
            Task4_CheckE = asyncio.create_task(CheckingExeptionsofThumbnail(savePath))
            await asyncio.sleep(1)
            await Task4_CheckE
            Task5_SaveE = asyncio.create_task(SavetoResults(CNTS_List, CDRW_List))
            await Task5_SaveE
            console.print(f'<{CollectionTitle}> 크롤링 결과 엑셀 저장 완료. 프로그램 재시작 시 꼭 [u]CrawlingResults, ThumbnailErrors 폴더 삭제[/u] 해주세요.', style="msg")

        else:
            console.print(f'썸네일 다운로드 오류로 더 이상 <{CollectionTitle}> 크롤링 작업을 진행할 수 없습니다. \n[u]기존 CrawlingResults 삭제 및 Windows 방화벽 설정 조정 후 프로그램을 재시작[/u] 해주세요.', style="msg")

    except Exception as e:
        logger.exception(f"오류 발생!: {e}")

        
if __name__ == '__main__':
    start_time = time.time() 
    asyncio.run(main())
    console.print(f'프로그램 실행 시간: {time.time()-start_time}', style="info")