import os
import io
import shutil
import aiofiles
import asyncio
import time
import numpy as np
import pandas as pd
from PIL import Image

#많은양 파일 열때 리미트 제한
semaphore = asyncio.Semaphore(100)

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

async def AsyncLoadThumbnail(path):
    try: 
        async with semaphore:
            async with aiofiles.open(path,'rb') as file:
                imgData = await file.read()
                imgarray = np.array(Image.open(io.BytesIO(imgData)))
                return imgarray
    except Exception as e:
        print(f"썸네일 로딩중 오류 발생 : {e}")
    
    
#썸네일 위치 불러오기
async def LoadThumbnailPath(directory):
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.jpg')]

async def LoadThumbnailImg(thumbnail_paths):
    return await asyncio.gather(*[AsyncLoadThumbnail(path) for path in thumbnail_paths])

async def main():
    #썸네일 오류만 한 폴더에 몰아넣기
    file_to = '/Users/user/Pictures/CrawlingResults/Result'
    #썸네일 디렉토리
    dir = '/Users/user/Pictures/CrawlingResults'
    #필터 기준 NA: 썸네일 없음, NEWS: 뉴스 썸네일
    ImgNA_path = '/Users/user/Pictures/OASIS/Thumnails/NA.jpg'
    ImgNEWS_path = '/Users/user/Pictures/OASIS/Thumnails/NEWS.jpg'

    #썸네일 파일이 있는 디렉토리 수집
    imgPath = await LoadThumbnailPath(dir)
    
    #썸네일 파일명 수집
    imgName = list(map(os.path.basename, imgPath))
    #이미지 비교를 위해 NumPyArray로 변환
    imgArray = await LoadThumbnailImg(imgPath)
    #imgArray = list(map(lambda path: np.array(Image.open(path)),imgPath))

    #검출용 파일 NA: 썸네일 없음 | NEWS: 뉴스 썸네일
    NA_array = np.array(Image.open(ImgNA_path))
    NEWS_array = np.array(Image.open(ImgNEWS_path))
    
    #NA
    NA_score = [filename if CosineSimilarity(NA_array, img) >= 1 else None for img, filename in zip(imgArray, imgName)]
    NA_count = sum(score is not None for score in NA_score)
    
    #NEWS
    NEWS_score = [filename if CosineSimilarity(NEWS_array, img) >= 1 else None for img, filename in zip(imgArray, imgName)]
    NEWS_count = sum(score is not None for score in NEWS_score)
    
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

    result = pd.DataFrame({'NA: {NA_count}': NA_score, 'NEWS: {NEWS_count}' : NEWS_score})
    result.index = result.index+1
    pd.melt(result)
    result.to_csv(os.path.join(file_to, f'Thumbnail_Error_List.csv'), mode='w', encoding='utf-8-sig', header=True, index=True)

    print(f'썸네일 없음: {NA_count}, 뉴스: {NEWS_count}, 분류 결과 엑셀 저장 완료')

    
#main 먼저 실행
if __name__ == '__main__':
    start_time = time.time() 
    asyncio.run(main())
    end_time = time.time()	
    duration = end_time - start_time
    print(f'프로그램 실행 시간: {duration}')