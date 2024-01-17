import os
import shutil
import concurrent.futures
import numpy as np
import pandas as pd
from PIL import Image

def CosineSimilarity(img1, img2):
    arr1 = np.array(img1)
    arr2 = np.array(img2)
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
def LoadingThumbnail(dir):
    Path = list()
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith('.jpg'):
                Path.append(os.path.join(root, file))
    return Path

def main():
    #썸네일 오류만 한 폴더에 몰아넣기
    file_to = '/Users/user/Pictures/OASIS/재난아카이브_코로나19/Result'
    #썸네일 디렉토리
    dir = '/Users/user/Pictures/OASIS/재난아카이브_코로나19'
    #필터 기준 NA: 썸네일 없음, NEWS: 뉴스 썸네일
    ImgNA_path = '/Users/user/Pictures/OASIS/Thumnails/NA.jpg'
    ImgNEWS_path = '/Users/user/Pictures/OASIS/Thumnails/NEWS.jpg'
    
    imgPath = list()
    imgArray = list()
    imgName = list()
    NA_score = list()
    NEWS_score = list()
    NA_count = 0
    NEWS_count = 0

    imgPath = LoadingThumbnail(dir)
    
    #썸네일 파일명 수집
    for path in imgPath:
        imgName.append(os.path.basename(path))

    for array in imgPath:
        tmpImg = Image.open(array)
        imgArray.append(np.array(tmpImg))

    NA = Image.open(ImgNA_path)
    NA_array = np.array(NA)
    
    NEWS = Image.open(ImgNEWS_path)
    NEWS_array = np.array(NEWS)
    
    #NA
    for (nalist, filename) in zip(imgArray, imgName):
        na_score = CosineSimilarity(NA_array, nalist)
        if na_score >= 1:
            #print(na_score)
            NA_score.append(filename)
            NA_count += 1
        else:
            NA_score.append(None)
    #NEWS
    for (newslist, filename) in zip(imgArray, imgName):
        news_score = CosineSimilarity(NEWS_array, newslist)
        if news_score >= 1:
            #print(news_score)
            NEWS_score.append(filename)
            NEWS_count += 1
        else:
            NEWS_score.append(None)    
    
    #Result 폴더가 없으면 만들고
    if not os.path.exists(file_to):
        os.mkdir(file_to)
    
    #Result에다가 복사
    for (na, news) in zip(NA_score, NEWS_score):
        try:
            if na is not None and os.path.exists(dir):
                print("Copying:", na)
                shutil.copyfile(os.path.join(dir, na), os.path.join(file_to, os.path.basename(na)))

            if news is not None and os.path.exists(dir):
                print("Copying:", news)
                shutil.copyfile(os.path.join(dir, news), os.path.join(file_to, os.path.basename(news)))
    
        except Exception as e:
            print("파일 복사 에러:", e)


    result = pd.DataFrame({'NA': NA_score, 'NEWS' : NEWS_score})
    result.index = result.index+1
    pd.melt(result)
    result.to_csv(f'Filltered_Thumbnail.csv', mode='w', encoding='utf-8-sig', header=True, index=True)

    print(f'썸네일 없음: {NA_count}, 뉴스: {NEWS_count}, 분류 결과 엑셀 저장 완료')

    
#main 먼저 실행
if __name__ == '__main__':
    main()