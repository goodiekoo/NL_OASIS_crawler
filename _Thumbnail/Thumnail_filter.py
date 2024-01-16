import os
import shutil
import numpy as np
import concurrent.futures
import pandas as pd
from PIL import Image
from io import BytesIO

#def Covert_Img_to_NumpyArray(folderPath):
def LoadImgfromDir(folderPath):
    imgPath = list()
    for (root, dir, files) in os.walk(folderPath):
        for file in files:
            if '.jpg' in file:
                imgPath.append(os.path.join(root, file))   
    return imgPath

#dir에서 NumPyarray로 불러옴
def ConvertImgtoNumpyArray(imgList):
    ArrayList = list()
    for img in imgList:
        tmp = np.array(img)
        ArrayList.append(tmp)
    return ArrayList

#cosine 유사도 측정을 위해 10 * 10 리사이징
def ResizeImg(imagePaths, resize_shape = (10, 10)):
    resizedImages = list()
    
    for imgPath in imagePaths:
        resizeImg = Image.open(imgPath)
        resizeImg = resizeImg.resize(resize_shape)
        resizedImages.append(resizeImg)

    return resizedImages

def CosineSimilarity(img1, img2):
    try:
        array1 = np.array(img1)
        array2 = np.array(img2)
        assert array1.shape == array2.shape
    except AssertionError:
        print(f"Shapes of array1: {array1.shape}, array2: {array2.shape}")
        raise

    h, w, c = array1.shape
    len_vec = h * w * c
    vector1 = array1.reshape(len_vec, ) / 255.
    vector2 = array2.reshape(len_vec, ) / 255.

    cosine_similarity = np.dot(vector1, vector2) / (np.linalg.norm(vector1) * np.linalg.norm(vector2))
    return cosine_similarity

def ProcessImg(args):
    NA_array, NEWS_array, img_array, img_path = args
    NA_score = CosineSimilarity(NA_array, img_array)
    NEWS_score = CosineSimilarity(NEWS_array, img_array)

    if NA_score >= 1.0:
        print(f"썸네일 없음 : {img_path[-20:]}")

    if NEWS_score >= 1.0:
        print(f"뉴스 : {img_path[-20:]}")

def main():
    #분류할 썸네일
    dir = r'/Users/user/Pictures/OASIS/도쿄올림픽'
    new_dir = 'results'
    ImgNA_path = r'/Users/user/Pictures/OASIS/Thumnails/NA.jpg'
    ImgNEWS_path = r'/Users/user/Pictures/OASIS/Thumnails/NEWS.jpg'

    #썸네일 불러오기
    Img_Path = LoadImgfromDir(dir)
    #썸네일 파일명
    fileName = [k.split("/")[-1] for k in Img_Path]

    resizedImgList = ResizeImg(Img_Path)
    arrayofImgList = ConvertImgtoNumpyArray(resizedImgList)

    #구분용 NA와 NEWS 썸네일은 미리 해놨음
    NA = Image.open(ImgNA_path)
    NA_array = np.array(NA)
    NEWS = Image.open(ImgNEWS_path)
    NEWS_array = np.array(NEWS)

    args_list = [(NA_array, NEWS_array, img_array, img_path) for img_array, img_path in zip(arrayofImgList, fileName)]
    
    filtered_results = []

    for args in args_list:
        NA_score = CosineSimilarity(args[0], args[2])
        NEWS_score = CosineSimilarity(args[1], args[2])

        if NA_score >= 1.0 or NEWS_score >= 1.0:
            filtered_results.append((args[3], 'NA' if NA_score >= 1.0 else 'NEWS'))

    print(f"Number of filtered results: {len(filtered_results)}")
    print("Filtered Results:")
    print(filtered_results)

    # Create a DataFrame
    df = pd.DataFrame(filtered_results, columns=['File Name', 'Label'])

    try:
        # Save the DataFrame to an Excel file
        df.to_excel('filtered_results.xlsx', index=False)
        print("Excel file saved successfully.")
    except Exception as e:
        print(f"Error saving Excel file: {e}")

#main 먼저 실행
if __name__ == '__main__':
    main()

