# Python Selenium-Project-NL_OASIS_crawler
국립중앙도서관 OASIS 웹사이트 데이터 점검 크롤러 

## 프로젝트 소개
https://www.nl.go.kr/oasis/

### OASIS_async.py (_py 폴더 내 위치)
(1) OASIS 내 재난 아카이브, 컬렉션의 정보 수집:
- 표준부호/번호 CNTS 코드 수집
- CNTS 별 대응하는 CDRW 코드를 XML에서 파싱
- 웹사이트 별 썸네일 수집 후 CNTS 코드로 네이밍
- 오류 확인용 썸네일 다운로드 (용량문제로 이미지 리사이징 처리)
- 최종 CNTS, CDRW, 썸네일 오류 리스트 csv 파일로 저장

(2) 수집된 썸네일의 뉴스, 오류 검출
- Cosine 유사도를 통해 오류 이미지와 얼마나 유사한지 확인
- aiofiles, asyncio로 많은 이미지를 빠르게 처리
- 오류 썸네일은 Result 폴더 생성 후 복사
- 중복 의심 썸네일 검출 (5개 이상 발견 시 셀 색상 처리)

#### 특징
- Selenium: OASIS 웹사이트 CNTS, CDRW(XML), 썸네일 크롤링 
- Pillow: 썸네일 이미지 리사이징(140*95px)
- Asyncio, Asynchttp: 비동기 처리로 크롤링 및 많은 파일 처리 속도 대폭 향상 
- Pandas DataFrame: 크롤링 결과 csv 저장

## 개발환경
- Python 3.10(64 bit)
- Chrome WebDriver (120.0.6099.109 기준)
- IDE: VS code 1.85.1

