# Python Selenium-Project-NL_OASIS_crawler
국립중앙도서관 OASIS 웹사이트 데이터 점검 크롤러 

## 프로젝트 소개
https://www.nl.go.kr/oasis/

### OASIS 내 재난 아카이브, 컬렉션의 정보 수집:
- 표준부호/번호 CNTS 코드 수집
- CNTS 별 대응하는 CDRW 코드를 XML에서 파싱
- 웹사이트 별 썸네일 수집 후 CNTS 코드로 네이밍
- 오류 확인용 썸네일 다운로드 (용량문제로 이미지 리사이징 처리)
- 최종 CNTS, CDRW 리스트 csv 파일로 저장
- (추가예정) 유사한 썸네일, 뉴스, 썸네일 없음 검출 기능

### 특징
- Selenium: OASIS 웹사이트 CNTS, CDRW(XML), 썸네일 크롤링 
- Pillow: 썸네일 이미지 리사이징(140*95px)
- ThreadPoolExecutor: 병렬 처리로 크롤링 속도 향상 
- Pandas DataFrame: 크롤링 결과 csv 저장
- Namespace로 웹 XML 파일 파싱

## 개발환경
- Python 3.12(64 bit)
- Chrome WebDriver (120.0.6099.109 기준)
- IDE: VS code 1.85.1

