# Python Selenium-Project-NL_OASIS_crawler
국립중앙도서관 OASIS 웹사이트(wayback) 데이터 점검 크롤러 

## 프로젝트 소개
https://www.nl.go.kr/oasis/

### OASIS 내 재난 아카이브, 컬렉션의 정보 수집:
- 표준부호/번호 CNTS 코드 수집
- CNTS 별 대응하는 CDRW 코드를 XML에서 파싱
- 웹사이트 별 썸네일 수집 후 CNTS 코드 부여
- CNTS, CDRW 리스트 csv 파일로 저장

## 개발환경
- Python 3.12(64 bit)
- Chrome WebDriver (120.0.6099.109 기준)
- Selenium 
- IDE: VS code
