# Python Selenium-Project-NL_OASIS_crawler
National Library of Korea OASIS Website Data Validation Crawler

## Project Overview
https://www.nl.go.kr/oasis/
This project is a Python-based crawler designed to automatically collect and validate disaster archive and collection information from the OASIS website of the National Library of Korea.


### Key Features
1. Data Collection (OASIS_async.py)
- Collect CNTS Codes: Gathers CNTS (standard number) codes from disaster archives and collections within OASIS.
- Parse CDRW Codes: Extracts CDRW codes corresponding to CNTS from XML files.
- Collect and Rename Thumbnails: Downloads thumbnail images from the website and renames them using the CNTS codes.
- Download Thumbnails for Error Checking: Resizes images to manage file size, then downloads thumbnails for error detection.
- Save Error Lists: Saves the final lists of CNTS, CDRW, and thumbnail errors into CSV files.

2. Error Detection and Analysis
- Detect Image Similarity: Uses Cosine similarity to detect how closely thumbnails resemble known error images.
- Speed Up Image Processing with Asynchronous Techniques: Utilizes aiofiles and asyncio for rapid processing of large numbers of images.
- Manage Error Thumbnails: Copies detected error thumbnails into a dedicated Result folder.
- Detect Suspected Duplicate Thumbnails: Identifies suspected duplicate thumbnails and highlights cells if more than 5 duplicates are found.

#### Key Technologies Used
- Selenium: Crawls CNTS codes, CDRW (XML), and thumbnails from the OASIS website.
- Pillow: Resizes collected thumbnail images to 140x95px.
- Asyncio, Asynchttp: Enhances crawling and processing speed for large files using asynchronous processing.
- Pandas DataFrame: Saves crawling results into CSV files.

## Development Environment
- Python 3.10(64 bit)
- Chrome WebDriver (120.0.6099.109)
- IDE: VS code 1.85.1

