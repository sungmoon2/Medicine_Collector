#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
최적화된 의약품 이미지 재추출 스크립트
- 404 에러 페이지 건너뛰기
- 이미 이미지가 있는 파일 건너뛰기 옵션
- 처리 속도 최적화
"""

import os
import json
import logging
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
import random
import datetime
import sys

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("image_reextraction.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 기본 설정값 수정
DEFAULT_CONFIG = {
    'data_dir': './json_data',    # 의약품 JSON 파일 디렉토리
    'workers': 4,                 # 동시 작업자 수를 줄임 (과도한 요청 방지)
    'delay': 0.5,                 # 요청 간 지연 시간 증가
    'report_path': './image_reextraction_report.html', # 보고서 저장 경로
    'skip_existing': True,        # 이미 이미지가 있는 파일 건너뛰기
    'check_url_first': True,      # URL 유효성 검사 먼저 수행
    'timeout': 10,                # 요청 타임아웃 증가
    'batch_size': 50,             # 배치 크기 감소
    'retries': 3,                 # 재시도 횟수
    'retry_delay': 5              # 재시도 간 지연 시간(초)
}

# 세션 객체 생성 (연결 재사용)
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://terms.naver.com/'
})

# 404 페이지 캐시 (중복 요청 방지)
invalid_ids = set()

# 이미지 파서 함수 (개선된 버전)
def extract_medicine_image(soup):
    """
    의약품 이미지 정보 추출 - 정확한 태그에서만 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
            
    Returns:
        dict: 이미지 데이터 또는 빈 딕셔너리(이미지 없는 경우)
    """
    image_data = {}
    
    # 알려진 더미/빈 이미지 URL 패턴 목록
    dummy_image_patterns = [
        "e.gif", "blank.gif", "spacer.gif", "transparent.gif",
        "empty.png", "pixel.gif", "noimage", "no_img", "no-img",
        "img_x", "_blank", "loading.gif", "spinner.gif"
    ]
    
    try:
        # <span class="img_box"> 태그 찾기
        img_box_spans = soup.find_all('span', class_='img_box')
        
        # 이미지 박스가 없으면 빈 딕셔너리 반환 (이미지 없음)
        if not img_box_spans:
            logger.debug("의약품 이미지 태그(img_box)가 없습니다.")
            return {}
        
        # 이미지 박스 내에서 이미지 찾기
        for span in img_box_spans:
            # a 태그 찾기
            a_tag = span.find('a')
            if not a_tag:
                continue
                
            # img 태그 찾기
            img_tag = a_tag.find('img')
            if not img_tag:
                continue
            
            # 이미지 URL 추출 우선순위에 따라 처리
            img_url = None
            
            # 1. origin_src 속성 (고해상도 원본 이미지)
            if img_tag.has_attr('origin_src') and img_tag['origin_src']:
                img_url = img_tag['origin_src']
                image_data["image_quality"] = "high"
            
            # 2. src 속성 (중간 해상도 이미지)
            elif img_tag.has_attr('src') and img_tag['src']:
                img_url = img_tag['src']
                image_data["image_quality"] = "medium"
            
            # 3. data-src 속성 (대체 이미지)
            elif img_tag.has_attr('data-src') and img_tag['data-src']:
                img_url = img_tag['data-src']
                image_data["image_quality"] = "low"
            
            # 이미지 URL이 없으면 건너뛰기
            if not img_url:
                continue
                
            # 상대 경로를 절대 경로로 변환
            if not img_url.startswith(('http://', 'https://')):
                img_url = img_url.replace('data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', '')
                img_url = f"https://terms.naver.com{img_url}" if img_url.startswith('/') else f"https://terms.naver.com/{img_url}"
            
            # 더미 이미지 필터링
            if any(dummy in img_url.lower() for dummy in dummy_image_patterns):
                continue
            
            # 이미지 정보 추출
            image_data["image_url"] = img_url
            
            # 이미지 크기 정보 추출
            if img_tag.has_attr('width') and img_tag.has_attr('height'):
                image_data["image_width"] = img_tag['width']
                image_data["image_height"] = img_tag['height']
            
            # 원본 크기 정보 추출
            if img_tag.has_attr('origin_width') and img_tag.has_attr('origin_height'):
                image_data["original_width"] = img_tag['origin_width']
                image_data["original_height"] = img_tag['origin_height']
            
            # alt 정보 추출
            if img_tag.has_attr('alt'):
                image_data["image_alt"] = img_tag['alt']
            
            # 유효한 이미지를 찾으면 즉시 반환 (더 이상 찾지 않음)
            logger.debug(f"의약품 이미지 URL 추출 성공: {img_url}")
            return image_data
        
        # img_box는 있지만 유효한 이미지를 찾지 못한 경우
        logger.debug("img_box에서 유효한 이미지를 찾을 수 없습니다.")
        return {}
        
    except Exception as e:
        logger.warning(f"이미지 추출 중 예외 발생: {str(e)}")
        return {}  # 오류가 발생하면 빈 딕셔너리 반환
    
# 세션 객체 생성 및 설정 개선
def create_session():
    """
    향상된 연결 안정성을 위한 세션 객체 생성
    
    Returns:
        requests.Session: 설정된 세션 객체
    """
    session = requests.Session()
    
    # 안정적인 연결을 위한 설정
    adapter = requests.adapters.HTTPAdapter(
        max_retries=3,
        pool_connections=10,
        pool_maxsize=20
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # 헤더 설정
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',  # 연결 유지
        'Keep-Alive': 'timeout=60',  # 연결 유지 시간
        'Referer': 'https://terms.naver.com/'
    })
    
    return session

def check_url_exists(url, timeout=5):
    """
    URL이 유효한지 확인 (HEAD 요청)
    
    Args:
        url (str): 확인할 URL
        timeout (int): 요청 타임아웃(초)
        
    Returns:
        bool: URL이 유효하면 True, 그렇지 않으면 False
    """
    try:
        response = session.head(url, timeout=timeout, allow_redirects=True)
        return response.status_code == 200
    except:
        return False
    
# 메인 함수 시작 시 세션 생성
session = create_session()
    
def reset_session(session):
    """
    세션 객체 재설정 (강제 종료 후 재연결)
    
    Args:
        session (requests.Session): 재설정할 세션 객체
        
    Returns:
        requests.Session: 재설정된 세션 객체
    """
    if session:
        try:
            session.close()
        except:
            pass
    return create_session()

def fetch_medicine_page(medicine_id, config, session):
    """
    의약품 ID로 페이지 데이터 가져오기 (재시도 로직 강화)
    
    Args:
        medicine_id (str): 의약품 ID
        config (dict): 설정 정보
        session (requests.Session): 요청에 사용할 세션 객체
        
    Returns:
        BeautifulSoup: 파싱된 페이지 또는 None
    """
    # invalid_ids는 전역 변수이므로 global 선언
    global invalid_ids
    
    # 이미 404 오류가 확인된 ID는 건너뛰기
    if medicine_id in invalid_ids:
        return None
    
    url = f"https://terms.naver.com/entry.naver?docId={medicine_id}"
    timeout = config['timeout']
    retries = config['retries']
    retry_delay = config['retry_delay']
    
    # 재시도 로직
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            
            # 404 오류 확인
            if response.status_code == 404:
                invalid_ids.add(medicine_id)
                return None
                
            # 429 오류 (요청이 너무 많음)
            if response.status_code == 429:
                # 더 오래 대기 후 재시도
                wait_time = retry_delay * (2 ** attempt)  # 지수 백오프
                logger.warning(f"요청 한도 초과 (429). {wait_time}초 대기 후 재시도... (시도 {attempt+1}/{retries})")
                time.sleep(wait_time)
                continue
                
            # 기타 오류 코드
            if response.status_code != 200:
                logger.warning(f"HTTP 오류 {response.status_code}. 재시도 중... (시도 {attempt+1}/{retries})")
                time.sleep(retry_delay)
                continue
            
            # 페이지 파싱
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 성공적인 응답 확인
            if "검색어를 입력해 주세요." in response.text or "오류가 발생했습니다." in response.text:
                logger.warning(f"유효하지 않은 의약품 페이지 (ID: {medicine_id}). 재시도 중... (시도 {attempt+1}/{retries})")
                time.sleep(retry_delay)
                continue
                
            return soup
            
        except requests.exceptions.RequestException as e:
            # 연결 문제, 타임아웃 등
            if "404" in str(e):
                invalid_ids.add(medicine_id)
                return None
                
            logger.warning(f"페이지 요청 중 오류 (ID: {medicine_id}): {str(e)}. 재시도 중... (시도 {attempt+1}/{retries})")
            
            # 이미 파라미터로 받은 세션을 사용하므로 전역 세션 변수를 직접 변경하지 않음
            # 대신 메인 함수가 필요시 세션을 재설정할 수 있도록 함
            time.sleep(retry_delay)
            continue
            
        except Exception as e:
            logger.error(f"페이지 파싱 중 오류 (ID: {medicine_id}): {str(e)}")
            
            # 재시도가 남아있으면 계속
            if attempt < retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                return None
    
    # 모든 재시도 실패
    logger.error(f"최대 재시도 횟수 초과. 페이지를 가져올 수 없습니다. (ID: {medicine_id})")
    return None
    
def should_process_file(file_path, config):
    """
    파일을 처리해야 하는지 확인
    
    Args:
        file_path (str): 의약품 JSON 파일 경로
        config (dict): 설정 정보
        
    Returns:
        tuple: (처리 여부, 의약품 ID, 기존 이미지 URL)
    """
    if not config['skip_existing']:
        return True, None, None
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            medicine_data = json.load(f)
        
        # 기존 이미지 URL 확인
        old_image_url = medicine_data.get('image_url', None)
        
        # 고품질 이미지가 이미 있는 경우 건너뛰기
        if old_image_url and 'image_quality' in medicine_data and medicine_data['image_quality'] == 'high':
            return False, None, old_image_url
        
        # 의약품 ID 추출
        medicine_id = None
        
        # 1. URL에서 ID 추출 시도
        if 'url' in medicine_data and medicine_data['url']:
            url = medicine_data['url']
            if 'docId=' in url:
                medicine_id = url.split('docId=')[1].split('&')[0]
        
        # 2. ID 필드 확인
        if not medicine_id and 'medicine_id' in medicine_data:
            medicine_id = medicine_data['medicine_id']
            
        # 3. doc_id 필드 확인
        if not medicine_id and 'doc_id' in medicine_data:
            medicine_id = medicine_data['doc_id']
        
        # 이미 404 오류가 확인된 ID는 건너뛰기
        if medicine_id in invalid_ids:
            return False, medicine_id, old_image_url
        
        # URL 유효성 검사 먼저 수행 (옵션)
        if config['check_url_first'] and medicine_id:
            url = f"https://terms.naver.com/entry.naver?docId={medicine_id}"
            if not check_url_exists(url, config['timeout']):
                invalid_ids.add(medicine_id)  # 404 ID 캐시에 추가
                return False, medicine_id, old_image_url
        
        return True, medicine_id, old_image_url
    except Exception as e:
        logger.error(f"파일 확인 중 오류 ({file_path}): {str(e)}")
        return True, None, None  # 오류 발생 시 처리하도록 설정

def process_medicine_file(file_path, config, session):
    """
    의약품 파일 처리 및 이미지 업데이트 (기존 URL 보존)
    
    Args:
        file_path (str): 의약품 JSON 파일 경로
        config (dict): 설정 정보
        session (requests.Session): 요청에 사용할 세션 객체
        
    Returns:
        dict: 처리 결과 정보
    """
    result = {
        'file_path': file_path,
        'file_name': os.path.basename(file_path),
        'success': False,
        'message': "",
        'medicine_name': "",
        'medicine_id': None,
        'page_url': "",
        'old_image_url': "",
        'new_image_url': "",
        'image_quality': "",
        'has_image': False,
        'skipped': False
    }
    
    try:
        # JSON 파일 로드
        with open(file_path, 'r', encoding='utf-8') as f:
            medicine_data = json.load(f)
        
        # 의약품 이름 저장
        if 'korean_name' in medicine_data:
            result['medicine_name'] = medicine_data['korean_name']
        elif 'name' in medicine_data:
            result['medicine_name'] = medicine_data['name']
        
        # 기존 이미지 URL 저장 (모든 이미지 관련 필드)
        if 'image_url' in medicine_data:
            result['old_image_url'] = medicine_data['image_url']
        
        # 의약품 ID 추출
        medicine_id = None
        page_url = ""
        
        # 1. URL에서 ID 추출 시도
        if 'url' in medicine_data and medicine_data['url']:
            page_url = medicine_data['url']
            if 'docId=' in page_url:
                medicine_id = page_url.split('docId=')[1].split('&')[0]
        
        # 2. ID 필드 확인
        if not medicine_id and 'medicine_id' in medicine_data:
            medicine_id = medicine_data['medicine_id']
            page_url = f"https://terms.naver.com/entry.naver?docId={medicine_id}"
            
        # 3. doc_id 필드 확인
        if not medicine_id and 'doc_id' in medicine_data:
            medicine_id = medicine_data['doc_id']
            page_url = f"https://terms.naver.com/entry.naver?docId={medicine_id}"
        
        result['medicine_id'] = medicine_id
        result['page_url'] = page_url
        
        # ID가 없으면 처리 불가
        if not medicine_id:
            result['message'] = "의약품 ID를 찾을 수 없습니다."
            return result
        
        # 이미 404 오류가 확인된 ID는 건너뛰기
        if medicine_id in invalid_ids:
            result['skipped'] = True
            result['message'] = "유효하지 않은 ID (404 오류)"
            return result
        
        # 페이지 데이터 가져오기
        soup = fetch_medicine_page(medicine_id, config, session)
        if not soup:
            result['message'] = "페이지 데이터를 가져올 수 없습니다."
            return result
        
        # 이미지 데이터 추출
        image_data = extract_medicine_image(soup)
        
        # 이미지가 없는 경우
        if not image_data:
            # 기존 이미지 정보 저장 (삭제하기 전)
            old_image_data = {}
            for key in medicine_data:
                if key.startswith('image_'):
                    old_image_data[key] = medicine_data[key]
            
            # 기존 이미지 필드 삭제
            for key in list(medicine_data.keys()):
                if key.startswith('image_'):
                    del medicine_data[key]
            
            # 파일 업데이트
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(medicine_data, f, ensure_ascii=False, indent=2)
            
            result['success'] = True
            result['message'] = "이미지 없음으로 처리되었습니다."
            result['has_image'] = False
            
            # 기존에 이미지가 있었는지 확인
            if old_image_data and 'image_url' in old_image_data:
                result['old_image_url'] = old_image_data['image_url']
        else:
            # 새 이미지 URL 저장
            result['new_image_url'] = image_data.get('image_url', '')
            result['image_quality'] = image_data.get('image_quality', '')
            result['has_image'] = True
            
            # 기존 이미지 정보 저장 (삭제하기 전)
            old_image_data = {}
            for key in medicine_data:
                if key.startswith('image_'):
                    old_image_data[key] = medicine_data[key]
            
            # 기존 이미지 필드 삭제
            for key in list(medicine_data.keys()):
                if key.startswith('image_'):
                    del medicine_data[key]
            
            # 새로운 이미지 데이터 추가
            for key, value in image_data.items():
                medicine_data[f"image_{key}"] = value
            
            # 파일 업데이트
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(medicine_data, f, ensure_ascii=False, indent=2)
            
            result['success'] = True
            result['message'] = f"이미지가 업데이트되었습니다. 품질: {image_data.get('image_quality', 'unknown')}"
            
            # 기존에 이미지가 있었는지 확인
            if old_image_data and 'image_url' in old_image_data:
                result['old_image_url'] = old_image_data['image_url']
        
        return result
    
    except Exception as e:
        result['message'] = f"처리 중 오류 발생: {str(e)}"
        logger.error(f"파일 처리 중 오류 ({file_path}): {str(e)}")
        return result

def generate_html_report(results, report_path):
    """
    이미지 재추출 결과 HTML 보고서 생성 (페이지 URL 추가)
    
    Args:
        results (list): 처리 결과 목록
        report_path (str): 보고서 저장 경로
    """
    # 결과 통계 계산
    total = len(results)
    success = sum(1 for r in results if r['success'])
    with_image = sum(1 for r in results if r['success'] and r['has_image'])
    no_image = sum(1 for r in results if r['success'] and not r['has_image'])
    failed = sum(1 for r in results if not r['success'] and not r['skipped'])
    skipped = sum(1 for r in results if r['skipped'])
    
    # 현재 시간
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # HTML 템플릿 헤더 부분
    html = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>의약품 이미지 재추출 보고서</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1, h2 {{
                color: #2c3e50;
            }}
            .summary {{
                background-color: #f8f9fa;
                border-radius: 5px;
                padding: 15px;
                margin: 20px 0;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }}
            .summary h2 {{
                margin-top: 0;
            }}
            .stats {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .stat-item {{
                background-color: #fff;
                border-radius: 4px;
                padding: 10px 15px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                flex: 1;
                min-width: 150px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 12px 15px;
                border-bottom: 1px solid #ddd;
                text-align: left;
            }}
            th {{
                background-color: #2c3e50;
                color: white;
                position: sticky;
                top: 0;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #eaeaea;
            }}
            .success {{
                color: #28a745;
            }}
            .failed {{
                color: #dc3545;
            }}
            .skipped {{
                color: #6c757d;
            }}
            .image-cell {{
                max-width: 300px;
                word-break: break-all;
            }}
            .image-preview {{
                max-width: 100px;
                max-height: 100px;
                margin-top: 5px;
                border: 1px solid #ddd;
                border-radius: 4px;
            }}
            .quality-high {{
                color: #28a745;
                font-weight: bold;
            }}
            .quality-medium {{
                color: #fd7e14;
                font-weight: bold;
            }}
            .quality-low {{
                color: #dc3545;
            }}
            .search-container {{
                margin: 20px 0;
            }}
            #searchInput {{
                padding: 8px;
                width: 300px;
                border: 1px solid #ddd;
                border-radius: 4px;
            }}
            .pagination {{
                display: flex;
                list-style: none;
                padding: 0;
                margin: 20px 0;
                justify-content: center;
            }}
            .pagination li {{
                margin: 0 5px;
            }}
            .pagination a {{
                display: inline-block;
                padding: 8px 12px;
                border: 1px solid #ddd;
                border-radius: 4px;
                text-decoration: none;
                color: #333;
            }}
            .pagination a.active {{
                background-color: #2c3e50;
                color: white;
                border-color: #2c3e50;
            }}
            .filter-buttons {{
                margin: 20px 0;
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
            }}
            .filter-button {{
                padding: 8px 15px;
                border: none;
                border-radius: 4px;
                background-color: #f0f0f0;
                cursor: pointer;
            }}
            .filter-button.active {{
                background-color: #2c3e50;
                color: white;
            }}
            .url-link {{
                color: #1a73e8;
                text-decoration: none;
            }}
            .url-link:hover {{
                text-decoration: underline;
            }}
            .image-changed {{
                background-color: #e8f0fe;
            }}
            .empty-cell {{
                color: #999;
                font-style: italic;
            }}
        </style>
    </head>
    <body>
        <h1>의약품 이미지 재추출 보고서</h1>
        <p>생성 시간: {now}</p>
        
        <div class="summary">
            <h2>처리 요약</h2>
            <div class="stats">
                <div class="stat-item">
                    <h3>총 파일</h3>
                    <p>{total}개</p>
                </div>
                <div class="stat-item">
                    <h3>성공</h3>
                    <p>{success}개</p>
                </div>
                <div class="stat-item">
                    <h3>이미지 있음</h3>
                    <p>{with_image}개</p>
                </div>
                <div class="stat-item">
                    <h3>이미지 없음</h3>
                    <p>{no_image}개</p>
                </div>
                <div class="stat-item">
                    <h3>실패</h3>
                    <p>{failed}개</p>
                </div>
                <div class="stat-item">
                    <h3>건너뜀</h3>
                    <p>{skipped}개</p>
                </div>
            </div>
        </div>
        
        <div class="search-container">
            <input type="text" id="searchInput" placeholder="의약품 이름 또는 ID 검색..." onkeyup="searchTable()">
        </div>
        
        <div class="filter-buttons">
            <button class="filter-button active" onclick="filterTable('all')">전체 ({total})</button>
            <button class="filter-button" onclick="filterTable('success')">성공 ({success})</button>
            <button class="filter-button" onclick="filterTable('with-image')">이미지 있음 ({with_image})</button>
            <button class="filter-button" onclick="filterTable('no-image')">이미지 없음 ({no_image})</button>
            <button class="filter-button" onclick="filterTable('failed')">실패 ({failed})</button>
            <button class="filter-button" onclick="filterTable('skipped')">건너뜀 ({skipped})</button>
            <button class="filter-button" onclick="filterTable('changed')">이미지 변경됨</button>
        </div>
        
        <table id="resultsTable">
            <thead>
                <tr>
                    <th>No.</th>
                    <th>상태</th>
                    <th>의약품 이름</th>
                    <th>의약품 ID</th>
                    <th>페이지 URL</th>
                    <th>기존 이미지 URL</th>
                    <th>새 이미지 URL</th>
                    <th>이미지 품질</th>
                </tr>
            </thead>
            <tbody>
    """
    
    # 결과 행 추가
    for i, result in enumerate(results, 1):
        if result['skipped']:
            status_class = "skipped"
            status_text = "건너뜀"
        elif result['success']:
            status_class = "success"
            status_text = "성공"
        else:
            status_class = "failed"
            status_text = "실패"
            
        data_status = "skipped" if result['skipped'] else ("success" if result['success'] else "failed")
        data_image = "with-image" if result['has_image'] else "no-image"
        
        # 이미지 변경 여부 확인
        image_changed = result['old_image_url'] != result['new_image_url'] and result['new_image_url']
        row_class = "image-changed" if image_changed else ""
        data_changed = "changed" if image_changed else ""
        
        quality_class = ""
        if result['image_quality'] == 'high':
            quality_class = "quality-high"
        elif result['image_quality'] == 'medium':
            quality_class = "quality-medium"
        elif result['image_quality'] == 'low':
            quality_class = "quality-low"
        
        medicine_id = result['medicine_id'] or "-"
        page_url = result['page_url'] or "-"
        if page_url != "-" and medicine_id != "-":
            page_url_html = f'<a href="{page_url}" class="url-link" target="_blank">{page_url}</a>'
        else:
            page_url_html = '<span class="empty-cell">페이지 없음</span>'
            
        old_url = result['old_image_url'] or "-"
        new_url = result['new_image_url'] or "-"
        
        old_img_tag = ""
        new_img_tag = ""
        
        # 기존 이미지 미리보기 (있는 경우)
        if old_url != "-":
            old_img_tag = f'<img src="{old_url}" class="image-preview" onerror="this.style.display=\'none\'">'
            old_url_html = f'<a href="{old_url}" class="url-link" target="_blank">{old_url}</a><br>{old_img_tag}'
        else:
            old_url_html = '<span class="empty-cell">이미지 없음</span>'
            
        # 새 이미지 미리보기 (있는 경우)
        if new_url != "-":
            new_img_tag = f'<img src="{new_url}" class="image-preview" onerror="this.style.display=\'none\'">'
            new_url_html = f'<a href="{new_url}" class="url-link" target="_blank">{new_url}</a><br>{new_img_tag}'
        else:
            new_url_html = '<span class="empty-cell">이미지 없음</span>'
        
        html += f"""
            <tr class="{row_class}" data-status="{data_status}" data-image="{data_image}" data-changed="{data_changed}">
                <td>{i}</td>
                <td class="{status_class}">{status_text}</td>
                <td>{result['medicine_name'] or '<span class="empty-cell">알 수 없음</span>'}</td>
                <td>{medicine_id}</td>
                <td>{page_url_html}</td>
                <td class="image-cell">{old_url_html}</td>
                <td class="image-cell">{new_url_html}</td>
                <td class="{quality_class}">{result['image_quality'] or '<span class="empty-cell">없음</span>'}</td>
            </tr>
        """
    
    # HTML 마무리 (JavaScript 포함)
    html += """
            </tbody>
        </table>
        
        <div id="pagination" class="pagination"></div>
        
        <script>
            // 테이블 검색 기능
            function searchTable() {
                const input = document.getElementById('searchInput');
                const filter = input.value.toLowerCase();
                const table = document.getElementById('resultsTable');
                const rows = table.getElementsByTagName('tr');
                
                // 첫 번째 행은 헤더이므로 건너뜁니다
                for (let i = 1; i < rows.length; i++) {
                    const medicineName = rows[i].getElementsByTagName('td')[2].textContent.toLowerCase();
                    const medicineId = rows[i].getElementsByTagName('td')[3].textContent.toLowerCase();
                    const pageUrl = rows[i].getElementsByTagName('td')[4].textContent.toLowerCase();
                    
                    if (medicineName.indexOf(filter) > -1 || medicineId.indexOf(filter) > -1 || pageUrl.indexOf(filter) > -1) {
                        rows[i].style.display = '';
                    } else {
                        rows[i].style.display = 'none';
                    }
                }
                
                // 페이지네이션 재설정
                resetPagination();
            }
            
            // 테이블 필터링
            function filterTable(filter) {
                // 버튼 활성화 상태 변경
                const buttons = document.querySelectorAll('.filter-button');
                buttons.forEach(btn => {
                    btn.classList.remove('active');
                });
                event.currentTarget.classList.add('active');
                
                const table = document.getElementById('resultsTable');
                const rows = table.getElementsByTagName('tr');
                
                // 첫 번째 행은 헤더이므로 건너뜁니다
                for (let i = 1; i < rows.length; i++) {
                    const row = rows[i];
                    
                    if (filter === 'all') {
                        row.style.display = '';
                    } else if (filter === 'success') {
                        row.style.display = row.getAttribute('data-status') === 'success' ? '' : 'none';
                    } else if (filter === 'failed') {
                        row.style.display = row.getAttribute('data-status') === 'failed' ? '' : 'none';
                    } else if (filter === 'skipped') {
                        row.style.display = row.getAttribute('data-status') === 'skipped' ? '' : 'none';
                    } else if (filter === 'with-image') {
                        row.style.display = row.getAttribute('data-image') === 'with-image' ? '' : 'none';
                    } else if (filter === 'no-image') {
                        row.style.display = row.getAttribute('data-image') === 'no-image' ? '' : 'none';
                    } else if (filter === 'changed') {
                        row.style.display = row.getAttribute('data-changed') === 'changed' ? '' : 'none';
                    }
                }
                
                // 페이지네이션 재설정
                resetPagination();
            }
            
            // 페이지네이션 기능
            const rowsPerPage = 20;
            let currentPage = 1;
            
            function displayTable() {
                const table = document.getElementById('resultsTable');
                const rows = table.getElementsByTagName('tr');
                
                // 표시 가능한 행 찾기
                const visibleRows = [];
                for (let i = 1; i < rows.length; i++) {
                    if (rows[i].style.display !== 'none') {
                        visibleRows.push(rows[i]);
                    }
                }
                
                // 모든 행 숨기기
                for (let i = 0; i < visibleRows.length; i++) {
                    if (i >= ((currentPage - 1) * rowsPerPage) && i < (currentPage * rowsPerPage)) {
                        visibleRows[i].classList.remove('hidden-row');
                    } else {
                        visibleRows[i].classList.add('hidden-row');
                    }
                }
                
                // 페이지네이션 업데이트
                updatePagination(visibleRows.length);
            }
            
            function updatePagination(totalVisibleRows) {
                const totalPages = Math.ceil(totalVisibleRows / rowsPerPage);
                
                // 페이지네이션 엘리먼트 생성
                let paginationHTML = '';
                
                // 페이지 수가 많을 경우 일부만 표시
                const maxPageButtons = 10;
                let startPage = Math.max(1, currentPage - Math.floor(maxPageButtons / 2));
                let endPage = Math.min(totalPages, startPage + maxPageButtons - 1);
                
                if (endPage - startPage + 1 < maxPageButtons) {
                    startPage = Math.max(1, endPage - maxPageButtons + 1);
                }
                
                // 이전 버튼
                if (currentPage > 1) {
                    paginationHTML += `<li><a href="#" onclick="changePage(${currentPage - 1}); return false;">&laquo;</a></li>`;
                }
                
                // 첫 페이지
                if (startPage > 1) {
                    paginationHTML += `<li><a href="#" onclick="changePage(1); return false;">1</a></li>`;
                    if (startPage > 2) {
                        paginationHTML += `<li>...</li>`;
                    }
                }
                
                // 페이지 번호
                for (let i = startPage; i <= endPage; i++) {
                    const activeClass = i === currentPage ? 'class="active"' : '';
                    paginationHTML += `<li><a href="#" ${activeClass} onclick="changePage(${i}); return false;">${i}</a></li>`;
                }
                
                // 마지막 페이지
                if (endPage < totalPages) {
                    if (endPage < totalPages - 1) {
                        paginationHTML += `<li>...</li>`;
                    }
                    paginationHTML += `<li><a href="#" onclick="changePage(${totalPages}); return false;">${totalPages}</a></li>`;
                }
                
                // 다음 버튼
                if (currentPage < totalPages) {
                    paginationHTML += `<li><a href="#" onclick="changePage(${currentPage + 1}); return false;">&raquo;</a></li>`;
                }
                
                document.getElementById('pagination').innerHTML = paginationHTML;
            }
            
            function changePage(page) {
                currentPage = page;
                displayTable();
                return false;
            }
            
            function resetPagination() {
                currentPage = 1;
                displayTable();
            }
            
            // 페이지 로드 시 실행
            window.onload = function() {
                resetPagination();
                
                // 숨겨진 행 스타일 정의
                const style = document.createElement('style');
                style.innerHTML = '.hidden-row { display: none; }';
                document.head.appendChild(style);
            };
        </script>
    </body>
    </html>
    """
    
    # HTML 파일로 저장
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    logger.info(f"HTML 보고서가 생성되었습니다: {report_path}")

def find_json_files(directory):
    """
    디렉토리에서 JSON 파일 찾기
    
    Args:
        directory (str): 검색할 디렉토리 경로
        
    Returns:
        list: JSON 파일 경로 목록
    """
    json_files = []
    
    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.json'):
                    json_files.append(os.path.join(root, file))
    except Exception as e:
        logger.error(f"JSON 파일 검색 중 오류: {str(e)}")
    
    return json_files

def main():
    print("=" * 60)
    print("의약품 이미지 재추출 스크립트")
    print("=" * 60)
    
    # 기본 설정 사용
    config = DEFAULT_CONFIG.copy()
    
    # 데이터 디렉토리 설정
    data_dirs = []
    
    # 1. 현재 디렉토리 기준으로 검색
    default_data_dir = os.path.join(os.getcwd(), 'json_data')
    if os.path.exists(default_data_dir) and os.path.isdir(default_data_dir):
        data_dirs.append(default_data_dir)
    
    # 2. 'collected_data/json' 디렉토리 확인
    collected_data_dir = os.path.join(os.getcwd(), 'collected_data', 'json')
    if os.path.exists(collected_data_dir) and os.path.isdir(collected_data_dir):
        data_dirs.append(collected_data_dir)
    
    # 3. 현재 디렉토리 내 json 이름이 들어간 디렉토리 검색
    for item in os.listdir(os.getcwd()):
        item_path = os.path.join(os.getcwd(), item)
        if os.path.isdir(item_path) and 'json' in item.lower():
            data_dirs.append(item_path)
    
    # 중복 제거 및 정렬
    data_dirs = sorted(list(set(data_dirs)))
    
    # 사용자 선택
    if not data_dirs:
        print("의약품 데이터 디렉토리를 찾을 수 없습니다.")
        user_dir = input("의약품 JSON 데이터 디렉토리 경로를 입력하세요: ")
        if user_dir and os.path.exists(user_dir) and os.path.isdir(user_dir):
            config['data_dir'] = user_dir
        else:
            print("유효한 디렉토리가 아닙니다. 기본 경로를 사용합니다.")
    elif len(data_dirs) == 1:
        config['data_dir'] = data_dirs[0]
        print(f"의약품 데이터 디렉토리: {config['data_dir']}")
    else:
        print("여러 데이터 디렉토리가 발견되었습니다:")
        for i, dir_path in enumerate(data_dirs, 1):
            print(f"{i}. {dir_path}")
        
        try:
            choice = int(input("사용할 디렉토리 번호를 선택하세요: "))
            if 1 <= choice <= len(data_dirs):
                config['data_dir'] = data_dirs[choice-1]
            else:
                print("유효하지 않은 선택입니다. 첫 번째 디렉토리를 사용합니다.")
                config['data_dir'] = data_dirs[0]
        except ValueError:
            print("숫자가 아닌 입력입니다. 첫 번째 디렉토리를 사용합니다.")
            config['data_dir'] = data_dirs[0]
    
    # 작업자 수 설정
    try:
        workers = int(input(f"동시 처리 작업자 수를 입력하세요 [기본값: {config['workers']}]: ") or config['workers'])
        if workers > 0:
            config['workers'] = workers
    except ValueError:
        print(f"유효하지 않은 입력입니다. 기본값({config['workers']})을 사용합니다.")
    
    # 지연 시간 설정
    try:
        delay = float(input(f"요청 간 지연 시간(초)을 입력하세요 [기본값: {config['delay']}]: ") or config['delay'])
        if delay >= 0:
            config['delay'] = delay
    except ValueError:
        print(f"유효하지 않은 입력입니다. 기본값({config['delay']})을 사용합니다.")
    
    # 이미 이미지가 있는 파일 건너뛰기 설정
    skip_existing = input(f"이미 이미지가 있는 파일을 건너뛸까요? (y/n) [기본값: {'y' if config['skip_existing'] else 'n'}]: ").strip().lower()
    if skip_existing:
        config['skip_existing'] = skip_existing == 'y'
    
    # HTML 보고서 경로 설정
    report_path = input(f"HTML 보고서 저장 경로를 입력하세요 [기본값: {config['report_path']}]: ") or config['report_path']
    config['report_path'] = report_path
    
    # 설정 정보 출력
    print("\n=== 설정 정보 ===")
    print(f"데이터 디렉토리: {config['data_dir']}")
    print(f"동시 작업자 수: {config['workers']}")
    print(f"요청 간 지연 시간: {config['delay']}초")
    print(f"건너뛰기 설정: {'이미 이미지가 있는 파일 건너뛰기' if config['skip_existing'] else '모든 파일 처리'}")
    print(f"HTML 보고서 경로: {config['report_path']}")
    print("================\n")
    
    # 작업 시작 확인
    confirm = input("위 설정으로 이미지 재추출을 시작하시겠습니까? (y/n): ")
    if confirm.lower() != 'y':
        print("작업이 취소되었습니다.")
        return
    
    # JSON 파일 검색
    print(f"의약품 JSON 파일 검색 중: {config['data_dir']}")
    json_files = find_json_files(config['data_dir'])
    
    total_files = len(json_files)
    if total_files == 0:
        print("처리할 JSON 파일이 없습니다.")
        return
    
    print(f"총 {total_files}개의 JSON 파일을 찾았습니다.")
    
    # 세션 객체 생성
    global session
    session = create_session()
    
    # 404 오류 캐시 초기화
    global invalid_ids
    invalid_ids = set()
    
    # 배치 처리 (메모리 효율성)
    results = []
    batch_size = config['batch_size']
    
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch = json_files[batch_start:batch_end]
        
        print(f"배치 처리 중: {batch_start+1}-{batch_end}/{total_files}")
        
        # 세션 갱신 (배치마다)
        session = reset_session(session)
        
        try:
            # 멀티스레딩으로 처리
            with ThreadPoolExecutor(max_workers=config['workers']) as executor:
                futures = {}
                
                # 각 파일에 대한 작업 예약
                for file_path in batch:
                    # 오래 걸리는 실행을 방지하기 위해 현재 진행 중인 작업 수 확인
                    while len(futures) >= config['workers']:
                        # 완료된 작업 제거
                        done, _ = concurrent.futures.wait(
                            futures.keys(),
                            timeout=0.1,
                            return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        
                        for future in done:
                            if future in futures:
                                file_path = futures[future]
                                result = future.result()
                                results.append(result)
                                del futures[future]
                                
                                # 로그 출력 (최소화)
                                if not result['skipped']:
                                    if result['success']:
                                        status = "성공" if result['has_image'] else "이미지 없음"
                                        logger.info(f"{os.path.basename(result['file_path'])} - {status}")
                                    else:
                                        logger.error(f"{os.path.basename(result['file_path'])} - 실패: {result['message']}")
                    
                    # 요청 간 지연 시간 추가 (서버 부하 방지)
                    time.sleep(config['delay'] + random.uniform(0, 0.2))
                    
                    # 새 작업 추가
                    future = executor.submit(process_medicine_file, file_path, config, session)
                    futures[future] = file_path
                
                # 남은 작업 처리
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"배치 {batch_start+1}-{batch_end}"):
                    file_path = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # 로그 출력 (최소화)
                        if not result['skipped']:
                            if result['success']:
                                status = "성공" if result['has_image'] else "이미지 없음"
                                logger.info(f"{os.path.basename(result['file_path'])} - {status}")
                            else:
                                logger.error(f"{os.path.basename(result['file_path'])} - 실패: {result['message']}")
                    except Exception as e:
                        logger.error(f"작업 완료 중 오류 ({file_path}): {str(e)}")
                        # 오류가 발생해도 계속 진행
                        results.append({
                            'file_path': file_path,
                            'file_name': os.path.basename(file_path),
                            'success': False,
                            'message': f"처리 중 예외 발생: {str(e)}",
                            'medicine_name': "",
                            'medicine_id': None,
                            'page_url': "",
                            'old_image_url': "",
                            'new_image_url': "",
                            'image_quality': "",
                            'has_image': False,
                            'skipped': False
                        })
        
        except KeyboardInterrupt:
            print("\n사용자에 의해 작업이 중단되었습니다.")
            # 세션 종료
            if session:
                session.close()
            break
            
        except Exception as e:
            logger.error(f"배치 처리 중 예외 발생: {str(e)}")
            print(f"배치 처리 중 오류 발생: {str(e)}")
            # 세션 종료
            if session:
                session.close()
            # 새 세션 생성
            session = create_session()
        
        # 중간 보고서 생성 (배치마다)
        if batch_end < total_files:
            intermediate_report_path = f"{os.path.splitext(config['report_path'])[0]}_batch_{batch_end}.html"
            print(f"중간 보고서 생성 중: {intermediate_report_path}")
            generate_html_report(results, intermediate_report_path)
    
    # 세션 종료
    if session:
        session.close()
    
    # 통계 계산
    success_count = sum(1 for r in results if r['success'])
    with_image = sum(1 for r in results if r['success'] and r['has_image'])
    no_image = sum(1 for r in results if r['success'] and not r['has_image'])
    failed_count = sum(1 for r in results if not r['success'] and not r['skipped'])
    skipped_count = sum(1 for r in results if r['skipped'])
    
    # 결과 출력
    print("\n=== 처리 결과 ===")
    print(f"총 파일 수: {total_files}개")
    print(f"성공: {success_count}개")
    print(f"  - 이미지 있음: {with_image}개")
    print(f"  - 이미지 없음: {no_image}개")
    print(f"실패: {failed_count}개")
    print(f"건너뜀: {skipped_count}개")
    print("================\n")
    
    # HTML 보고서 생성
    print(f"HTML 보고서 생성 중: {config['report_path']}")
    generate_html_report(results, config['report_path'])
    
    print(f"\n작업이 완료되었습니다. HTML 보고서: {config['report_path']}")
    
    # 브라우저에서 HTML 보고서 열기 옵션
    if os.path.exists(config['report_path']):
        open_browser = input("HTML 보고서를 브라우저에서 열까요? (y/n): ")
        if open_browser.lower() == 'y':
            try:
                import webbrowser
                webbrowser.open(f"file://{os.path.abspath(config['report_path'])}")
            except Exception as e:
                print(f"브라우저에서 파일을 열지 못했습니다: {str(e)}")

if __name__ == "__main__":
    try:
        # concurrent.futures 모듈 가져오기
        import concurrent.futures
        
        # 전역 변수 초기화
        invalid_ids = set()
        session = None
        
        main()
    except KeyboardInterrupt:
        print("\n작업이 사용자에 의해 중단되었습니다.")
        # 세션 종료
        if 'session' in globals() and session:
            session.close()
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {str(e)}")
        print(f"오류 발생: {str(e)}")
    finally:
        # 세션 종료
        if 'session' in globals() and session:
            session.close()
        print("\n프로그램을 종료합니다.")
        input("엔터 키를 눌러 종료하세요...")