#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 API 관련 함수
"""

import os
import re
import json
import time
import requests
import logging
import random
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)

def search_api(keyword, display=100, start=1, max_retries=3, client_id=None, client_secret=None, user_agents=None, output_dir="collected_data"):
    """
    네이버 검색 API를 사용하여 검색 (개선된 버전)
    
    Args:
        keyword: 검색 키워드
        display: 결과 개수 (최대 100)
        start: 시작 인덱스
        max_retries: 최대 재시도 횟수
        client_id: 네이버 API 클라이언트 ID
        client_secret: 네이버 API 클라이언트 시크릿
        user_agents: 사용자 에이전트 목록
        output_dir: 데이터 저장 디렉토리
        
    Returns:
        dict: 검색 결과
    """
    # 수정/추가: API 요청 한도 체크 로직
    daily_request_path = os.path.join(output_dir, "daily_request_count.json")
    
    # 요청 카운트 로드
    try:
        if os.path.exists(daily_request_path):
            with open(daily_request_path, 'r') as f:
                request_data = json.load(f)
                # 날짜 체크 및 카운트 리셋
                if request_data.get('date') != datetime.now().strftime('%Y-%m-%d'):
                    request_data = {'date': datetime.now().strftime('%Y-%m-%d'), 'count': 0}
        else:
            request_data = {'date': datetime.now().strftime('%Y-%m-%d'), 'count': 0}
        
        # 요청 한도 체크 (25,000회)
        if request_data['count'] >= 25000:
            logger.error("일일 API 요청 한도(25,000회)에 도달했습니다. 수집을 중단합니다.")
            
            # 요청 한도 초과 예외 발생
            raise Exception("일일 API 요청 한도 초과")
        
        # 요청 카운트 증가
        request_data['count'] += 1
        
        # 요청 카운트 저장
        with open(daily_request_path, 'w') as f:
            json.dump(request_data, f)
    
    except Exception as e:
        if "일일 API 요청 한도 초과" not in str(e):
            logger.error(f"API 요청 한도 체크 중 오류: {e}")
        raise

    # 검색 API 엔드포인트
    search_url = "https://openapi.naver.com/v1/search/encyc.json"
    
    # 기본 HTTP 세션 설정
    session = requests.Session()
    
    # API 요청 헤더
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "User-Agent": random.choice(user_agents) if user_agents else "Python Requests"
    }
    
    # 검색 파라미터
    params = {
        "query": f"{keyword} 의약품",
        "display": display,
        "start": start
    }
    
    # 지능형 대기 및 재시도 메커니즘
    retry_count = 0
    backoff_time = 1  # 초기 대기 시간 (초)
    last_request_time = getattr(session, 'last_request_time', 0)
    
    while retry_count < max_retries:
        try:
            # 요청 전 잠시 대기 (API 제한 방지)
            # 이전 요청과의 간격 확인 (최소 0.5초 이상)
            current_time = time.time()
            elapsed = current_time - last_request_time
            if elapsed < 0.5:
                time.sleep(0.5 - elapsed)
            last_request_time = time.time()
            
            # API 요청
            response = session.get(
                search_url, 
                headers=headers, 
                params=params,
                timeout=10
            )
            
            # 응답 상태 코드 확인
            if response.status_code == 429:  # Too Many Requests
                wait_time = int(response.headers.get('Retry-After', backoff_time))
                logger.warning(f"API 요청 제한 감지: {wait_time}초 대기 후 재시도합니다.")
                time.sleep(wait_time)
                retry_count += 1
                backoff_time *= 2  # 대기 시간 두 배로 증가
                continue
            
            # 기타 클라이언트 오류
            if 400 <= response.status_code < 500:
                error_msg = f"API 클라이언트 오류: {response.status_code} - {response.text}"
                logger.error(error_msg)
                
                if response.status_code == 400:  # Bad Request
                    # 검색어 문제일 가능성 있음
                    return {"items": [], "error": "잘못된 요청"}
                
                # 다른 클라이언트 오류는 재시도
                retry_count += 1
                time.sleep(backoff_time)
                backoff_time *= 2
                continue
            
            # 서버 오류
            if response.status_code >= 500:
                logger.warning(f"API 서버 오류: {response.status_code} - 재시도 {retry_count+1}/{max_retries}")
                retry_count += 1
                time.sleep(backoff_time)
                backoff_time *= 2
                continue
            
            # 응답 확인
            response.raise_for_status()
            
            # JSON 결과 반환
            result = response.json()
            return result
            
        except requests.RequestException as e:
            logger.warning(f"API 요청 오류 (키워드: {keyword}, 시도: {retry_count+1}/{max_retries}): {e}")
            retry_count += 1
            
            # 네트워크 오류는 더 짧은 대기 후 재시도
            time.sleep(backoff_time)
            backoff_time *= 1.5
            
            # 마지막 시도에서 실패한 경우
            if retry_count >= max_retries:
                logger.error(f"최대 재시도 횟수 초과. 키워드: {keyword}")
                return {"items": [], "error": str(e)}
    
    # 모든 재시도 실패 시
    return {"items": []}

def filter_medicine_items(search_result):
    """
    검색 결과에서 의약품사전 항목 필터링
    
    Args:
        search_result: 검색 API 응답
        
    Returns:
        list: 필터링된 의약품사전 항목
    """
    medicine_items = []
    
    # 항목이 없으면 빈 리스트 반환
    if not search_result or "items" not in search_result:
        return medicine_items
    
    # 각 항목 확인
    for item in search_result["items"]:
        # HTML 태그 제거
        title = re.sub(r'<.*?>', '', item.get("title", ""))
        description = re.sub(r'<.*?>', '', item.get("description", ""))
        
        # 의약품사전 필터링
        category = item.get("category", "")
        
        # 1. 카테고리가 의약품사전인 경우
        if "의약품사전" in category:
            medicine_items.append(item)
        # 2. 설명에 의약품 관련 키워드가 있는 경우
        elif any(keyword in description for keyword in ["성분", "효능", "효과", "부작용", "용법", "용량"]):
            # 링크 확인 (terms.naver.com + cid=51000)
            link = item.get("link", "")
            if "terms.naver.com" in link and "cid=51000" in link:
                medicine_items.append(item)
    
    return medicine_items