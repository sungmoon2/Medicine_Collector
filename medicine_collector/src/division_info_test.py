#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
특정 의약품의 분할선 정보 추출을 테스트하는 스크립트
"""

import sys
import os
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import time
import random
from dotenv import load_dotenv

# 현재 스크립트 위치 기준으로 상위 디렉토리 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, project_root)

# 환경변수 로드
load_dotenv()

# 사용자 에이전트 목록
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/111.0.1661.54 Safari/537.36"
]

def extract_division_info(soup):
    """
    분할선 정보 추출 (td 태그 내용 직접 추출 버전)
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
    
    Returns:
        dict or None: 분할선 정보 또는 분할선이 없는 경우 None
    """
    print("분할선 정보 추출 시작...")
    
    # 1. 분할선 라벨을 가진 th 태그 찾기
    for th in soup.find_all('th'):
        th_text = th.text.strip()
        # 분할선 관련 키워드 확인
        if '분할선' in th_text:
            print(f"분할선 관련 th 태그 발견: '{th_text}'")
            
            # 같은 행(tr)에 있는 td 태그 찾기
            parent_tr = th.parent
            if parent_tr and parent_tr.name == 'tr':
                td = parent_tr.find('td')
                if td:
                    td_text = td.text.strip()
                    print(f"분할선 정보 td 태그 내용: '{td_text}'")
                    
                    # 원본 텍스트 그대로 사용하면서 분할선 유형도 결정
                    division_description = td_text  # 원본 텍스트 그대로 사용
                    
                    # 참고용 분할선 유형 정보 추가
                    if '+' in td_text:
                        division_type = "십자형"
                    elif '-' in td_text:
                        division_type = "일자형"
                    else:
                        division_type = "기타"
                    
                    return {
                        "division_description": division_description,  # 원본 텍스트 (예: "+, +")
                        "division_type": division_type  # 분류 (예: "십자형")
                    }
    
    # 2. 테이블 행(tr) 검사 - 분할선 관련 행 찾기
    for tr in soup.find_all('tr'):
        tr_text = tr.text.strip()
        if '분할선' in tr_text or '절단선' in tr_text or '나누는 선' in tr_text:
            print(f"발견된 TR 태그 내용: '{tr_text}'")
            
            # 행에서 td 태그 찾기
            tds = tr.find_all('td')
            if len(tds) > 0:
                # 첫 번째 td는 제목일 수 있으므로 두 번째 이후 td 확인
                td_content = tds[-1].text.strip() if len(tds) > 1 else tds[0].text.strip()
                
                # 분할선 유형 결정
                if '+' in td_content:
                    division_type = "십자형"
                elif '-' in td_content:
                    division_type = "일자형"
                elif '기타' in td_content:
                    division_type = "기타"
                else:
                    division_type = "기타"
                
                return {
                    "division_description": td_content,
                    "division_type": division_type
                }
    
    # 3. 키워드 목록 확장 (모든 가능한 표현 포함)
    division_keywords = [
        '분할선', '나누는 선', '절단선', '십자선', '십자형', '일자형', '일자선',
        '+', '-', ',', '기타', '특수선'
    ]
    
    # 4. 전체 텍스트에서 가장 관련성 높은 문장 찾기
    full_text = soup.get_text()
    relevant_sentences = []
    
    for sentence in re.split(r'[.。\n]+', full_text):
        for keyword in division_keywords:
            if keyword in sentence:
                relevant_sentences.append(sentence.strip())
                break
    
    print(f"분할선 관련 문장 {len(relevant_sentences)}개 발견")
    for i, sent in enumerate(relevant_sentences[:3]):  # 처음 3개만 출력
        print(f"  문장 {i+1}: {sent[:50]}...")
    
    # 5. 표에서 분할선 정보 찾기 (더 정확한 방법)
    for th in soup.find_all(['th', 'dt', 'strong']):
        th_text = th.text.strip().lower()
        if any(keyword in th_text for keyword in ['분할선', '절단선', '나누는 선']):
            # 다음 td 또는 dd 요소 찾기
            next_elem = th.find_next(['td', 'dd'])
            if next_elem:
                description = next_elem.text.strip()
                print(f"표에서 발견한 분할선 정보: {description}")
                
                # 분할선 유형 결정
                division_type = determine_division_type(description)
                return {
                    "division_description": description,
                    "division_type": division_type
                }
    
    # 6. HTML 구조에서 더 깊이 찾기
    for elem in soup.find_all(['p', 'div', 'span', 'li']):
        text = elem.text.strip()
        if any(keyword in text for keyword in division_keywords) and len(text) < 200:
            print(f"HTML 요소에서 발견한 분할선 정보: {text}")
            division_type = determine_division_type(text)
            return {
                "division_description": text,
                "division_type": division_type
            }
    
    # 7. 관련 문장들에서 가장 적합한 것 선택
    if relevant_sentences:
        # 가장 짧고 구체적인 문장 선택 (길이 50자 미만)
        short_sentences = [s for s in relevant_sentences if len(s) < 50]
        if short_sentences:
            best_sentence = min(short_sentences, key=len)
        else:
            # 짧은 문장이 없으면 가장 관련성 높은 문장 선택
            scores = []
            for sentence in relevant_sentences:
                score = sum(1 for kw in division_keywords if kw in sentence)
                scores.append((score, sentence))
            best_sentence = max(scores, key=lambda x: x[0])[1]
        
        print(f"선택된 최적 문장: {best_sentence}")
        division_type = determine_division_type(best_sentence)
        return {
            "division_description": best_sentence,
            "division_type": division_type
        }
    
    # 8. 이미지 설명에서 분할선 정보 확인
    for img in soup.find_all('img'):
        if 'alt' in img.attrs:
            alt_text = img.attrs['alt']
            if any(keyword in alt_text for keyword in division_keywords):
                print(f"이미지 설명에서 발견한 분할선 정보: {alt_text}")
                division_type = determine_division_type(alt_text)
                return {
                    "division_description": alt_text,
                    "division_type": division_type
                }
    
    print("분할선 정보를 찾지 못했습니다.")
    return None

def determine_division_type(text):
    """
    텍스트에서 분할선 유형 결정
    
    Args:
        text: 분석할 텍스트
        
    Returns:
        str: 분할선 유형 (십자형, 일자형, 기타)
    """
    text = text.lower()
    if '+' in text or '십자' in text:
        return "십자형"
    elif '-' in text or '일자' in text:
        return "일자형"
    elif '없' in text:
        return "없음"
    else:
        return "기타"

def fetch_medicine_page(url):
    """
    의약품 페이지 가져오기
    
    Args:
        url: 페이지 URL
        
    Returns:
        BeautifulSoup 객체 또는 None
    """
    try:
        # HTTP 세션 초기화
        session = requests.Session()
        
        # 기본 헤더 설정
        session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive'
        })
        
        # 랜덤 대기
        time.sleep(random.uniform(0.5, 1.5))
        
        # 페이지 요청
        print(f"URL 요청 중: {url}")
        response = session.get(url, timeout=10)
        
        # 페이지 유효성 확인
        if response.status_code != 200:
            print(f"페이지 가져오기 실패: {url}, 상태 코드: {response.status_code}")
            return None
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
        
    except Exception as e:
        print(f"페이지 처리 중 오류: {url}, {e}")
        return None

def search_medicine_on_naver(medicine_name):
    """
    네이버 검색 API를 사용하여 의약품 검색
    
    Args:
        medicine_name: 의약품 이름
        
    Returns:
        검색 결과 또는 None
    """
    # 네이버 API 인증 정보 로드
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("에러: NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET 환경 변수가 필요합니다.")
        return None
    
    # 검색 API 엔드포인트
    search_url = "https://openapi.naver.com/v1/search/encyc.json"
    
    # API 요청 헤더
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    
    # 검색 파라미터
    params = {
        "query": f"{medicine_name} 의약품",
        "display": 100,
        "start": 1
    }
    
    try:
        # 요청 전 잠시 대기 (API 제한 방지)
        time.sleep(random.uniform(0.5, 1.5))
        
        # API 요청
        response = requests.get(
            search_url, 
            headers=headers, 
            params=params,
            timeout=10
        )
        
        # 응답 확인
        response.raise_for_status()
        
        # JSON 결과 반환
        return response.json()
        
    except requests.RequestException as e:
        print(f"API 검색 오류 (키워드: {medicine_name}): {e}")
        return None

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

def test_division_info_extraction(medicine_names):
    """
    지정한 의약품 이름으로 분할선 정보 추출 테스트
    
    Args:
        medicine_names: 테스트할 의약품 이름 리스트
    """
    results = {}
    
    # 각 약품 이름으로 검색 및 데이터 추출
    for medicine_name in medicine_names:
        print(f"\n===== {medicine_name} 검색 중... =====")
        
        # 검색 수행
        search_result = search_medicine_on_naver(medicine_name)
        if not search_result:
            print(f"  - {medicine_name}에 대한 검색 결과를 가져올 수 없습니다.")
            continue
        
        # 의약품 항목 필터링
        medicine_items = filter_medicine_items(search_result)
        
        if not medicine_items:
            print(f"  - {medicine_name}에 대한 의약품 항목이 없습니다.")
            continue
        
        print(f"  - {len(medicine_items)}개 의약품 항목 발견")
        
        # 정확한 이름 매칭을 위한 플래그
        found_exact_match = False
        
        for item in medicine_items:
            title = re.sub(r'<.*?>', '', item.get("title", ""))
            link = item.get("link", "")
            
            # 정확한 이름이거나 이름을 포함하는 경우
            if medicine_name in title:
                print(f"  - 항목 처리 중: {title}")
                print(f"  - 링크: {link}")
                
                # 페이지 가져오기
                soup = fetch_medicine_page(link)
                
                if soup:
                    # 페이지 제목 출력
                    page_title = soup.find('title')
                    if page_title:
                        print(f"  - 페이지 제목: {page_title.text.strip()}")
                    
                    # 분할선 정보 추출
                    division_info = extract_division_info(soup)
                    
                    if division_info:
                        print(f"  - 추출된 분할선 정보: {division_info}")
                        results[medicine_name] = division_info
                    else:
                        print("  - 분할선 정보를 추출하지 못했습니다.")
                    
                    found_exact_match = True
                    break
                else:
                    print(f"  - 페이지를 가져오지 못했습니다: {link}")
        
        if not found_exact_match:
            print(f"  - {medicine_name}과 정확히 일치하는 항목을 찾지 못했습니다.")
    
    return results

if __name__ == "__main__":
    # 테스트할 의약품 목록
    target_medicines = [
        "슈다페드정(슈도에페드린염산염)",
        "카바민씨알정200mg(카르바마제핀)(수출용)"
    ]
    
    # 추가로 테스트하고 싶은 의약품이 있다면 여기에 추가
    # target_medicines.extend([
    #     "타이레놀",
    #     "아스피린"
    # ])
    
    # 테스트 실행
    results = test_division_info_extraction(target_medicines)
    
    # 결과 요약 출력
    print("\n===== 테스트 결과 요약 =====")
    for name, info in results.items():
        print(f"\n{name}:")
        print(f"  - 분할선 설명: {info.get('division_description', '정보 없음')}")
        print(f"  - 분할선 유형: {info.get('division_type', '정보 없음')}")
    
    print("\n테스트가 완료되었습니다.")