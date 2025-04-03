#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
HTML 기본 파싱 기능 모듈
"""

import os
import re
import time
import random
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .profile_parser import extract_profile_data, extract_basic_info
from .image_parser import extract_medicine_image
from .section_parser import extract_detailed_sections, normalize_field_names
from .profile_parser import extract_supplementary_identification as extract_identification_info_safe
from utils.safety import safe_regex_search, safe_regex_group

# 로거 설정
logger = logging.getLogger(__name__)

# 전역 종료 플래그 (다른 모듈에서 설정)
shutdown_requested = False

def is_medicine_page(soup):
    """
    의약품사전 페이지 여부 확인 (개선된 버전)
    
    Args:
        soup: BeautifulSoup 객체
        
    Returns:
        bool: 의약품사전 페이지면 True
    """
    # 1. URL에 cid=51000이 포함된 경우 (이미 외부에서 검증)
    
    # 2. 페이지 핵심 텍스트 추출 (효율성 향상)
    # title 태그와 주요 헤딩 태그 텍스트만 먼저 확인
    important_tags = soup.find_all(['title', 'h1', 'h2', 'h3'], limit=10)
    important_text = ' '.join(tag.get_text() for tag in important_tags)
    
    # 빠른 키워드 체크: 제목과 헤딩에 의약품사전 키워드 있는지
    if '의약품' in important_text and ('사전' in important_text or '정보' in important_text):
        return True
    
    # 3. 의약품 관련 섹션 확인 (효율적 검색)
    section_titles = soup.find_all(['h3', 'h4', 'h5', 'strong', 'dt'], limit=20)
    medicine_sections = ["성분", "효능", "효과", "부작용", "용법", "용량", "성상", "보관", "주의사항"]
    
    # 카운터 기반 확인 (2개 이상 섹션이 있어야 의약품 페이지로 판단)
    section_count = 0
    for title in section_titles:
        title_text = title.get_text().strip()
        if any(section in title_text for section in medicine_sections):
            section_count += 1
            if section_count >= 2:  # 두 개 이상의 의약품 관련 섹션이 있으면 확정
                return True
    
    # 4. 의약품 프로필 확인 (프로필 요소가 있는지)
    profile_elements = soup.find_all(['dl', 'table'], class_=['profile', 'info', 'drug_info'], limit=5)
    
    if profile_elements:
        profile_text = " ".join([elem.get_text() for elem in profile_elements])
        profile_keywords = ["분류", "업체명", "성상", "보험코드", "구분", "약효분류", "전문/일반"]
        keyword_count = sum(1 for keyword in profile_keywords if keyword in profile_text)
        
        if keyword_count >= 2:  # 두 개 이상의 의약품 프로필 키워드가 있으면 확정
            return True
    
    # 5. 페이지 텍스트 전체에서 의약품 관련 키워드 빈도 확인
    # (비용이 많이 드는 작업이므로 마지막에 수행)
    full_text = soup.get_text()
    medicine_keywords = [
        "의약품", "성분", "효능", "효과", "부작용", "용법", "용량", "주의사항", 
        "약물", "제약", "보관", "복용", "투여", "정제", "캡슐"
    ]
    
    # 키워드 빈도 계산
    keyword_count = sum(1 for keyword in medicine_keywords if keyword in full_text)
    
    # 최소 5개 이상의 키워드가 등장하면 의약품 페이지로 간주
    if keyword_count >= 5:
        return True
    
    # 의약품 페이지가 아닌 것으로 판단
    return False

def fetch_medicine_data(item, medicine_data=None, max_retries=3, user_agents=None, output_dir="collected_data"):
    """
    검색 결과 항목에서 의약품 페이지 데이터 추출 (안전하게 개선된 버전)
    
    Args:
        item: 검색 결과 항목
        medicine_data: 기본 의약품 데이터 (없으면 생성)
        max_retries: 최대 재시도 횟수
        user_agents: 사용자 에이전트 목록
        output_dir: 출력 디렉토리
                
    Returns:
        dict: 추출된 의약품 데이터
    """
    # 전역 종료 플래그 확인
    global shutdown_requested
    if shutdown_requested:
        logger.info("종료 요청으로 데이터 추출을 중단합니다.")
        return None
    
    # 타이틀 정리 (로그용)
    title = re.sub(r'<.*?>', '', item.get("title", ""))
    
    # 기본 메타데이터 구성
    if medicine_data is None:
        medicine_data = {
            "title": title,
            "link": item.get("link", ""),
            "description": re.sub(r'<.*?>', '', item.get("description", "")),
            "category": item.get("category", ""),
            "collection_time": datetime.now().isoformat()
        }
    
    # URL 체크
    url = medicine_data["link"]
    if not url or not url.startswith("http"):
        logger.warning(f"[{title}] 유효하지 않은 URL: {url}")
        return None
    
    # URL에서 docId 추출 시도 (캐싱을 위한 고유 ID)
    doc_id = None
    doc_id_match = safe_regex_search(r'docId=([^&]+)', url)
    if doc_id_match:
        doc_id = f"M{safe_regex_group(doc_id_match, 1)}"
        # 이미 처리된 ID인지 확인 (중복 처리 방지)
        existing_ids_path = os.path.join(output_dir, "processed_medicine_ids.txt")
        if os.path.exists(existing_ids_path):
            try:
                with open(existing_ids_path, 'r', encoding='utf-8') as f:
                    if f"{doc_id}\n" in f.read():
                        logger.info(f"│  [ID: {doc_id}] 이미 처리된 ID입니다 - 건너뜁니다.")
                        return None
            except Exception as e:
                logger.warning(f"│  [ID: {doc_id}] ID 목록 읽기 오류: {str(e)}")
    
    # 로그 식별 텍스트 구성
    log_id = doc_id if doc_id else "Unknown"
    
    # 로그 구분 시작
    logger.info(f"┌─ 의약품 데이터 처리 시작: {title}")
    logger.info(f"│  [ID: {log_id}] URL: {url}")
    
    # 기본 HTTP 세션 설정
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(user_agents) if user_agents else "Python Requests",
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive'
    })
    
    # 페이지 요청 및 처리
    retry_count = 0
    backoff_time = 1  # 초기 대기 시간
    last_page_request_time = getattr(session, 'last_page_request_time', 0)
    
    while retry_count < max_retries and not shutdown_requested:
        try:
            # 요청 간 간격 유지 (과도한 요청 방지)
            current_time = time.time()
            elapsed = current_time - last_page_request_time
            if elapsed < 1.0:  # 최소 1초 간격
                time.sleep(1.0 - elapsed)
            last_page_request_time = time.time()
            
            # 랜덤 User-Agent 및 Referer 설정
            session.headers.update({
                'User-Agent': random.choice(user_agents) if user_agents else "Python Requests",
                'Referer': 'https://search.naver.com/'
            })
            
            # 페이지 요청
            response = session.get(url, timeout=15)
            
            # 페이지 유효성 확인
            if response.status_code != 200:
                if response.status_code >= 500:
                    # 서버 오류는 재시도
                    logger.warning(f"│  [ID: {log_id}] 서버 오류 {response.status_code}, 재시도 {retry_count+1}/{max_retries}")
                    retry_count += 1
                    time.sleep(backoff_time)
                    backoff_time *= 2
                    continue
                
                logger.warning(f"│  [ID: {log_id}] 페이지 가져오기 실패: 상태 코드 {response.status_code}")
                logger.info(f"└─ 처리 실패: {title}")
                return None
            
            # HTML 효율적 파싱 (필요한 부분만 파싱)
            html_text = response.text
            
            # BeautifulSoup 객체 생성 (안전하게 처리)
            soup = None
            try:
                soup = BeautifulSoup(html_text, 'html.parser')
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] HTML 파싱 오류: {str(e)} - 재시도 중")
                retry_count += 1
                continue
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info("│  종료 요청으로 데이터 추출을 중단합니다.")
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            # 의약품 페이지 확인
            if not is_medicine_page(soup):
                logger.warning(f"│  [ID: {log_id}] 의약품 페이지가 아님")
                logger.info(f"└─ 처리 실패: {title}")
                return None
            
            # 데이터 추출 프로세스 시작
            logger.info(f"│  [ID: {log_id}] 데이터 추출 시작")
            
            # 1. 기본 데이터 추출
            extracted_data = {
                "url": url,
                "extracted_time": datetime.now().isoformat()
            }
            
            # 2. 기본 타이틀 및 영문명 추출 (안전하게 처리)
            try:
                extract_basic_info(soup, extracted_data)
                if "korean_name" in extracted_data and extracted_data["korean_name"]:
                    logger.info(f"│  [ID: {log_id}] 제품명: {extracted_data['korean_name']}")
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] 기본 정보 추출 중 오류: {str(e)}")
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            # 3. 이미지 추출 (안전하게 처리)
            try:
                # 의약품 ID와 이름 전달하여 로깅 개선
                med_name = extracted_data.get("korean_name", title)
                img_data = extract_medicine_image(soup, log_id, med_name)
                if img_data:
                    extracted_data.update(img_data)
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] 이미지 추출 중 오류: {str(e)}")
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            # 4. 프로필 테이블 추출 (구조화된 데이터) (안전하게 처리)
            profile_data = {}
            try:
                profile_data = extract_profile_data(soup)
                if profile_data:
                    extracted_data.update(profile_data)
                    logger.info(f"│  [ID: {log_id}] 프로필 정보 추출됨: {len(profile_data)} 항목")
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] 프로필 데이터 추출 중 오류: {str(e)}")
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            # 5. 섹션별 상세 정보 추출 (안전하게 처리)
            try:
                section_data = extract_detailed_sections(soup)
                if section_data:
                    extracted_data.update(section_data)
                    section_names = ", ".join(list(section_data.keys())[:3])
                    logger.info(f"│  [ID: {log_id}] 섹션 정보 추출됨: {len(section_data)} 항목 ({section_names} 등)")
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] 상세 섹션 추출 중 오류: {str(e)}")
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            # 6. 구조화된 데이터에서 빠진 식별 정보만 보완 (안전하게 처리)
            try:
                # 오류가 발생하더라도 계속 진행하도록 내부에서 예외 처리
                identification_data = extract_identification_info_safe(soup, profile_data)
                if identification_data:
                    extracted_data.update(identification_data)
                    logger.info(f"│  [ID: {log_id}] 식별 정보 추출됨")
            except Exception as e:
                # 이미 내부에서 예외 처리하지만 만일의 경우를 위한 추가 처리
                logger.warning(f"│  [ID: {log_id}] 식별 정보 추출 중 오류: {str(e)}")
            
            # 7. 필드명 정리 (안전하게 처리)
            try:
                normalize_field_names(extracted_data)
            except Exception as e:
                logger.warning(f"│  [ID: {log_id}] 필드명 정규화 중 오류: {str(e)}")
            
            # 원본 데이터와 병합
            medicine_data.update(extracted_data)
            
            # 디버그 정보: 추출된 필드
            core_fields = [key for key in medicine_data if key not in ["url", "extracted_time", "collection_time"]]
            
            # 필드 정보 로깅 - 간결하게 표시
            field_count = len(core_fields)
            if field_count > 0:
                visible_fields = core_fields[:5]  # 처음 5개 필드만 표시
                field_str = ", ".join(visible_fields)
                if field_count > 5:
                    field_str += f", ... (외 {field_count-5}개)"
                logger.info(f"│  [ID: {log_id}] 총 {field_count}개 필드 추출됨: {field_str}")
            
            # id 필드 추가
            if doc_id and "id" not in medicine_data:
                medicine_data["id"] = doc_id
            
            logger.info(f"└─ 처리 완료: {title}")
            return medicine_data
                
        except Exception as e:
            retry_count += 1
            logger.warning(f"│  [ID: {log_id}] 처리 중 오류 (시도 {retry_count}/{max_retries}): {str(e)}")
            time.sleep(backoff_time)
            backoff_time *= 2
            
            # 종료 요청 확인
            if shutdown_requested:
                logger.info("│  종료 요청으로 데이터 추출을 중단합니다.")
                logger.info(f"└─ 처리 중단: {title}")
                return None
            
            if retry_count >= max_retries:
                logger.error(f"│  [ID: {log_id}] 최대 재시도 횟수 초과: {str(e)}")
                # 오류 정보 저장
                try:
                    error_log_path = os.path.join(
                        output_dir, 
                        "error_logs", 
                        f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
                    
                    with open(error_log_path, 'w', encoding='utf-8') as f:
                        import json
                        json.dump({
                            "url": url,
                            "title": title,
                            "id": log_id,
                            "error": str(e),
                            "timestamp": datetime.now().isoformat()
                        }, f, ensure_ascii=False, indent=2)
                except Exception as log_error:
                    logger.warning(f"│  [ID: {log_id}] 오류 로그 저장 실패: {str(log_error)}")
                
                logger.info(f"└─ 처리 실패: {title}")
                return None
    
    # 종료 요청 또는 최대 재시도 횟수 초과
    logger.info(f"└─ 처리 중단: {title}")
    return None