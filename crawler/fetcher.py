#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 지식백과 의약품사전 페이지 요청 및 파싱
"""

import os
import re
import time
import random
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# User-Agent 목록
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

class MedicineFetcher:
    """의약품 정보 가져오기 클래스"""
    
    def __init__(self, max_retries=3, delay_range=(1, 3)):
        """
        초기화
        
        Args:
            max_retries (int): 최대 재시도 횟수
            delay_range (tuple): 요청 간 지연 시간 범위 (초)
        """
        self.max_retries = max_retries
        self.delay_range = delay_range
        self.logger = logging.getLogger('medicine_crawler')
        self.session = self._init_session()
        
    def _init_session(self):
        """
        HTTP 세션 초기화
        
        Returns:
            requests.Session: 초기화된 세션
        """
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
        return session
    
    def fetch_medicine_data(self, docid):
        """
        의약품 정보 가져오기 (향상된 리디렉션 감지)
        
        Args:
            docid (str): 의약품 docId
                
        Returns:
            dict: 의약품 데이터 또는 None (실패 시)
        """
        url = f"https://terms.naver.com/entry.naver?docId={docid}&cid=51000&categoryId=51000"
        self.current_url = url  # 현재 처리 중인 URL 저장
        
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.logger.info(f"[docId: {docid}] 요청 시도 #{retry_count + 1}")
                
                # 요청 전 지연
                delay = random.uniform(*self.delay_range)
                time.sleep(delay)
                
                # 랜덤 User-Agent 설정
                self.session.headers.update({
                    'User-Agent': random.choice(USER_AGENTS),
                    'Referer': 'https://terms.naver.com/list.naver?cid=51000&categoryId=51000'
                })
                
                # 페이지 요청
                response = self.session.get(url, timeout=10, allow_redirects=True)
                
                # 실패 시 재시도
                if response.status_code != 200:
                    self.logger.warning(f"[docId: {docid}] HTTP 오류: {response.status_code}, 재시도 #{retry_count + 1}")
                    retry_count += 1
                    continue
                
                # 리디렉션 분석
                redirect_count = len(response.history)
                final_url = response.url
                
                if redirect_count > 0:
                    # 리디렉션 발생
                    redirect_from = response.history[0].url
                    redirect_status = response.history[0].status_code
                    
                    # 네이버 홈으로 리디렉션된 경우
                    if "terms.naver.com" not in final_url or final_url == "https://terms.naver.com":
                        # 보안 관련 헤더 체크
                        security_headers = [h.lower() for h in response.headers.keys()]
                        has_security_headers = any(h in security_headers for h in ['x-frame-options', 'content-security-policy', 'strict-transport-security'])
                        
                        # 응답 분석 - 로봇 차단 관련 텍스트 확인
                        response_text = response.text.lower()
                        bot_detection_text = any(text in response_text for text in ['captcha', '자동화', '로봇', '비정상적인 접근', '일시적으로 차단'])
                        
                        if has_security_headers and bot_detection_text:
                            self.logger.warning(f"[docId: {docid}] 보안 정책에 의한 차단 감지: 상태코드={redirect_status}, 리디렉션={redirect_from} -> {final_url}")
                        elif "not found" in response_text or "존재하지 않" in response_text:
                            self.logger.warning(f"[docId: {docid}] 페이지가 존재하지 않음: {redirect_from} -> {final_url}")
                        else:
                            self.logger.warning(f"[docId: {docid}] 알 수 없는 리디렉션: {redirect_from} -> {final_url}")
                        
                        retry_count += 1
                        continue
                    
                    # 다른 문서로 리디렉션된 경우
                    elif f"docId={docid}" not in final_url:
                        new_docid = re.search(r'docId=(\d+)', final_url)
                        if new_docid:
                            self.logger.warning(f"[docId: {docid}] 다른 문서로 리디렉션: {docid} -> {new_docid.group(1)}")
                        else:
                            self.logger.warning(f"[docId: {docid}] 다른 페이지로 리디렉션: {redirect_from} -> {final_url}")
                        retry_count += 1
                        continue
                
                # URL 검증 - 의약품 카테고리 확인
                if "cid=51000" not in response.url or "categoryId=51000" not in response.url:
                    self.logger.warning(f"[docId: {docid}] 의약품 카테고리가 아닌 URL: {response.url}, 건너뜀")
                    return None
                
                # HTML 파싱
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 의약품 페이지 확인
                if not self._is_valid_medicine_page(soup):
                    # 페이지는 존재하지만 의약품이 아닌 경우
                    title_tag = soup.find('title')
                    title_text = title_tag.text if title_tag else "제목 없음"
                    
                    # 로그에 페이지 유형 추가
                    page_type = self._detect_page_type(soup)
                    self.logger.warning(f"[docId: {docid}] 의약품 페이지 아님: 유형={page_type}, 제목={title_text}")
                    retry_count += 1
                    continue
                
                # 의약품 데이터 추출
                medicine_data = self._parse_medicine_data(soup, docid, url)
                
                if medicine_data:
                    self.logger.info(f"[docId: {docid}] 데이터 추출 성공: {medicine_data.get('korean_name', '-')}")
                    return medicine_data
                else:
                    self.logger.warning(f"[docId: {docid}] 데이터 추출 실패, 재시도 #{retry_count + 1}")
                    retry_count += 1
                    continue
            
            except requests.Timeout:
                self.logger.warning(f"[docId: {docid}] 요청 타임아웃, 재시도 #{retry_count + 1}")
                retry_count += 1
                continue
            except requests.ConnectionError:
                self.logger.warning(f"[docId: {docid}] 연결 오류 (접속 차단 가능성), 재시도 #{retry_count + 1}")
                # 접속 차단 가능성이 있으므로 더 오래 대기
                time.sleep(random.uniform(5, 10))
                retry_count += 1
                continue
            except Exception as e:
                self.logger.error(f"[docId: {docid}] 요청 중 오류: {str(e)}, 재시도 #{retry_count + 1}")
                retry_count += 1
                continue
        
        self.logger.error(f"[docId: {docid}] 최대 재시도 횟수 초과, 데이터 가져오기 실패")
        return None

    def _detect_page_type(self, soup):
        """
        페이지 유형 감지
        """
        # 제목 검사
        title = soup.find('title')
        title_text = title.text if title else ""
        
        # 카테고리 정보 검사
        category_elements = soup.select('.location_area a, .location li a, .path a')
        categories = [el.text for el in category_elements]
        
        # 각 유형별 키워드
        type_keywords = {
            "인물사전": ["인물", "사람", "작가", "배우", "가수", "정치인", "운동선수"],
            "지리정보": ["지역", "시", "군", "구", "동", "면", "리", "국가", "도시"],
            "백과사전": ["백과", "사전", "정보", "지식"],
            "영화/드라마": ["영화", "드라마", "시리즈", "작품", "감독"],
            "의학정보": ["의학", "질병", "증상", "진단", "치료"],
            "동식물": ["동물", "식물", "생물", "종", "과"],
            "역사": ["역사", "시대", "연대", "왕조", "사건"]
        }
        
        # 유형 감지
        for page_type, keywords in type_keywords.items():
            # 제목에서 확인
            if any(kw in title_text for kw in keywords):
                return page_type
                
            # 카테고리에서 확인
            if any(any(kw in cat for kw in keywords) for cat in categories):
                return page_type
        
        # meta 태그에서 키워드 확인
        meta_keywords = soup.find('meta', {'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            content = meta_keywords.get('content')
            for page_type, keywords in type_keywords.items():
                if any(kw in content for kw in keywords):
                    return page_type
        
        return "알 수 없음"

    def _is_valid_medicine_page(self, soup):
        """
        의약품 페이지인지 정확하게 확인
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
                
        Returns:
            bool: 의약품 페이지 여부
        """
        # 1. URL 확인 - 이미 fetcher에서 확인하므로 여기서는 생략 가능
        
        # 2. 상단 메뉴에서 '의약품 사전' 링크 확인 (결정적 증거)
        cite_elements = soup.find_all('p', class_='cite')
        for cite in cite_elements:
            links = cite.find_all('a')
            for link in links:
                if 'list.naver?cid=51000' in link.get('href', '') and '의약품 사전' in link.text:
                    return True
        
        # 3. 내비게이션 경로에서 '의약품 사전' 확인
        nav_elements = soup.find_all(['div', 'ul'], class_=['location_wrap', 'path', 'breadcrumb'])
        for nav in nav_elements:
            links = nav.find_all('a')
            for link in links:
                if 'list.naver?cid=51000' in link.get('href', '') and '의약품' in link.text:
                    return True
        
        # 위 조건을 만족하지 않으면 의약품 페이지가 아님
        return False
    
    def _parse_medicine_data(self, soup, docid, url):
        """
        HTML에서 의약품 데이터 추출
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
            docid (str): docId
            url (str): 페이지 URL
            
        Returns:
            dict: 의약품 데이터
        """
        medicine_data = {
            "id": f"M{docid}",
            "url": url,
            "source_url": url,
            "extracted_time": datetime.now().isoformat(),
            "collection_time": datetime.now().isoformat()
        }
        
        # 1. 기본 정보 추출
        self._extract_basic_info(soup, medicine_data)
        
        # 2. 프로필 정보 추출
        self._extract_profile_data(soup, medicine_data)
        
        # 3. 상세 정보 추출
        self._extract_detailed_sections(soup, medicine_data)
        
        # 4. 이미지 정보 추출
        self._extract_medicine_image(soup, medicine_data)
        
        # 필수 정보 확인
        if not medicine_data.get('korean_name'):
            title_tag = soup.find('title')
            if title_tag:
                medicine_data['korean_name'] = re.sub(r'[\s-]+네이버.*$', '', title_tag.text.strip())
        
        # 필드 정규화
        self._normalize_field_names(medicine_data)
        
        return medicine_data
    
    def _extract_basic_info(self, soup, medicine_data):
        """
        기본 정보 추출
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
            medicine_data (dict): 의약품 데이터
        """
        # 한글 이름 추출
        for selector in ['h2.headword', 'h3.headword', 'div.word_head h2', 'div.title_area h2', '.article_head h2']:
            title_tag = soup.select_one(selector)
            if title_tag:
                medicine_data["korean_name"] = title_tag.text.strip()
                break
        
        # 영문명 추출
        for selector in ['span.word_txt', 'p.eng_title', 'div.section_subtitle', '.eng_title']:
            eng_name_tag = soup.select_one(selector)
            if eng_name_tag:
                medicine_data["english_name"] = eng_name_tag.text.strip()
                break
    
    def _extract_profile_data(self, soup, medicine_data):
        """
        프로필 정보 추출
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
            medicine_data (dict): 의약품 데이터
        """
        # 필드 매핑
        field_mapping = {
            "분류": "classification",
            "구분": "category",
            "업체명": "company",
            "보험코드": "insurance_code",
            "성상": "appearance",
            "제형": "shape_type",
            "모양": "shape",
            "색깔": "color",
            "크기": "size",
            "식별표기": "identification",
            "분할선": "division_line",
            "허가일": "approval_date",
            "허가번호": "approval_number",
            "전문/일반": "medicine_type",
            "제조/수입": "manufacture_type",
            "성분/함량": "components_amount",
            "약가": "price"
        }
        
        # 테이블 기반 데이터 추출
        profile_tables = soup.find_all('table', class_=['tmp_profile_tb', 'profile_table', 'drug_info', 'drug_profile'])
        
        for table in profile_tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 2:
                    field_name = cells[0].text.strip()
                    field_value = cells[1].text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name:
                            medicine_data[data_key] = field_value if field_value else "정보 없음"
                            break
        
        # 정의 리스트 형식 검색 (dl/dt/dd)
        dl_elements = soup.find_all('dl')
        
        for dl in dl_elements:
            dt_tags = dl.find_all('dt')
            dd_tags = dl.find_all('dd')
            
            for i, dt in enumerate(dt_tags):
                if i < len(dd_tags):
                    field_name = dt.text.strip()
                    field_value = dd_tags[i].text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name and data_key not in medicine_data:
                            medicine_data[data_key] = field_value if field_value else "정보 없음"
                            break
    
    def _extract_detailed_sections(self, soup, medicine_data):
        """
        섹션별 상세 정보 추출
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
            medicine_data (dict): 의약품 데이터
        """
        section_mapping = {
            "성분": "components",
            "성분정보": "components",
            "주성분": "components",
            "효능": "efficacy",
            "효과": "efficacy",
            "효능효과": "efficacy",
            "주의사항": "precautions",
            "사용상주의사항": "precautions",
            "사용상의주의사항": "precautions",
            "용법": "dosage",
            "용량": "dosage",
            "용법용량": "dosage",
            "투여법": "dosage",
            "저장방법": "storage",
            "보관방법": "storage",
            "보관조건": "storage",
            "사용기간": "expiration",
            "유효기간": "expiration",
            "유통기한": "expiration",
            "약리작용": "pharmacology",
            "부작용": "side_effects",
            "이상반응": "side_effects",
            "상호작용": "interactions",
            "약물상호작용": "interactions"
        }
        
        # 헤딩 태그 기반 섹션 추출
        heading_tags = []
        for heading_level in range(2, 6):
            heading_tags.extend(soup.find_all(f'h{heading_level}'))
        
        for heading in heading_tags:
            heading_text = heading.text.strip()
            
            matched_key = None
            for key, value in section_mapping.items():
                if key in heading_text and value not in medicine_data:
                    matched_key = value
                    break
            
            if not matched_key:
                continue
            
            # 섹션 내용 추출
            contents = []
            current = heading.find_next_sibling()
            next_heading = None
            
            # 다음 헤딩 찾기
            for tag in heading.find_all_next(['h2', 'h3', 'h4', 'h5']):
                next_heading = tag
                break
            
            # 다음 헤딩까지 내용 수집
            while current and (not next_heading or current != next_heading):
                if current.name in ['p', 'div', 'ul', 'ol', 'table']:
                    content_text = current.text.strip()
                    if content_text:
                        contents.append(content_text)
                
                current = current.find_next_sibling()
            
            if contents:
                medicine_data[matched_key] = "\n".join(contents)
        
        # 클래스 기반 섹션 추출
        section_classes = [
            'section', 'content', 'drug_section', 'cont_block', 
            'detail_info', 'detail_content', 'drug_detail', 
            'medicine_info', 'drug_info'
        ]
        
        content_sections = []
        for class_name in section_classes:
            content_sections.extend(soup.find_all(class_=class_name))
        
        for section in content_sections:
            # 섹션 제목 찾기
            title_elem = section.find(['h3', 'h4', 'strong', 'dt', 'th'])
            
            if title_elem:
                section_title = title_elem.text.strip()
                
                matched_key = None
                for key, value in section_mapping.items():
                    if key in section_title and value not in medicine_data:
                        matched_key = value
                        break
                
                if not matched_key:
                    continue
                
                # 제목 제외한 내용 추출
                section_content = section.text.strip()
                if section_title in section_content:
                    section_content = section_content.replace(section_title, "", 1).strip()
                
                if section_content:
                    medicine_data[matched_key] = section_content
    
    def _extract_medicine_image(self, soup, medicine_data):
        """
        의약품 이미지 정보 추출
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML
            medicine_data (dict): 의약품 데이터
        """
        # 더미 이미지 패턴
        dummy_patterns = [
            "e.gif", "blank.gif", "spacer.gif", "transparent.gif",
            "empty.png", "pixel.gif", "noimage", "no_img", "no-img"
        ]
        
        # 이미지 박스 검색
        img_box_spans = soup.find_all('span', class_='img_box')
        
        for span in img_box_spans:
            a_tag = span.find('a')
            if not a_tag:
                continue
            
            img_tag = a_tag.find('img')
            if not img_tag:
                continue
            
            # 이미지 URL 추출
            img_url = None
            
            # 원본 이미지 속성 우선
            if img_tag.has_attr('origin_src') and img_tag['origin_src']:
                img_url = img_tag['origin_src']
                medicine_data["image_quality"] = "high"
            # src 속성
            elif img_tag.has_attr('src') and img_tag['src']:
                img_url = img_tag['src']
                medicine_data["image_quality"] = "medium"
            # data-src 속성
            elif img_tag.has_attr('data-src') and img_tag['data-src']:
                img_url = img_tag['data-src']
                medicine_data["image_quality"] = "low"
            
            # URL이 없으면 건너뛰기
            if not img_url:
                continue
            
            # 상대 경로를 절대 경로로 변환
            if not img_url.startswith(('http://', 'https://')):
                img_url = urljoin('https://terms.naver.com', img_url)
            
            # 더미 이미지 필터링
            if any(dummy in img_url.lower() for dummy in dummy_patterns):
                continue
            
            # 이미지 정보 추출
            medicine_data["image_url"] = img_url
            
            # 이미지 크기 정보
            if img_tag.has_attr('width') and img_tag.has_attr('height'):
                medicine_data["image_width"] = img_tag['width']
                medicine_data["image_height"] = img_tag['height']
            
            # 원본 크기 정보
            if img_tag.has_attr('origin_width') and img_tag.has_attr('origin_height'):
                medicine_data["original_width"] = img_tag['origin_width']
                medicine_data["original_height"] = img_tag['origin_height']
            
            # alt 정보
            if img_tag.has_attr('alt'):
                medicine_data["image_alt"] = img_tag['alt']
            
            # 첫 번째 유효한 이미지만 추출
            break
    
    def _normalize_field_names(self, medicine_data):
        """
        필드명 정규화
        
        Args:
            medicine_data (dict): 의약품 데이터
        """
        # 필드명 매핑
        field_mapping = {
            "link": "source_url",
            "description": "overview",
            "category_name": "category",
            "medicine_name": "korean_name",
            "eng_name": "english_name",
            "company_name": "company",
            "shape_info": "shape",
            "color_info": "color",
            "size_info": "size",
            "effect": "efficacy",
            "caution": "precautions",
            "usage": "dosage",
            "storage_method": "storage",
            "validity": "expiration",
        }
        
        # 필드명 변경
        for old_key, new_key in field_mapping.items():
            if old_key in medicine_data and old_key != new_key:
                # 새 필드가 없거나, 새 필드가 있지만 값이 없는 경우
                if new_key not in medicine_data or not medicine_data[new_key]:
                    medicine_data[new_key] = medicine_data[old_key]
                # 기존 필드 삭제
                del medicine_data[old_key]
        
        # 누락된 필드 추가
        for field in ["components", "efficacy", "dosage", "precautions", "storage"]:
            if field not in medicine_data:
                medicine_data[field] = "정보 없음"