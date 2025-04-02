#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 검색 API를 활용한 의약품 정보 수집기

이 스크립트는 네이버 검색 API를 사용하여 의약품 정보를 검색하고,
검색 결과에서 의약품사전 페이지를 찾아 데이터를 추출합니다.
추출된 데이터는 JSON 형태로 저장되며, 수집 결과는 HTML 파일로 생성됩니다.
"""

import os
import sys
import requests
import json
import time
import re
import csv
import argparse
import random
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()  # 추가된 부분

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("medicine_collector.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MedicineCollector:
    """네이버 검색 API를 사용한 의약품 정보 수집 클래스"""
    
    def __init__(self, client_id, client_secret, output_dir="collected_data"):
        """
        초기화
        
        Args:
            client_id: 네이버 API 클라이언트 ID
            client_secret: 네이버 API 클라이언트 시크릿
            output_dir: 데이터 저장 디렉토리
        """
        # API 인증 정보
        self.client_id = client_id
        self.client_secret = client_secret
        
        # 저장 경로
        self.output_dir = output_dir
        self.data_dir = os.path.join(output_dir, "data")
        self.html_dir = os.path.join(output_dir, "html")
        self.json_dir = os.path.join(output_dir, "json")
        
        # 디렉토리 생성
        for dir_path in [self.output_dir, self.data_dir, self.html_dir, self.json_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # HTTP 세션 초기화
        self.session = requests.Session()
        
        # 사용자 에이전트 목록
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/111.0.1661.54 Safari/537.36"
        ]
        
        # 기본 헤더 설정
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive'
        })
        
        # 수집 통계
        self.stats = {
            'total_searches': 0,
            'total_found': 0,
            'total_saved': 0,
            'failed_items': 0,
            'medicine_items': []
        }
        
        # 현재 HTML 저장 파일 및 아이템 카운터
        self.current_html_file = None
        self.current_html_count = 0
        self.html_item_limit = 100  # HTML 파일당 최대 항목 수
        
        logger.info("의약품 데이터 수집기 초기화 완료")
    
    def search_api(self, keyword, display=100, start=1):
        """
        네이버 검색 API를 사용하여 검색
        
        Args:
            keyword: 검색 키워드
            display: 결과 개수 (최대 100)
            start: 시작 인덱스
            
        Returns:
            dict: 검색 결과
        """
        # 수정/추가: API 요청 한도 체크 로직
        daily_request_path = os.path.join(self.output_dir, "daily_request_count.json")
        
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
                
                # 현재 진행 상태 저장
                self.save_checkpoint(keyword)
                
                # 요청 한도 초과 예외 발생
                raise Exception("일일 API 요청 한도 초과")
            
            # 요청 카운트 증가
            request_data['count'] += 1
            
            # 요청 카운트 저장
            with open(daily_request_path, 'w') as f:
                json.dump(request_data, f)
        
        except Exception as e:
            logger.error(f"API 요청 한도 체크 중 오류: {e}")
            raise

        # 기존 API 요청 로직 (수정 없음)
        # 검색 API 엔드포인트
        search_url = "https://openapi.naver.com/v1/search/encyc.json"
        
        # API 요청 헤더
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret
        }
        
        # 검색 파라미터
        params = {
            "query": f"{keyword} 의약품",
            "display": display,
            "start": start
        }
        
        try:
            # 요청 전 잠시 대기 (API 제한 방지)
            time.sleep(random.uniform(0.5, 1.5))
            
            # API 요청
            response = self.session.get(
                search_url, 
                headers=headers, 
                params=params,
                timeout=10
            )
            
            # 응답 확인
            response.raise_for_status()
            self.stats['total_searches'] += 1
            
            # JSON 결과 반환
            result = response.json()
            return result
            
        except requests.RequestException as e:
            logger.error(f"API 검색 오류 (키워드: {keyword}): {e}")
            return {"items": []}
    
    def filter_medicine_items(self, search_result):
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
    
    def fetch_medicine_data(self, item):
        """
        검색 결과 항목에서 의약품 페이지 데이터 추출
        
        Args:
            item: 검색 결과 항목
            
        Returns:
            dict: 추출된 의약품 데이터
        """
        medicine_data = {
            "title": re.sub(r'<.*?>', '', item.get("title", "")),
            "link": item.get("link", ""),
            "description": re.sub(r'<.*?>', '', item.get("description", "")),
            "category": item.get("category", ""),
            "collection_time": datetime.now().isoformat()
        }
        
        # URL 체크
        url = medicine_data["link"]
        if not url or not url.startswith("http"):
            logger.warning(f"유효하지 않은 URL: {url}")
            return None
        
        try:
            # 랜덤 대기
            time.sleep(random.uniform(0.5, 1.5))
            
            # 페이지 요청
            self.session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Referer': 'https://search.naver.com/'
            })
            
            response = self.session.get(url, timeout=10)
            
            # 페이지 유효성 확인
            if response.status_code != 200:
                logger.warning(f"페이지 가져오기 실패: {url}, 상태 코드: {response.status_code}")
                return None
            
            # HTML 파싱
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 의약품 페이지 확인
            if not self._is_medicine_page(soup):
                logger.warning(f"의약품 페이지가 아님: {url}")
                return None
            
            # 데이터 추출
            extracted_data = self._extract_medicine_data(soup, url)
            if not extracted_data:
                logger.warning(f"데이터 추출 실패: {url}")
                return None
            
            # 데이터 병합
            medicine_data.update(extracted_data)
            
            return medicine_data
            
        except Exception as e:
            logger.error(f"페이지 처리 중 오류: {url}, {e}")
            return None
    
    def _is_medicine_page(self, soup):
        """
        의약품사전 페이지 여부 확인 (개선된 버전)
        
        Args:
            soup: BeautifulSoup 객체
            
        Returns:
            bool: 의약품사전 페이지면 True
        """
        # 1. URL에 cid=51000이 포함된 경우 (이미 외부에서 검증)
        
        # 2. 의약품사전 키워드 포함 여부 (더 일반적인 방법)
        page_text = soup.get_text()
        if '의약품사전' in page_text:
            return True
        
        # 3. 의약품 관련 섹션 확인 (클래스에 의존하지 않음)
        section_titles = soup.find_all(['h3', 'h4', 'h5', 'strong'])
        medicine_sections = ["성분", "효능", "효과", "부작용", "용법", "용량", "성상", "보관", "주의사항"]
        
        for title in section_titles:
            if any(section in title.text for section in medicine_sections):
                return True
        
        # 4. 의약품 프로필 확인 (프로필 요소가 있는지)
        profile_elements = soup.find_all(['dl', 'dt', 'dd'])
        profile_keywords = ["분류", "업체명", "성상", "보험코드", "구분"]
        
        if profile_elements:
            profile_text = " ".join([elem.get_text() for elem in profile_elements])
            if any(keyword in profile_text for keyword in profile_keywords):
                return True
        
        return False
    
    def _extract_medicine_data(self, soup, url):
        """
        의약품 데이터 추출 메서드 (개선된 버전)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
            url (str): 현재 페이지 URL
        
        Returns:
            dict: 추출된 의약품 데이터
        """
        medicine_data = {
            "url": url,
            "extracted_time": datetime.now().isoformat()
        }

        # 1. 기본 타이틀 및 영문명 추출 (개선된 로직)
        self._extract_basic_info(soup, medicine_data)
        
        # 2. 이미지 추출
        img_data = self._extract_medicine_image(soup)
        if img_data:
            medicine_data.update(img_data)

        # 3. 분할선 정보 추출
        division_info = self._extract_division_info(soup)
        if division_info:
            medicine_data["division_info"] = division_info
        else:
            # 분할선 정보가 없는 경우 명시적으로 null 저장
            medicine_data["division_info"] = None

        # 4. 프로필 테이블 추출 (개선된 로직)
        profile_data = self._extract_profile_data(soup)
        medicine_data.update(profile_data)

        # 5. 섹션별 상세 정보 추출 (개선된 로직)
        section_data = self._extract_detailed_sections(soup)
        medicine_data.update(section_data)
        
        # 6. 식별 정보 추출 (새로운 기능)
        identification_data = self._extract_identification_info(soup)
        if identification_data:
            medicine_data.update(identification_data)

        # 필드명 정리
        self._normalize_field_names(medicine_data)
        
        # 디버그 정보: 추출된 필드
        found_fields = [key for key in medicine_data if key not in ["url", "extracted_time"]]
        logger.info(f"추출된 필드 ({len(found_fields)}개): {', '.join(found_fields)}")
        
        return medicine_data

    def _extract_basic_info(self, soup, medicine_data):
        """
        기본 정보 추출 (타이틀, 영문명)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
            medicine_data (dict): 데이터를 저장할 딕셔너리
        """
        # 한글 이름 추출 시도 (여러 클래스 시도)
        for selector in ['h2.headword', 'h3.headword', 'div.word_head h2', 'div.title_area h2', '.article_head h2']:
            title_tag = soup.select_one(selector)
            if title_tag:
                medicine_data["korean_name"] = title_tag.text.strip()
                break
        
        # 영문명 추출 (여러 클래스 시도)
        for selector in ['span.word_txt', 'p.eng_title', 'div.section_subtitle', '.eng_title']:
            eng_name_tag = soup.select_one(selector)
            if eng_name_tag:
                medicine_data["english_name"] = eng_name_tag.text.strip()
                break
        
        # 한글 이름이 없으면 타이틀 태그에서 시도
        if "korean_name" not in medicine_data:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.text.strip()
                # '- 네이버 지식백과' 등의 접미사 제거
                title_text = re.sub(r'[\s-]+네이버.*$', '', title_text)
                medicine_data["korean_name"] = title_text

    def _extract_medicine_image(self, soup):
        """
        의약품 이미지 정보 추출 (개선된 버전)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
            
        Returns:
            dict: 이미지 데이터
        """
        image_data = {}
        
        # 여러 이미지 컨테이너 선택자 시도
        for img_container in [
            'span.img_box', 
            'div.img_box',
            'div.thumb_area',
            'div.thumb_wrap',
            'div.photo_frame'
        ]:
            container = soup.select_one(img_container)
            if container and container.find('img'):
                img_tag = container.find('img')
                if 'src' in img_tag.attrs:
                    img_url = img_tag['src']
                    
                    # 상대 경로를 절대 경로로 변환
                    if not img_url.startswith('http'):
                        img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
                    
                    image_data["image_url"] = img_url
                    
                    # 추가 이미지 정보 추출
                    if 'width' in img_tag.attrs:
                        image_data["image_width"] = img_tag['width']
                    if 'height' in img_tag.attrs:
                        image_data["image_height"] = img_tag['height']
                    if 'alt' in img_tag.attrs:
                        image_data["image_alt"] = img_tag['alt']
                    
                    # 첫 번째 찾은 유효한 이미지에서 중단
                    break
        
        # 일반 이미지 태그에서 직접 찾기 (백업 방법)
        if "image_url" not in image_data:
            img_tags = soup.find_all('img')
            for img_tag in img_tags:
                # 의약품 관련 이미지 필터링
                if 'src' in img_tag.attrs and 'alt' in img_tag.attrs:
                    alt_text = img_tag['alt'].lower()
                    src_text = img_tag['src'].lower()
                    
                    # 관련 키워드가 있는 이미지만 선택
                    if any(keyword in alt_text for keyword in ['약', '정', '알약', '의약품', '캡슐']) or \
                       any(keyword in src_text for keyword in ['drug', 'pill', 'med', 'pharm']):
                        img_url = img_tag['src']
                        
                        # 상대 경로를 절대 경로로 변환
                        if not img_url.startswith('http'):
                            img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
                        
                        image_data["image_url"] = img_url
                        
                        # 추가 이미지 정보 추출
                        if 'width' in img_tag.attrs:
                            image_data["image_width"] = img_tag['width']
                        if 'height' in img_tag.attrs:
                            image_data["image_height"] = img_tag['height']
                        if 'alt' in img_tag.attrs:
                            image_data["image_alt"] = img_tag['alt']
                        
                        break
        
        return image_data
    
    def _extract_division_info(self, soup):
        """
        분할선 정보 추출 (td 태그 내용 직접 추출 버전)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
        
        Returns:
            dict or None: 분할선 정보 또는 분할선이 없는 경우 None
        """
        # 1. 분할선 라벨을 가진 th 태그 찾기
        for th in soup.find_all('th'):
            th_text = th.text.strip()
            # 분할선 관련 키워드 확인
            if '분할선' in th_text:
                # 같은 행(tr)에 있는 td 태그 찾기
                parent_tr = th.parent
                if parent_tr and parent_tr.name == 'tr':
                    td = parent_tr.find('td')
                    if td:
                        td_text = td.text.strip()
                        
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
        
        # 2. 텍스트 기반 검색 (기존 코드)
        division_elements = soup.find_all(['span', 'div'], string=re.compile(r'분할선|나누는.*선|절단선'))
        
        if division_elements:
            division_text = [elem.text.strip() for elem in division_elements]
            description = division_text[0] if division_text else None
            
            # 분할선 유형 결정
            division_type = None
            if description:
                if '+' in description:
                    division_type = "십자형"
                elif '-' in description:
                    division_type = "일자형"
                else:
                    division_type = "기타"
            
            return {
                "division_description": description,
                "division_type": division_type
            }
        
        # 3. 표에서 분할선 정보 검색 (백업 방법)
        for th in soup.find_all(['th', 'dt']):
            if any(keyword in th.text.lower() for keyword in ['분할선', '절단선', '나누는 선']):
                td = th.find_next(['td', 'dd'])
                if td:
                    description = td.text.strip()
                    
                    # 분할선 유형 결정
                    division_type = None
                    if '+' in description:
                        division_type = "십자형"
                    elif '-' in description:
                        division_type = "일자형"
                    else:
                        division_type = "기타"
                    
                    return {
                        "division_description": description,
                        "division_type": division_type
                    }
        
        # 4. 전체 텍스트에서 관련 패턴 찾기
        full_text = soup.get_text()
        division_patterns = [
            r'분할선[^\.\n]*?([+\-][\s,]*[+\-])',
            r'분할선[^\.\n]*?([+\-])',
            r'절단선[^\.\n]*?([+\-])'
        ]
        
        for pattern in division_patterns:
            match = re.search(pattern, full_text)
            if match:
                full_match = match.group(0)
                specific_mark = match.group(1)
                
                # 분할선 유형 결정
                division_type = "십자형" if "+" in specific_mark else "일자형" if "-" in specific_mark else "기타"
                
                return {
                    "division_description": full_match,
                    "division_type": division_type
                }
        
        # 분할선 정보가 없는 경우 None 반환
        return None

    def _extract_profile_data(self, soup):
        """
        프로필 데이터 추출 (개선된 버전)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
        
        Returns:
            dict: 프로필 데이터
        """
        profile_data = {}
        
        # 필드 매핑 (확장)
        field_mapping = {
            "분류": "category",
            "구분": "type", 
            "업체명": "company",
            "제조사": "company",
            "제조업체": "company",
            "성상": "appearance",
            "모양": "shape",
            "제형": "shape",
            "색깔": "color", 
            "색상": "color",
            "크기": "size",
            "식별표기": "identification",
            "식별부호": "identification",
            "식별코드": "identification",
            "보험코드": "insurance_code",
            "보험급여코드": "insurance_code",
            "허가일": "approval_date",
            "허가번호": "approval_number",
        }
        
        # 1. 프로필 테이블 및 div 검색 (클래스 기반)
        profile_sections = soup.find_all(['table', 'div'], class_=[
            'tmp_profile_tb', 'profile_table', 
            'wr_tmp_profile', 'tmp_profile', 'profile_wrap',
            'drug_info', 'drug_profile', 'medicine_info',
            'detail_table', 'detail_info'
        ])
        
        # 테이블에서 정보 추출
        for section in profile_sections:
            # 테이블 형식
            rows = section.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                
                if th and td:
                    field_name = th.text.strip()
                    field_value = td.text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name and data_key not in profile_data:
                            profile_data[data_key] = field_value
                            break
        
        # 2. 정의 리스트 형식 검색 (dl/dt/dd)
        for dl in soup.find_all('dl'):
            dt_tags = dl.find_all('dt')
            dd_tags = dl.find_all('dd')
            
            for i, dt in enumerate(dt_tags):
                if i < len(dd_tags):
                    field_name = dt.text.strip()
                    field_value = dd_tags[i].text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name and data_key not in profile_data:
                            profile_data[data_key] = field_value
                            break
        
        # 3. 기타 구조 (p 태그 기반)
        profile_paragraphs = soup.find_all('p', class_=['drug_info', 'drug_detail', 'medicine_profile'])
        
        for p in profile_paragraphs:
            text = p.text.strip()
            for key, data_key in field_mapping.items():
                pattern = f"{key}[:\\s]+([^,\\n]+)"
                match = re.search(pattern, text)
                if match and data_key not in profile_data:
                    profile_data[data_key] = match.group(1).strip()
        
        # 4. 색상 정보가 외형에 포함된 경우 분리
        if 'appearance' in profile_data and 'color' not in profile_data:
            appearance = profile_data['appearance']
            color_match = re.search(r'([가-힣]+\s*색|하양|노랑|파랑|빨강|초록|보라|주황|검정)', appearance)
            if color_match:
                profile_data['color'] = color_match.group(1)
        
        # 5. 모양 정보가 외형에 포함된 경우 분리
        if 'appearance' in profile_data and 'shape' not in profile_data:
            appearance = profile_data['appearance']
            shape_match = re.search(r'(정제|캡슐제|환제|산제|과립제|시럽제|주사제|[가-힣]+형|원형|타원형|삼각형|사각형|마름모)', appearance)
            if shape_match:
                profile_data['shape'] = shape_match.group(1)
        
        return profile_data

    def _extract_detailed_sections(self, soup):
        """
        섹션별 상세 정보 추출 (개선된 버전)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
        
        Returns:
            dict: 섹션별 상세 정보
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
            "유통기한": "expiration"
        }
        
        section_data = {}
        
        # 1. 헤딩 태그 기반 섹션 추출 (h2, h3, h4, h5)
        for heading_level in range(2, 6):
            for heading in soup.find_all(f'h{heading_level}'):
                heading_text = heading.text.strip()
                
                # 섹션 매핑 확인
                matched_key = None
                for key, value in section_mapping.items():
                    if key in heading_text and value not in section_data:
                        matched_key = value
                        break
                
                if not matched_key:
                    continue
                
                # 섹션 내용 추출
                contents = []
                current = heading.find_next_sibling()
                
                # 다음 헤딩 태그까지 내용 수집
                while current and current.name not in ['h2', 'h3', 'h4', 'h5']:
                    if current.name in ['p', 'div', 'ul', 'ol']:
                        # 명확한 내용을 가진 태그만 추가
                        content_text = current.text.strip()
                        if content_text and len(content_text) > 5:  # 최소 길이 필터
                            contents.append(content_text)
                    current = current.find_next_sibling()
                
                if contents:
                    section_data[matched_key] = "\n".join(contents)
        
        # 2. 클래스 기반 섹션 추출
        section_classes = [
            {'class': ['section', 'content', 'drug_section'], 'title_tag': ['h3', 'h4', 'strong']},
            {'class': ['cont_block', 'detail_info', 'detail_content'], 'title_tag': ['h3', 'h4', 'strong', 'dt']},
            {'class': ['drug_detail', 'medicine_info', 'drug_info'], 'title_tag': ['h3', 'h4', 'strong']}
        ]
        
        for class_info in section_classes:
            for section_class in class_info['class']:
                sections = soup.find_all(class_=section_class)
                
                for section in sections:
                    # 섹션 제목 찾기
                    section_title = None
                    for title_tag in class_info['title_tag']:
                        title_elem = section.find(title_tag)
                        if title_elem:
                            section_title = title_elem.text.strip()
                            break
                    
                    if not section_title:
                        continue
                    
                    # 섹션 매핑 확인
                    matched_key = None
                    for key, value in section_mapping.items():
                        if key in section_title and value not in section_data:
                            matched_key = value
                            break
                    
                    if not matched_key:
                        continue
                    
                    # 섹션 제목 요소 제외한 나머지 텍스트 추출
                    title_elem = None
                    for title_tag in class_info['title_tag']:
                        title_elem = section.find(title_tag)
                        if title_elem:
                            break
                    
                    if title_elem:
                        # 제목 요소 임시 제거
                        title_elem_copy = title_elem.extract()
                        section_content = section.text.strip()
                        # 제목 요소 복원
                        if title_elem_copy.parent:
                            title_elem_copy.parent.append(title_elem_copy)
                        
                        if section_content:
                            section_data[matched_key] = section_content
        
        # 3. 구조화된 DL/DT/DD 기반 섹션
        for dl in soup.find_all('dl'):
            for dt in dl.find_all('dt'):
                dt_text = dt.text.strip()
                
                # 매핑 확인
                matched_key = None
                for key, value in section_mapping.items():
                    if key in dt_text and value not in section_data:
                        matched_key = value
                        break
                
                if not matched_key:
                    continue
                
                # 해당 dt의 dd 찾기
                dd = dt.find_next('dd')
                if dd:
                    dd_text = dd.text.strip()
                    if dd_text:
                        section_data[matched_key] = dd_text
        
        return section_data
    
    def _extract_identification_info(self, soup):
        """
        식별 정보 추출 (색상, 모양, 크기 등)
        
        Args:
            soup (BeautifulSoup): 파싱된 HTML 객체
            
        Returns:
            dict: 식별 정보
        """
        identification_data = {}
        
        # 1. 색상 정보 추출 (단일 색상 또는 여러 색상)
        color_elements = soup.find_all(string=re.compile(r'[가-힣]+\s*색|하양|노랑|파랑|빨강|초록|보라|주황|검정'))
        color_matches = set()
        
        for element in color_elements:
            matches = re.findall(r'([가-힣]+\s*색|하양|노랑|파랑|빨강|초록|보라|주황|검정)', element)
            color_matches.update(matches)
        
        if color_matches and 'color' not in identification_data:
            identification_data['color'] = ', '.join(color_matches)
        
        # 2. 크기 정보 추출
        size_elements = soup.find_all(string=re.compile(r'(?:크기|직경|지름|두께)[:\s]*[0-9]+(?:\.[0-9]+)?(?:mm|㎜|cm|㎝)'))
        
        for element in size_elements:
            # 장축, 단축, 두께 등의 정보 추출
            size_matches = re.findall(r'(?:장축|단축|직경|지름|두께)[:\s]*([0-9]+(?:\.[0-9]+)?(?:mm|㎜|cm|㎝))', element)
            if size_matches:
                size_text = element.strip()
                if size_text:
                    identification_data['size'] = size_text
                    break
        
        # 3. 형태(모양) 정보 추출
        shape_elements = soup.find_all(string=re.compile(r'(?:원형|타원형|장방형|삼각형|사각형|오각형|육각형|마름모)'))
        
        for element in shape_elements:
            shape_match = re.search(r'(원형|타원형|장방형|삼각형|사각형|오각형|육각형|마름모)', element)
            if shape_match and 'shape' not in identification_data:
                identification_data['shape'] = shape_match.group(1)
                break
        
        return identification_data
    
    def _normalize_field_names(self, medicine_data):
        """
        필드명 정규화 (일관된 이름으로 변경)
        
        Args:
            medicine_data (dict): 정규화할 의약품 데이터
        """
        # 필드명 매핑 (예전 필드명 → 새 필드명)
        field_mapping = {
            "link": "source_url",  # 중복 필드 통합
            "description": "overview",  # 설명 필드 이름 변경
            "category_name": "category",  # 카테고리 필드 통합
            "medicine_name": "korean_name",  # 이름 필드 통합
            "eng_name": "english_name",  # 영문명 필드 통합
            "company_name": "company",  # 회사 필드 통합
            "shape_info": "shape",  # 모양 필드 통합
            "color_info": "color",  # 색상 필드 통합
            "size_info": "size",  # 크기 필드 통합
            "effect": "efficacy",  # 효과 필드 통합
            "caution": "precautions",  # 주의사항 필드 통합
            "usage": "dosage",  # 용법 필드 통합
            "storage_method": "storage",  # 보관 필드 통합
            "validity": "expiration",  # 유효기간 필드 통합
        }
        
        # 필드명 변경
        for old_key, new_key in field_mapping.items():
            if old_key in medicine_data and old_key != new_key:
                # 새 필드가 없거나, 새 필드가 있지만 값이 없는 경우
                if new_key not in medicine_data or not medicine_data[new_key]:
                    medicine_data[new_key] = medicine_data[old_key]
                # 기존 필드 삭제
                del medicine_data[old_key]

    def is_duplicate_medicine(self, medicine_data):
        """
        중복 의약품 검사
        
        Args:
            medicine_data (dict): 의약품 데이터
        
        Returns:
            bool: 중복 여부
        """
        # 고유 식별자 기준 중복 체크 (예: URL, ID)
        existing_ids_path = os.path.join(self.output_dir, "processed_medicine_ids.txt")
        
        # 의약품 고유 ID 생성 (기존 _generate_medicine_id 메서드 활용)
        medicine_id = self._generate_medicine_id(medicine_data)
        
        # 이미 처리된 ID 목록 로드
        processed_ids = set()
        if os.path.exists(existing_ids_path):
            with open(existing_ids_path, 'r', encoding='utf-8') as f:
                processed_ids = set(f.read().splitlines())
        
        # 중복 체크
        if medicine_id in processed_ids:
            return True
        
        # 새로운 ID 추가
        with open(existing_ids_path, 'a', encoding='utf-8') as f:
            f.write(f"{medicine_id}\n")
        
        return False

    def save_medicine_data(self, medicine_data):
        """
        의약품 데이터 저장 (개선된 버전)
        
        Args:
            medicine_data (dict): 저장할 의약품 데이터
        
        Returns:
            tuple: (성공 여부, 파일 경로)
        """
        try:
            # 중복 검사
            if self.is_duplicate_medicine(medicine_data):
                logger.info(f"중복 의약품 스킵: {medicine_data.get('korean_name', '이름 없음')}")
                return False, None
            
            if not medicine_data or "korean_name" not in medicine_data:
                return False, None
            
            # 1. 고유 ID 생성
            medicine_id = self._generate_medicine_id(medicine_data)
            medicine_data["id"] = medicine_id
            
            # 2. JSON 파일로 저장
            json_filename = f"{medicine_id}_{self._sanitize_filename(medicine_data['korean_name'])}.json"
            json_path = os.path.join(self.json_dir, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(medicine_data, f, ensure_ascii=False, indent=2)
            
            # 3. 통계 업데이트
            self.stats['total_saved'] += 1
            self.stats['medicine_items'].append({
                'id': medicine_id,
                'name': medicine_data['korean_name'],
                'path': json_path
            })
            
            # 4. HTML 보고서에 데이터 추가
            self._add_to_html_report(medicine_data)
            
            logger.info(f"의약품 데이터 저장 완료: {medicine_data['korean_name']} (ID: {medicine_id})")
            return True, json_path
            
        except Exception as e:
            # 상세한 오류 로깅
            logger.error(f"데이터 저장 중 오류: {e}")
            logger.error(f"오류 발생 데이터: {json.dumps(medicine_data, ensure_ascii=False, indent=2)}")
            
            # 오류 데이터 별도 저장
            error_log_path = os.path.join(self.output_dir, "error_logs", f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
            
            with open(error_log_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "error": str(e),
                    "medicine_data": medicine_data
                }, f, ensure_ascii=False, indent=2)
            
            self.stats['failed_items'] += 1
            return False, None
    
    def _generate_medicine_id(self, medicine_data):
        """
        의약품 고유 ID 생성
        
        Args:
            medicine_data: 의약품 데이터
            
        Returns:
            str: 생성된 ID
        """
        # URL에서 docId 추출 시도
        url = medicine_data.get('url', '')
        doc_id_match = re.search(r'docId=(\d+)', url)
        
        if doc_id_match:
            return f"M{doc_id_match.group(1)}"
        
        # URL이 없으면 이름 + 회사 기반으로 ID 생성
        name = medicine_data.get('korean_name', '')
        company = medicine_data.get('company', '')
        
        id_base = f"{name}_{company}"
        return f"MC{abs(hash(id_base)) % 10000000:07d}"
    
    def _sanitize_filename(self, filename):
        """
        안전한 파일명 생성
        
        Args:
            filename: 원본 파일명
            
        Returns:
            str: 안전한 파일명
        """
        # 파일명에 사용할 수 없는 문자 제거
        invalid_chars = r'[<>:"/\\|?*]'
        safe_filename = re.sub(invalid_chars, '_', filename)
        
        # 길이 제한
        if len(safe_filename) > 50:
            safe_filename = safe_filename[:50]
        
        return safe_filename
    
    def _init_html_report(self):
        """HTML 보고서 파일 초기화"""
        # 현재 HTML 파일 번호 계산
        html_files = [f for f in os.listdir(self.html_dir) if f.startswith('medicine_report_') and f.endswith('.html')]
        report_num = len(html_files) + 1
        
        # 파일명 생성
        html_filename = f"medicine_report_{report_num:03d}.html"
        html_path = os.path.join(self.html_dir, html_filename)
        
        # HTML 기본 구조 생성
        html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>의약품 데이터 수집 보고서</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        .medicine-item { 
            border: 1px solid #ddd; 
            margin-bottom: 20px; 
            padding: 15px; 
            border-radius: 5px;
            background-color: #f9f9f9;
        }
        .medicine-item h3 { 
            margin-top: 0; 
            color: #3498db; 
            border-bottom: 1px solid #ddd;
            padding-bottom: 5px;
        }
        .medicine-info { display: flex; flex-wrap: wrap; }
        .info-group { margin-right: 20px; margin-bottom: 10px; flex-basis: 30%; }
        .info-label { font-weight: bold; color: #7f8c8d; }
        .info-value { margin-left: 5px; }
        .missing { color: #e74c3c; }
        .exists { color: #27ae60; }
        .medicine-separator { 
            height: 2px; 
            background-color: #3498db; 
            margin: 30px 0; 
        }
        .timestamp { 
            color: #7f8c8d; 
            font-size: 0.8em; 
            text-align: right; 
            margin-top: 20px; 
        }
    </style>
</head>
<body>
    <h1>의약품 데이터 수집 보고서</h1>
    <div class="timestamp">생성 시간: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</div>
    <div id="medicine-list">
"""
        
        # 파일 저장
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # 현재 HTML 파일 정보 업데이트
        self.current_html_file = html_path
        self.current_html_count = 0
        
        return html_path
    
    def _add_to_html_report(self, medicine_data):
        """
        HTML 보고서에 의약품 데이터 추가
        
        Args:
            medicine_data: 추가할 의약품 데이터
        """
        # HTML 보고서 파일이 없거나 항목 수 제한에 도달한 경우 새 파일 생성
        if self.current_html_file is None or self.current_html_count >= self.html_item_limit:
            self._init_html_report()
        
        # 의약품 데이터를 HTML로 변환
        html_item = """
        <div class="medicine-item">
            <h3>""" + medicine_data.get('korean_name', '이름 없음') + """</h3>
            <div class="medicine-info">
    """
        
        # 기본 정보 그룹
        html_item += """
                <div class="info-group">
                    <div><span class="info-label">ID:</span> <span class="info-value">""" + medicine_data.get('id', '') + """</span></div>
                    <div><span class="info-label">영문명:</span> <span class="info-value">""" + medicine_data.get('english_name', '정보 없음') + """</span></div>
                    <div><span class="info-label">제조사:</span> <span class="info-value">""" + medicine_data.get('company', '정보 없음') + """</span></div>
                    <div><span class="info-label">분류:</span> <span class="info-value">""" + medicine_data.get('category', '정보 없음') + """</span></div>
                    <div><span class="info-label">보험코드:</span> <span class="info-value">""" + medicine_data.get('insurance_code', '정보 없음') + """</span></div>
                </div>
    """
        
        # 외형 정보 그룹
        html_item += """
                <div class="info-group">
                    <div><span class="info-label">성상:</span> <span class="info-value">""" + medicine_data.get('appearance', '정보 없음') + """</span></div>
                    <div><span class="info-label">모양:</span> <span class="info-value">""" + medicine_data.get('shape', '정보 없음') + """</span></div>
                    <div><span class="info-label">색깔:</span> <span class="info-value">""" + medicine_data.get('color', '정보 없음') + """</span></div>
                    <div><span class="info-label">크기:</span> <span class="info-value">""" + medicine_data.get('size', '정보 없음') + """</span></div>
                    <div><span class="info-label">식별표기:</span> <span class="info-value">""" + medicine_data.get('identification', '정보 없음') + """</span></div>
    """

        # 분할선 정보 추가
        division_info_text = "정보 없음"
        if "division_info" in medicine_data and medicine_data["division_info"]:
            if isinstance(medicine_data["division_info"], dict) and "division_description" in medicine_data["division_info"]:
                division_info_text = medicine_data["division_info"]["division_description"] or "정보 없음"
        
        html_item += """
                    <div><span class="info-label">분할선:</span> <span class="info-value">""" + division_info_text + """</span></div>
                </div>
    """
        
        # 데이터 완성도 그룹
        html_item += """
                <div class="info-group">
                    <div><span class="info-label">성분정보:</span> <span class="info-value """ + ('exists' if 'components' in medicine_data and medicine_data['components'] else 'missing') + """">""" + ('있음' if 'components' in medicine_data and medicine_data['components'] else '없음') + """</span></div>
                    <div><span class="info-label">효능효과:</span> <span class="info-value """ + ('exists' if 'efficacy' in medicine_data and medicine_data['efficacy'] else 'missing') + """">""" + ('있음' if 'efficacy' in medicine_data and medicine_data['efficacy'] else '없음') + """</span></div>
                    <div><span class="info-label">용법용량:</span> <span class="info-value """ + ('exists' if 'dosage' in medicine_data and medicine_data['dosage'] else 'missing') + """">""" + ('있음' if 'dosage' in medicine_data and medicine_data['dosage'] else '없음') + """</span></div>
                    <div><span class="info-label">주의사항:</span> <span class="info-value """ + ('exists' if 'precautions' in medicine_data and medicine_data['precautions'] else 'missing') + """">""" + ('있음' if 'precautions' in medicine_data and medicine_data['precautions'] else '없음') + """</span></div>
                    <div><span class="info-label">이미지:</span> <span class="info-value """ + ('exists' if 'image_url' in medicine_data and medicine_data['image_url'] else 'missing') + """">""" + ('있음' if 'image_url' in medicine_data and medicine_data['image_url'] else '없음') + """</span></div>
                </div>
    """
        
        # 항목 마무리
        html_item += """
            </div>
        </div>
        <div class="medicine-separator"></div>
    """
        
        # HTML 파일에 항목 추가
        with open(self.current_html_file, 'a', encoding='utf-8') as f:
            f.write(html_item)
        
        # 항목 카운터 증가
        self.current_html_count += 1
        
    def _finalize_html_report(self):
        """현재 HTML 보고서 마무리"""
        if self.current_html_file is None:
            return
        
        # HTML 마무리 태그 추가
        with open(self.current_html_file, 'a', encoding='utf-8') as f:
            f.write("""
    </div>
    <div class="timestamp">완료 시간: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</div>
</body>
</html>
""")
    
    def export_to_csv(self, output_path=None):
        """
        수집된 의약품 데이터를 CSV로 내보내기
        
        Args:
            output_path: 출력 파일 경로 (없으면 자동 생성)
            
        Returns:
            str: CSV 파일 경로
        """
        if not self.stats['medicine_items']:
            logger.warning("내보낼 의약품 데이터가 없습니다.")
            return None
        
        # 출력 경로 설정
        if output_path is None:
            output_path = os.path.join(self.output_dir, f"medicine_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        try:
            # 모든 JSON 파일 로드
            all_data = []
            for item in self.stats['medicine_items']:
                with open(item['path'], 'r', encoding='utf-8') as f:
                    medicine_data = json.load(f)
                    all_data.append(medicine_data)
            
            # 모든 키 수집
            all_keys = set()
            for data in all_data:
                all_keys.update(data.keys())
            
            # MySQL 친화적인 열 순서 설정
            # 중요 필드가 먼저 오도록 정렬
            ordered_keys = [
                'id', 'korean_name', 'english_name', 'category', 'type', 'company',
                'components', 'efficacy', 'dosage', 'precautions', 'storage', 'period',
                'appearance', 'shape', 'color', 'size', 'identification', 'insurance_code',
                'image_url', 'url', 'extracted_time'
            ]
            
            # 나머지 키는 알파벳 순으로 추가
            remaining_keys = sorted(list(all_keys - set(ordered_keys)))
            ordered_keys.extend(remaining_keys)
            
            # CSV 파일 작성
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_keys)
                writer.writeheader()
                for data in all_data:
                    writer.writerow({k: data.get(k, '') for k in ordered_keys})
            
            logger.info(f"CSV 내보내기 완료: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"CSV 내보내기 중 오류: {e}")
            return None
        
    def load_keywords(self):
        """
        todo 키워드 파일 로드
        """
        todo_path = os.path.join(self.output_dir, "keywords_todo.txt")
        done_path = os.path.join(self.output_dir, "keywords_done.txt")

        # todo 키워드 파일이 없으면 초기 키워드 생성
        if not os.path.exists(todo_path):
            initial_keywords = generate_medicine_keywords()
            with open(todo_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(initial_keywords))
            return initial_keywords

        # 기존 todo 키워드 로드
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = [line.strip() for line in f if line.strip()]

        return todo_keywords

    def update_keyword_progress(self, completed_keyword):
        """
        완료된 키워드 관리
        """
        todo_path = os.path.join(self.output_dir, "keywords_todo.txt")
        done_path = os.path.join(self.output_dir, "keywords_done.txt")

        # todo 키워드에서 제거
        todo_keywords = self.load_keywords()
        todo_keywords.remove(completed_keyword)

        # todo 키워드 파일 업데이트
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(todo_keywords))

        # done 키워드 파일에 추가
        with open(done_path, 'a', encoding='utf-8') as f:
            f.write(f"{completed_keyword}\n")

    def save_checkpoint(self, keyword):
        """
        현재 진행 상태 체크포인트 저장
        
        Args:
            keyword (str): 현재 처리 중인 키워드
        """
        # 체크포인트 경로 명시적으로 정의
        checkpoint_path = os.path.join(self.output_dir, "checkpoint.json")
        
        checkpoint_data = {
            "current_keyword": keyword,
            "total_searches": self.stats['total_searches'],
            "total_found": self.stats['total_found'],
            "total_saved": self.stats['total_saved'],
            "failed_items": self.stats['failed_items'],
            "timestamp": datetime.now().isoformat()
        }
        
        with open(checkpoint_path, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)

    def load_checkpoint(self):
        """
        마지막 체크포인트 로드
        
        Returns:
            dict or None: 체크포인트 데이터
        """
        checkpoint_path = os.path.join(self.output_dir, "checkpoint.json")
        
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        return None

    def collect_medicines(self, keywords, max_items=None):
        """
        키워드 목록으로 의약품 정보 수집 (개선된 버전)
        
        Args:
            keywords: 검색할 키워드 목록
            max_items: 최대 수집 항목 수
            
        Returns:
            dict: 수집 통계
        """
        if not keywords:
            logger.warning("검색할 키워드가 없습니다.")
            return self.stats
        
        logger.info(f"총 {len(keywords)}개 키워드로 검색 시작")
        
        # 체크포인트 로드
        checkpoint = self.load_checkpoint()
        
        # 통계 초기화
        self.stats = {
            'total_searches': checkpoint.get('total_searches', 0) if checkpoint else 0,
            'total_found': checkpoint.get('total_found', 0) if checkpoint else 0,
            'total_saved': checkpoint.get('total_saved', 0) if checkpoint else 0,
            'failed_items': checkpoint.get('failed_items', 0) if checkpoint else 0,
            'medicine_items': []
        }
        
        # 체크포인트가 있으면 이어서 진행
        if checkpoint:
            # 키워드 리스트 자르기 (이미 처리된 키워드 제외)
            start_index = keywords.index(checkpoint['current_keyword']) + 1 if checkpoint['current_keyword'] in keywords else 0
            keywords = keywords[start_index:]
        
        # 현재 진행 중인 키워드 표시
        with open(os.path.join(self.output_dir, "current_keyword.txt"), 'w', encoding='utf-8') as f:
            f.write(keywords[0] if keywords else "")
        
        try:
            # 진행 상황 표시
            for keyword in tqdm(keywords, desc="키워드 처리"):
                # 최대 항목 수 체크
                if max_items and self.stats['total_saved'] >= max_items:
                    logger.info(f"최대 항목 수 {max_items}개에 도달했습니다.")
                    break
                
                # 현재 키워드 체크포인트 저장
                self.save_checkpoint(keyword)
                
                logger.info(f"키워드 '{keyword}' 검색 중...")
                
                # API 검색
                search_result = self.search_api(keyword)
                
                # 의약품 항목 필터링
                medicine_items = self.filter_medicine_items(search_result)
                
                if not medicine_items:
                    logger.info(f"키워드 '{keyword}'에 대한 의약품 항목이 없습니다.")
                    continue
                
                # 필터링된 항목 수 기록
                item_count = len(medicine_items)
                self.stats['total_found'] += item_count
                logger.info(f"키워드 '{keyword}'에서 {item_count}개 의약품 항목 발견")
                
                # 각 항목 처리
                for item in medicine_items:
                    # 최대 항목 수 체크
                    if max_items and self.stats['total_saved'] >= max_items:
                        break
                    
                    try:
                        # 의약품 데이터 가져오기
                        medicine_data = self.fetch_medicine_data(item)
                        
                        if medicine_data:
                            # 데이터 저장
                            success, path = self.save_medicine_data(medicine_data)
                            
                            if not success:
                                self.stats['failed_items'] += 1
                                logger.warning(f"데이터 저장 실패: {item.get('title', '제목 없음')}")
                        else:
                            self.stats['failed_items'] += 1
                            logger.warning(f"데이터 가져오기 실패: {item.get('title', '제목 없음')}")
                    
                    except Exception as e:
                        self.stats['failed_items'] += 1
                        logger.error(f"항목 처리 중 오류: {e}")
                    
                    # 요청 간 지연
                    time.sleep(random.uniform(0.5, 1.5))
                
                # 키워드 진행 상태 업데이트
                self.update_keyword_progress(keyword)
            
            # HTML 보고서 마무리
            self._finalize_html_report()
            
            # CSV 파일 생성
            csv_path = self.export_to_csv()
            if csv_path:
                self.stats['csv_path'] = csv_path
            
            # 최종 통계 출력
            logger.info("의약품 수집 완료")
            logger.info(f"총 검색 횟수: {self.stats['total_searches']}회")
            logger.info(f"발견한 항목: {self.stats['total_found']}개")
            logger.info(f"저장된 항목: {self.stats['total_saved']}개")
            logger.info(f"실패한 항목: {self.stats['failed_items']}개")
        
        except Exception as e:
            logger.error(f"수집 중 오류 발생: {e}")
            # 마지막 체크포인트 저장
            self.save_checkpoint(keyword)
            raise
        
        finally:
            # 최종 체크포인트 제거
            checkpoint_path = os.path.join(self.output_dir, "checkpoint.json")
            if os.path.exists(checkpoint_path):
                os.remove(checkpoint_path)
        
        return self.stats
    
    def load_keywords(self):
        """
        todo 키워드 파일 로드
        """
        todo_path = os.path.join(self.output_dir, "keywords_todo.txt")
        done_path = os.path.join(self.output_dir, "keywords_done.txt")

        # todo 키워드 파일이 없으면 초기 키워드 생성
        if not os.path.exists(todo_path):
            initial_keywords = self.generate_medicine_keywords()
            with open(todo_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(initial_keywords))
            return initial_keywords

        # 기존 todo 키워드 로드
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = [line.strip() for line in f if line.strip()]

        return todo_keywords

    def update_keyword_progress(self, completed_keyword):
        """
        완료된 키워드 관리
        
        Args:
            completed_keyword (str): 완료된 키워드
        """
        todo_path = os.path.join(self.output_dir, "keywords_todo.txt")
        done_path = os.path.join(self.output_dir, "keywords_done.txt")

        # todo 키워드에서 제거
        todo_keywords = self.load_keywords()
        
        try:
            todo_keywords.remove(completed_keyword)
        except ValueError:
            logger.warning(f"키워드 '{completed_keyword}'를 todo 목록에서 찾을 수 없습니다.")

        # todo 키워드 파일 업데이트
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(todo_keywords))

        # done 키워드 파일에 추가
        with open(done_path, 'a', encoding='utf-8') as f:
            f.write(f"{completed_keyword}\n")
    
    def generate_medicine_keywords(self):
        """일반적인 의약품 검색 키워드 생성"""
        keywords = []
        
        # 인기 약품
        popular_medicines = [
            "타이레놀", "게보린", "아스피린", "부루펜", "판피린", "판콜", "텐텐", "이가탄",
            "베아제", "훼스탈", "백초시럽", "판콜에이", "신신파스", "포카시엘", "우루사", "인사돌",
            "센트럼", "삐콤씨", "컨디션", "박카스", "아로나민", "아모잘탄", "엔테론", "듀파락",
            "케토톱", "트라스트", "아록시아", "하루모아", "타세놀", "펜잘", "써스펜", "드로스피렌"
        ]
        keywords.extend(popular_medicines)
        
        # 의약품 카테고리
        categories = [
            "소화제", "진통제", "해열제", "항생제", "항히스타민제", "고혈압약", "당뇨약",
            "콜레스테롤약", "수면제", "항우울제", "항암제", "갑상선약", "비타민", "철분제",
            "혈압약", "구충제", "탈모약", "피부약", "알레르기약", "항균제", "제산제"
        ]
        keywords.extend(categories)
        
        # 주요 제약회사
        companies = [
            "동아제약", "유한양행", "녹십자", "한미약품", "종근당", "대웅제약", "일동제약",
            "보령제약", "SK케미칼", "삼성바이오로직스", "셀트리온", "JW중외제약", "한독",
            "동화약품", "삼진제약", "경동제약", "광동제약", "영진약품", "일양약품"
        ]
        
        # 제약회사 + 인기 약품 조합
        for company in companies[:5]:  # 상위 5개 회사만 사용
            for medicine in popular_medicines[:5]:  # 상위 5개 약품만 사용
                keywords.append(f"{company} {medicine}")
        
        # 중복 제거 및 정렬
        keywords = sorted(list(set(keywords)))
        
        return keywords

    def print_banner():
        """프로그램 시작 배너 출력"""
        banner = r"""
        ======================================================================
        네이버 검색 API 기반 의약품 정보 수집기 v1.0
        ======================================================================
        """
        print(banner)

def main():
    """
    메인 함수
    """
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='네이버 검색 API를 활용한 의약품 정보 수집기')
    parser.add_argument('--client-id', help='네이버 API 클라이언트 ID')
    parser.add_argument('--client-secret', help='네이버 API 클라이언트 시크릿')
    parser.add_argument('--output-dir', default='collected_data', help='데이터 저장 디렉토리')
    parser.add_argument('--max-items', type=int, help='최대 수집 항목 수')
    parser.add_argument('--keywords', help='검색할 키워드 (쉼표로 구분)')
    args = parser.parse_args()
    
    # 배너 출력
    MedicineCollector.print_banner()
    
    # API 인증 정보 확인
    client_id = args.client_id or os.environ.get('NAVER_CLIENT_ID')
    client_secret = args.client_secret or os.environ.get('NAVER_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("오류: 네이버 API 인증 정보를 찾을 수 없습니다.")
        print(".env 파일에 NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET을 설정하거나")
        print("명령행 인자로 --client-id와 --client-secret을 제공하세요.")
        return 1
    
    # 수집기 초기화
    collector = MedicineCollector(client_id, client_secret, args.output_dir)
    
    # 키워드 설정
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',')]
    else:
        print("검색 키워드를 생성합니다...")
        collector = MedicineCollector(client_id, client_secret, args.output_dir)
        keywords = collector.generate_medicine_keywords()
        print(f"총 {len(keywords)}개 키워드가 생성되었습니다.")
    
    # 수집 실행
    try:
        stats = collector.collect_medicines(keywords, args.max_items)
        
        # 결과 요약
        print("\n의약품 정보 수집 완료:")
        print(f"총 검색 횟수: {stats['total_searches']}회")
        print(f"발견한 항목: {stats['total_found']}개")
        print(f"저장된 항목: {stats['total_saved']}개")
        print(f"실패한 항목: {stats['failed_items']}개")
        
        if 'csv_path' in stats:
            print(f"CSV 파일: {stats['csv_path']}")
        
        # HTML 보고서 경로 안내
        html_files = [f for f in os.listdir(collector.html_dir) if f.endswith('.html')]
        if html_files:
            print(f"\nHTML 보고서가 {collector.html_dir} 디렉토리에 생성되었습니다.")
            for html_file in html_files:
                print(f"- {os.path.join(collector.html_dir, html_file)}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.")
        return 1
        
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}", exc_info=True)
        print(f"\n\n오류 발생: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())