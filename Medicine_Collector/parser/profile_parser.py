#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
의약품 프로필 및 식별 정보 파싱 통합 모듈
"""

import re
import logging
from utils.safety import safe_regex_search, safe_regex_group

# 로거 설정
logger = logging.getLogger(__name__)

def extract_basic_info(soup, medicine_data):
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

def extract_profile_data(soup, elements_cache=None):
    """
    HTML 구조에서 직접 키-값 쌍으로 모든 프로필 데이터 추출 (통합 버전)
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        elements_cache (dict, optional): 미리 추출된 요소 캐시
    
    Returns:
        dict: 추출된 모든 프로필 데이터 (식별 정보 포함)
    """
    profile_data = {}
    
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
    
    # 1. 테이블 기반 데이터 추출 (가장 확실한 방법)
    profile_tables = []
    if elements_cache and 'profile_tables' in elements_cache:
        profile_tables = elements_cache['profile_tables']
    else:
        profile_tables = soup.find_all('table', class_=['tmp_profile_tb', 'profile_table', 'drug_info', 'drug_profile'])
    
    for table in profile_tables:
        rows = table.find_all('tr')
        for row in rows:
            # th와 td 쌍 찾기
            cells = row.find_all(['th', 'td'])
            if len(cells) >= 2:
                th = cells[0]
                td = cells[1]
                
                field_name = th.text.strip()
                field_value = td.text.strip()
                
                # 매핑된 필드 찾기
                for key, data_key in field_mapping.items():
                    if key in field_name:
                        # 값이 비어있으면 "정보 없음"으로 설정
                        profile_data[data_key] = field_value if field_value else "정보 없음"
                        
                        # 분할선 정보가 있으면 상세 정보 추가 분석
                        if key == "분할선" and field_value:
                            profile_data["division_info"] = analyze_division_line(field_value)
                        break
    
    # 2. 정의 리스트 형식 검색 (dl/dt/dd) - 최적화된 방식
    dl_elements = []
    if not profile_data:  # 테이블에서 추출 실패한 경우에만 시도
        if elements_cache and 'profile_dls' in elements_cache:
            dl_elements = elements_cache['profile_dls']
        else:
            dl_elements = soup.find_all('dl')
        
        for dl in dl_elements:
            dt_tags = dl.find_all('dt')
            dd_tags = dl.find_all('dd')
            
            # dt와 dd 쌍 처리
            for i, dt in enumerate(dt_tags):
                if i < len(dd_tags):
                    field_name = dt.text.strip()
                    field_value = dd_tags[i].text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name and data_key not in profile_data:
                            profile_data[data_key] = field_value if field_value else "정보 없음"
                            
                            # 분할선 정보가 있으면 상세 정보 추가 분석
                            if key == "분할선" and field_value:
                                profile_data["division_info"] = analyze_division_line(field_value)
                            break
    
    # 3. 클래스 기반 프로필 섹션 검색 (추가 백업 방법)
    if not profile_data:  # 위 방법들에서 추출 실패한 경우에만 시도
        profile_sections = soup.find_all(['div', 'section'], class_=[
            'wr_tmp_profile', 'tmp_profile', 'profile_wrap',
            'medicine_info', 'detail_table', 'detail_info'
        ])
        
        for section in profile_sections:
            # 키-값 쌍 추출 시도
            key_elems = section.find_all(['th', 'dt', 'strong', 'b'])
            for key_elem in key_elems:
                field_name = key_elem.text.strip()
                
                # 값 요소 찾기 (형제 또는 근접 요소)
                value_elem = None
                if key_elem.name == 'th':
                    # 테이블 행의 경우 같은 행의 td 찾기
                    parent_row = key_elem.parent
                    if parent_row and parent_row.name == 'tr':
                        value_elem = parent_row.find('td')
                elif key_elem.name in ['dt', 'strong', 'b']:
                    # 다음 형제 요소 찾기
                    next_elem = key_elem.find_next_sibling(['dd', 'span', 'div', 'p'])
                    if next_elem:
                        value_elem = next_elem
                
                if value_elem:
                    field_value = value_elem.text.strip()
                    
                    for key, data_key in field_mapping.items():
                        if key in field_name and data_key not in profile_data:
                            profile_data[data_key] = field_value if field_value else "정보 없음"
                            
                            # 분할선 정보가 있으면 상세 정보 추가 분석
                            if key == "분할선" and field_value and field_value != "정보 없음":
                                profile_data["division_info"] = analyze_division_line(field_value)
                            break
    
    # 4. 분할선 정보가 없는 경우 별도 추출 시도
    if "division_info" not in profile_data:
        division_info = extract_division_info(soup)
        if division_info:
            profile_data["division_info"] = division_info
            # division_line 필드도 일관성을 위해 설정
            profile_data["division_line"] = division_info.get("division_description", "정보 없음")
    
    # 5. 식별 정보 보완 추출 (HTML 구조에서 찾지 못한 경우)
    try:
        missing_fields = []
        if "color" not in profile_data:
            missing_fields.append("color")
        if "shape" not in profile_data:
            missing_fields.append("shape")
        if "size" not in profile_data:
            missing_fields.append("size")
        if "identification" not in profile_data:
            missing_fields.append("identification")
        
        # 누락된 필드가 있는 경우에만 추가 추출 시도
        if missing_fields:
            supplementary_data = extract_supplementary_identification(soup, missing_fields)
            for field, value in supplementary_data.items():
                if field not in profile_data or profile_data[field] == "정보 없음":
                    profile_data[field] = value
    except Exception as e:
        logger.warning(f"식별 정보 보완 추출 중 오류: {str(e)}")
    
    # 6. 데이터 표준화 및 정제
    standardize_profile_data(profile_data)
    
    return profile_data

def standardize_profile_data(profile_data):
    """
    프로필 데이터 표준화
    
    Args:
        profile_data (dict): 추출된 프로필 데이터
    """
    # 1. 색상 정보 표준화
    if "color" in profile_data and profile_data["color"] and profile_data["color"] != "정보 없음":
        color_text = profile_data["color"].lower()
        
        # 이미 표준화된 색상인지 확인 (중복 표준화 방지)
        standard_colors = ["흰색", "노란색", "황색", "주황색", "빨간색", "적색", "분홍색", 
                          "핑크색", "보라색", "자주색", "파란색", "청색", "녹색", "초록색", 
                          "갈색", "회색", "검정색", "투명"]
        
        # 표준화된 색상이 이미 있는지 확인
        if not any(std_color in color_text for std_color in standard_colors):
            # 표준화된 색상명으로 변환
            color_mapping = {
                "흰": "흰색", "백": "흰색", "화이트": "흰색",
                "노랑": "노란색", "노란": "노란색", "황": "노란색", "옐로우": "노란색", 
                "빨강": "빨간색", "빨간": "빨간색", "적": "빨간색", "레드": "빨간색",
                "파랑": "파란색", "파란": "파란색", "청": "파란색", "블루": "파란색",
                "초록": "녹색", "초록색": "녹색", "그린": "녹색",
                "주황": "주황색", "오렌지": "주황색",
                "보라": "보라색", "퍼플": "보라색",
                "분홍": "분홍색", "핑크": "분홍색"
            }
            
            # 단어 단위로 치환하도록 정규식 사용
            for orig, normalized in color_mapping.items():
                if orig in color_text and orig + "색" not in color_text:
                    color_text = color_text.replace(orig, normalized)
            
            profile_data["color"] = color_text
    
    # 2. 모양 정보 표준화
    if "shape" in profile_data and profile_data["shape"] and profile_data["shape"] != "정보 없음":
        shape_text = profile_data["shape"]
        
        # 모양 이름에서 불필요한 단어 제거
        shape_text = re.sub(r'(모양|형태|제형|정제)', '', shape_text)
        
        # 표준 모양명으로 변환
        shape_mapping = {
            "원": "원형",
            "타원": "타원형",
            "장방": "장방형",
            "삼각": "삼각형",
            "사각": "사각형",
            "오각": "오각형",
            "육각": "육각형",
            "팔각": "팔각형"
        }
        
        for orig, normalized in shape_mapping.items():
            if orig in shape_text and orig + "형" not in shape_text:
                shape_text = shape_text.replace(orig, normalized)
        
        profile_data["shape"] = shape_text.strip()
        
        # 모양 패턴 추출 (추가 정보)
        shape_pattern = re.compile(r'(원형|타원형|장방형|삼각형|사각형|오각형|육각형|마름모)')
        shape_match = shape_pattern.search(shape_text)
        if shape_match:
            profile_data["shape_type"] = shape_match.group(1)
    
    # 3. 크기 정보 표준화
    if "size" in profile_data and profile_data["size"] and profile_data["size"] != "정보 없음":
        size_text = profile_data["size"]
        
        # 단위 표준화
        size_text = re.sub(r'㎜', 'mm', size_text)
        size_text = re.sub(r'㎝', 'cm', size_text)
        
        # 숫자와 단위 사이 공백 표준화
        size_text = re.sub(r'(\d+(?:\.\d+)?)\s*(mm|cm)', r'\1\2', size_text)
        
        profile_data["size"] = size_text
        
        # 크기 정보 구조화 (추가 분석)
        size_info = {}
        size_pattern = re.compile(r'(장축|단축|지름|두께|높이)[:\s]*([0-9]+(?:\.[0-9]+)?(?:mm|㎜|cm|㎝)?)')
        size_matches = size_pattern.findall(size_text)
        
        if size_matches:
            for dimension, value in size_matches:
                size_info[dimension] = value.strip()
            
            # 원본 텍스트는 유지하면서 구조화된 정보 추가
            profile_data["size_info"] = size_info

def analyze_division_line(division_text):
    """
    분할선 정보 분석
    
    Args:
        division_text (str): 분할선 설명 텍스트
    
    Returns:
        dict: 분할선 정보 (설명 및 유형)
    """
    # 입력 검증
    if not division_text or not isinstance(division_text, str) or division_text == "정보 없음":
        return {"division_description": "정보 없음", "division_type": "없음"}
    
    # 원본 텍스트 보존
    original_text = division_text.strip()
    
    # 분할선 패턴 분석
    has_plus = '+' in original_text
    has_minus = '-' in original_text or '─' in original_text or '—' in original_text
    
    # 분할선 유형 분류
    if has_plus and has_minus:
        division_type = "십자형+일자형"
    elif has_plus:
        # + 개수 확인
        plus_count = original_text.count('+')
        if plus_count > 1:
            division_type = "다중십자형"
        else:
            division_type = "십자형"
    elif has_minus:
        # - 개수 확인
        minus_count = sum(1 for c in original_text if c in ['-', '─', '—'])
        if minus_count > 1:
            division_type = "다중일자형"
        else:
            division_type = "일자형"
    else:
        # 단어 기반 분석
        if "십자" in original_text:
            division_type = "십자형"
        elif "일자" in original_text or "한줄" in original_text:
            division_type = "일자형"
        elif "없" in original_text:
            division_type = "없음"
        else:
            division_type = "기타"
    
    return {
        "division_description": original_text,
        "division_type": division_type
    }

def extract_division_info(soup):
    """
    분할선 정보 추출 (th-td 쌍 분석)
    
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
    
    # 2. 텍스트 기반 검색 (백업 방법)
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
    
    # 3. 표에서 분할선 정보 검색 (추가 백업 방법)
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
    
    # 4. 전체 텍스트에서 관련 패턴 찾기 (최후의 백업 방법)
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

def extract_supplementary_identification(soup, missing_fields):
    """
    HTML 구조에서 추출하지 못한 식별 정보를 텍스트 기반으로 보완 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        missing_fields (list): 누락된 필드 목록
    
    Returns:
        dict: 보완된 식별 정보
    """
    supplementary_data = {}
    
    # 식별 정보 관련 텍스트 추출
    identification_tags = soup.find_all(['div', 'p', 'span', 'td', 'dd'], limit=100)
    identification_text = " ".join([tag.get_text() for tag in identification_tags])
    
    # 1. 색상 정보 추출
    if 'color' in missing_fields:
        try:
            color_patterns = [
                r'(색상|색깔)[:\s]*([가-힣]+(?:색|빛)?)',
                r'(흰색|백색|노란색|노랑|황색|주황색|빨간색|적색|분홍색|핑크색|보라색|자주색|파란색|청색|녹색|초록색|갈색|회색|검정색|투명)(?:의|색|빛)?'
            ]
            
            for pattern in color_patterns:
                color_match = safe_regex_search(pattern, identification_text)
                if color_match:
                    if '색상' in pattern or '색깔' in pattern:
                        # 첫 번째 패턴 - 두 번째 그룹이 색상
                        color = safe_regex_group(color_match, 2, "")
                    else:
                        # 두 번째 패턴 - 첫 번째 그룹이 색상
                        color = safe_regex_group(color_match, 1, "")
                    
                    if color:
                        supplementary_data['color'] = color.strip()
                        break
        except Exception as e:
            logger.warning(f"색상 정보 보완 추출 중 오류: {str(e)}")
    
    # 2. 모양 정보 추출
    if 'shape' in missing_fields:
        try:
            shape_patterns = [
                r'(모양|제형)[:\s]*([가-힣]+형)',
                r'(원형|타원형|장방형|삼각형|사각형|오각형|육각형|마름모)(?:의|모양)?'
            ]
            
            for pattern in shape_patterns:
                shape_match = safe_regex_search(pattern, identification_text)
                if shape_match:
                    if '모양' in pattern or '제형' in pattern:
                        # 첫 번째 패턴 - 두 번째 그룹이 모양
                        shape = safe_regex_group(shape_match, 2, "")
                    else:
                        # 두 번째 패턴 - 첫 번째 그룹이 모양
                        shape = safe_regex_group(shape_match, 1, "")
                    
                    if shape:
                        supplementary_data['shape'] = shape.strip()
                        break
        except Exception as e:
            logger.warning(f"모양 정보 보완 추출 중 오류: {str(e)}")
    
    # 3. 크기 정보 추출
    if 'size' in missing_fields:
        try:
            # 크기 정보 추출 - 다양한 패턴 고려
            size_patterns = [
                r'(크기|직경|지름|두께)[:\s]*([0-9]+(?:\.[0-9]+)?(?:mm|㎜|cm|㎝)?)',
                r'(장축|단축)[:\s]*([0-9]+(?:\.[0-9]+)?(?:mm|㎜|cm|㎝)?)'
            ]
            
            size_info = []
            for pattern in size_patterns:
                size_matches = re.finditer(pattern, identification_text)
                for match in size_matches:
                    try:
                        dimension = safe_regex_group(match, 1, "")
                        value = safe_regex_group(match, 2, "")
                        if dimension and value:
                            size_info.append(f"{dimension}: {value}")
                    except Exception:
                        continue
            
            if size_info:
                supplementary_data['size'] = ", ".join(size_info)
        except Exception as e:
            logger.warning(f"크기 정보 보완 추출 중 오류: {str(e)}")
    
    # 4. 식별 표기 정보 추출
    if 'identification' in missing_fields:
        try:
            id_patterns = [
                r'(식별(?:표시|표기|마크|코드))[:\s]*([A-Za-z0-9]+)',
                r'(식별(?:표시|표기|마크|코드))[:\s]*([^\n.,;]+)'
            ]
            
            for pattern in id_patterns:
                id_match = safe_regex_search(pattern, identification_text)
                if id_match:
                    id_text = safe_regex_group(id_match, 2, "").strip()
                    if id_text:
                        supplementary_data['identification'] = id_text
                        break
            
            # 식별 표기 단어나 레이블 없이 직접 표기된 경우 (일반적인 패턴)
            if 'identification' not in supplementary_data:
                try:
                    # 알파벳+숫자 조합 검색 (일반적인 식별 표기 패턴)
                    direct_id_pattern = r'\b([A-Z]{1,3}[0-9]{1,4}|[0-9]{1,4}[A-Z]{1,3})\b'
                    direct_matches = re.findall(direct_id_pattern, identification_text)
                    
                    if direct_matches:
                        # 가장 가능성 높은 패턴 선택 (첫 번째 매치)
                        supplementary_data['identification'] = direct_matches[0]
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"식별 표기 정보 보완 추출 중 오류: {str(e)}")
    
    return supplementary_data