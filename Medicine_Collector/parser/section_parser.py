#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
섹션별 상세 정보 파싱 모듈
"""

import re
import logging

# 로거 설정
logger = logging.getLogger(__name__)

def extract_detailed_sections(soup, elements_cache=None):
    """
    섹션별 상세 정보 추출 (성능 개선 버전)
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        elements_cache (dict, optional): 미리 추출된 요소 캐시
    
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
        "유통기한": "expiration",
        "약리작용": "pharmacology",
        "부작용": "side_effects",
        "이상반응": "side_effects",
        "상호작용": "interactions",
        "약물상호작용": "interactions"
    }
    
    # 추가 섹션 매핑 (의약품 정보에 자주 등장하지만 덜 일반적인 섹션)
    extended_section_mapping = {
        "과량투여": "overdose",
        "과다복용": "overdose",
        "임부투여": "pregnancy",
        "임신 중 투여": "pregnancy",
        "어린이투여": "pediatric_use",
        "소아투여": "pediatric_use",
        "노인투여": "geriatric_use",
        "고령자투여": "geriatric_use",
        "금기사항": "contraindications",
        "저장방법": "storage_conditions",
        "포장단위": "packaging",
        "제조원": "manufacturer"
    }
    
    # 섹션 매핑 통합
    section_mapping.update(extended_section_mapping)
    
    section_data = {}
    extracted_sections = set()  # 이미 추출한 섹션 추적
    
    try:
        # 1. 헤딩 태그 기반 섹션 추출 (효율적인 접근법)
        heading_tags = []
        if elements_cache and 'section_headings' in elements_cache:
            heading_tags = elements_cache['section_headings']
        else:
            for heading_level in range(2, 6):
                try:
                    heading_tags.extend(soup.find_all(f'h{heading_level}'))
                except Exception as e:
                    logger.warning(f"헤딩 태그 h{heading_level} 검색 중 오류: {str(e)}")
            
        for heading in heading_tags:
            try:
                heading_text = heading.text.strip()
                
                # 섹션 매핑 확인
                matched_key = None
                for key, value in section_mapping.items():
                    if key in heading_text and value not in extracted_sections:
                        matched_key = value
                        break
                
                if not matched_key:
                    continue
                
                # 섹션 내용 추출 (최적화된 방식)
                contents = []
                current = heading.find_next_sibling()
                next_heading = None
                
                # 다음 헤딩 찾기
                try:
                    for tag in heading.find_all_next(['h2', 'h3', 'h4', 'h5']):
                        next_heading = tag
                        break
                except Exception as e:
                    logger.warning(f"다음 헤딩 검색 중 오류: {str(e)}")
                
                # 다음 헤딩까지 내용 수집
                while current and (not next_heading or current != next_heading):
                    if current.name in ['p', 'div', 'ul', 'ol', 'table']:
                        # 명확한 내용을 가진 태그만 추가
                        try:
                            content_text = current.text.strip()
                            if content_text:  # 최소 길이 필터 제거
                                contents.append(content_text)
                        except Exception:
                            pass
                    
                    try:
                        current = current.find_next_sibling()
                    except Exception:
                        break
                
                if contents:
                    section_data[matched_key] = "\n".join(contents)
                    extracted_sections.add(matched_key)
            except Exception as e:
                logger.warning(f"헤딩 태그 처리 중 오류: {str(e)}")
        
        # 2. 클래스 기반 섹션 추출 (최적화)
        content_sections = []
        if elements_cache and 'content_sections' in elements_cache:
            content_sections = elements_cache['content_sections']
        else:
            section_classes = [
                'section', 'content', 'drug_section', 'cont_block', 
                'detail_info', 'detail_content', 'drug_detail', 
                'medicine_info', 'drug_info'
            ]
            for class_name in section_classes:
                try:
                    content_sections.extend(soup.find_all(class_=class_name))
                except Exception as e:
                    logger.warning(f"클래스 {class_name} 검색 중 오류: {str(e)}")
        
        for section in content_sections:
            try:
                # 섹션 제목 찾기
                section_title = None
                title_elem = None
                
                try:
                    title_elem = section.find(['h3', 'h4', 'strong', 'dt', 'th'])
                except Exception as e:
                    logger.warning(f"섹션 제목 요소 검색 중 오류: {str(e)}")
                
                if title_elem:
                    section_title = title_elem.text.strip()
                
                    # 섹션 매핑 확인
                    matched_key = None
                    for key, value in section_mapping.items():
                        if key in section_title and value not in extracted_sections:
                            matched_key = value
                            break
                    
                    if not matched_key:
                        continue
                    
                    # 제목 요소 임시 제거하여 내용만 추출 (안전하게 처리)
                    title_elem_copy = None
                    section_content = ""
                    
                    try:
                        title_elem_copy = title_elem.extract()
                        section_content = section.text.strip()
                    except Exception:
                        # 추출 실패 시 직접 텍스트 얻기
                        try:
                            section_content = section.text.strip()
                            if title_elem and section_title in section_content:
                                section_content = section_content.replace(section_title, "", 1).strip()
                        except Exception:
                            pass
                    
                    # 제목 요소 복원 (안전하게 처리)
                    try:
                        if title_elem_copy and title_elem_copy.parent:
                            title_elem_copy.parent.append(title_elem_copy)
                    except Exception:
                        pass
                    
                    if section_content:
                        # 내용 정제
                        try:
                            section_content = clean_section_content(section_content)
                        except Exception:
                            pass
                        
                        section_data[matched_key] = section_content
                        extracted_sections.add(matched_key)
            except Exception as e:
                logger.warning(f"섹션 처리 중 오류: {str(e)}")
        
        # 3. 구조화된 DL/DT/DD 기반 섹션 (최적화)
        dl_elements = []
        if elements_cache and 'profile_dls' in elements_cache:
            dl_elements = elements_cache['profile_dls']
        else:
            try:
                dl_elements = soup.find_all('dl')
            except Exception as e:
                logger.warning(f"DL 요소 검색 중 오류: {str(e)}")
        
        for dl in dl_elements:
            try:
                dt_elements = dl.find_all('dt')
                for dt in dt_elements:
                    dt_text = dt.text.strip()
                    
                    # 매핑 확인
                    matched_key = None
                    for key, value in section_mapping.items():
                        if key in dt_text and value not in extracted_sections:
                            matched_key = value
                            break
                    
                    if not matched_key:
                        continue
                    
                    # 해당 dt의 dd 찾기
                    try:
                        dd = dt.find_next('dd')
                        if dd:
                            dd_text = dd.text.strip()
                            if dd_text:
                                # 내용 정제
                                try:
                                    dd_text = clean_section_content(dd_text)
                                except Exception:
                                    pass
                                
                                section_data[matched_key] = dd_text
                                extracted_sections.add(matched_key)
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"DL/DT/DD 처리 중 오류: {str(e)}")
        
        # 4. 섹션별 추가 처리 및 정제
        if "components" in section_data:
            try:
                section_data["components"] = process_components_section(section_data["components"])
            except Exception as e:
                logger.warning(f"성분 섹션 처리 중 오류: {str(e)}")
        
        if "efficacy" in section_data:
            try:
                section_data["efficacy"] = process_efficacy_section(section_data["efficacy"])
            except Exception as e:
                logger.warning(f"효능 섹션 처리 중 오류: {str(e)}")
        
        if "dosage" in section_data:
            try:
                section_data["dosage"] = process_dosage_section(section_data["dosage"])
            except Exception as e:
                logger.warning(f"용량 섹션 처리 중 오류: {str(e)}")
        
        if "precautions" in section_data:
            try:
                section_data["precautions"] = process_precautions_section(section_data["precautions"])
            except Exception as e:
                logger.warning(f"주의사항 섹션 처리 중 오류: {str(e)}")
    
    except Exception as e:
        logger.warning(f"섹션 추출 중 일반 오류: {str(e)}")
    
    return section_data

def clean_section_content(content):
    """
    섹션 내용 정제
    
    Args:
        content (str): 원본 섹션 내용
    
    Returns:
        str: 정제된 섹션 내용
    """
    if not content:
        return ""
    
    # 여러 줄바꿈을 하나로 통합
    cleaned = re.sub(r'\n\s*\n', '\n', content)
    
    # 앞뒤 공백 제거
    cleaned = cleaned.strip()
    
    # 불필요한 공백 제거
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # 줄바꿈 처리 (문단 구분 유지)
    cleaned = re.sub(r'([.!?)])(\s*)\n', r'\1\n\n', cleaned)
    
    return cleaned

def process_components_section(components_text):
    """
    성분 섹션 처리
    
    Args:
        components_text (str): 성분 섹션 텍스트
    
    Returns:
        str: 처리된 성분 섹션 텍스트
    """
    # 이미 깔끔하게 정제되어 있으면 그대로 반환
    if len(components_text) < 500 and '\n' in components_text:
        return components_text
    
    # 주성분 패턴 찾기
    components_pattern = re.compile(r'([^,.:;]+)(?:[.,:;]\s*|$)')
    components = components_pattern.findall(components_text)
    
    # 성분마다 줄바꿈 추가
    if components and len(components) > 1:
        return '\n'.join(comp.strip() for comp in components if comp.strip())
    
    return components_text

def process_efficacy_section(efficacy_text):
    """
    효능효과 섹션 처리
    
    Args:
        efficacy_text (str): 효능효과 섹션 텍스트
    
    Returns:
        str: 처리된 효능효과 섹션 텍스트
    """
    # 문단 분리가 잘 안된 경우 처리
    if '\n' not in efficacy_text and len(efficacy_text) > 100:
        # 문장 끝으로 분리
        sentences = re.split(r'([.!?])\s+', efficacy_text)
        processed_text = ""
        
        for i in range(0, len(sentences)-1, 2):
            if i+1 < len(sentences):
                processed_text += sentences[i] + sentences[i+1] + "\n"
            else:
                processed_text += sentences[i] + "\n"
        
        return processed_text.strip()
    
    return efficacy_text

def process_dosage_section(dosage_text):
    """
    용법용량 섹션 처리
    
    Args:
        dosage_text (str): 용법용량 섹션 텍스트
    
    Returns:
        str: 처리된 용법용량 섹션 텍스트
    """
    # 숫자+단위 패턴 강조
    dosage_pattern = re.compile(r'(\d+(?:\.\d+)?(?:\s*~\s*\d+(?:\.\d+)?)?\s*(?:mg|g|mL|정|캡슐|방울|μg|mcg|IU))')
    
    # 이미 줄바꿈이 적절히 있는 경우 그대로 반환
    if '\n' in dosage_text and dosage_text.count('\n') > 2:
        return dosage_text
    
    # 투여 대상별 분리 시도
    target_patterns = [
        r'(?:성인|어른)(?:[의:]\s*경우|은|투여량|용량|복용량|사용법)?',
        r'(?:소아|어린이)(?:[의:]\s*경우|은|투여량|용량|복용량|사용법)?',
        r'(?:고령자|노인|노약자)(?:[의:]\s*경우|은|투여량|용량|복용량|사용법)?'
    ]
    
    processed_text = dosage_text
    for pattern in target_patterns:
        processed_text = re.sub(f'({pattern})', r'\n\n\1', processed_text)
    
    # 번호 목록 형식 정리
    processed_text = re.sub(r'(\d+\.\s+)', r'\n\1', processed_text)
    
    return processed_text.strip()

def process_precautions_section(precautions_text):
    """
    주의사항 섹션 처리
    
    Args:
        precautions_text (str): 주의사항 섹션 텍스트
    
    Returns:
        str: 처리된 주의사항 섹션 텍스트
    """
    # 이미 줄바꿈이 적절히 있는 경우 그대로 반환
    if '\n' in precautions_text and precautions_text.count('\n') > 3:
        return precautions_text
    
    # 주의사항 분류별 분리
    caution_categories = [
        r'(?:다음|경우|환자)[에는]\s*(?:투여하지|사용하지)\s*(?:말|마십시오|않는다)',
        r'(?:다음|경우)[에는]\s*(?:신중히|주의하여)\s*(?:투여|사용)하십시오',
        r'(?:이상반응|부작용)',
        r'(?:상호작용)',
        r'(?:임부|임신|수유부|수유)[에]\s*(?:대한|관한)\s*(?:투여|사용)',
        r'(?:소아|어린이)[에]\s*(?:대한|관한)\s*(?:투여|사용)',
        r'(?:고령자|노인)[에]\s*(?:대한|관한)\s*(?:투여|사용)'
    ]
    
    processed_text = precautions_text
    for category in caution_categories:
        processed_text = re.sub(f'({category})', r'\n\n\1', processed_text)
    
    # 번호 목록 형식 정리
    processed_text = re.sub(r'(\d+\.\s+)', r'\n\1', processed_text)
    
    # 중요 경고 문구 강조
    warning_patterns = [
        r'경고',
        r'주의',
        r'금기',
        r'위험',
        r'심각한'
    ]
    
    for warning in warning_patterns:
        processed_text = re.sub(f'({warning})', r'\n\1', processed_text)
    
    return processed_text.strip()

def normalize_field_names(medicine_data):
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