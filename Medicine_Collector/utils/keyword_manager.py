#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
개선된 키워드 관리 기능 - 중복 문제 해결 및 처리완료 키워드 관리 강화
"""

import os
import re
import json
import glob
import logging
import random
from tqdm import tqdm

# 로거 설정
logger = logging.getLogger(__name__)
# 콘솔 로그 추가
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# 1. 키워드 정규화 함수 개선 (기존 normalize_keyword 대체)
def normalize_keyword(keyword):
    """
    키워드 정규화 - 개선된 버전
    
    Args:
        keyword (str): 원본 키워드
        
    Returns:
        str: 정규화된 키워드
    """
    if not keyword:
        return ""
    
    # 1. 공백 제거
    norm_keyword = keyword.strip()
    
    # 2. 괄호 및 괄호 내용 제거
    norm_keyword = re.sub(r'\([^)]*\)', '', norm_keyword).strip()
    
    # 3. 숫자와 단위 제거 (확장된 패턴)
    norm_keyword = re.sub(r'\d+(\.\d+)?(mg|ml|g|mcg|μg|%|정|캡슐|시럽|주|액|/\w+)', '', norm_keyword)
    
    # 4. '중' 접두사/접미사 제거
    norm_keyword = re.sub(r'^중\s*|\s*중$', '', norm_keyword)
    
    # 5. 일반적인 접두사/접미사 제거
    prefixes_suffixes = ['내수용', '수출용', '바이알', '각', '이상']
    for term in prefixes_suffixes:
        norm_keyword = re.sub(f'^{term}\\s*|\\s*{term}$', '', norm_keyword)
    
    # 6. 기타 특수문자 제거
    norm_keyword = re.sub(r'[^\w\s가-힣]', '', norm_keyword).strip()
    
    return norm_keyword

def load_keywords(output_dir):
    """
    키워드 파일 로드 - 중복 제거 기능 추가
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        list: 키워드 목록
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    # done 키워드 로드 (이미 처리된 키워드)
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
        logger.info(f"이미 처리된 키워드: {len(done_keywords)}개")

    # todo 키워드 파일이 없으면 초기 키워드 생성
    if not os.path.exists(todo_path):
        initial_keywords = generate_initial_keywords()
        
        # 이미 처리된 키워드 제외
        initial_keywords = [kw for kw in initial_keywords if kw not in done_keywords]
        
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(initial_keywords))
        logger.info(f"초기 키워드 생성 완료: {len(initial_keywords)}개")
        return initial_keywords

    # 기존 todo 키워드 로드 및 중복 제거
    with open(todo_path, 'r', encoding='utf-8') as f:
        todo_keywords = [line.strip() for line in f if line.strip()]
    
    # 중복 제거 및 이미 처리된 키워드 제외
    unique_todo_keywords = []
    seen = set()
    
    for keyword in todo_keywords:
        if keyword and keyword not in seen and keyword not in done_keywords:
            unique_todo_keywords.append(keyword)
            seen.add(keyword)
    
    # 중복이 제거되었다면 파일 업데이트
    if len(todo_keywords) != len(unique_todo_keywords):
        logger.info(f"중복 및 완료된 키워드 제거: {len(todo_keywords)} -> {len(unique_todo_keywords)}")
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(unique_todo_keywords))
    
    return unique_todo_keywords

def update_keyword_progress(completed_keyword, output_dir):
    """
    완료된 키워드 관리 - 강화된 버전
    
    Args:
        completed_keyword (str): 완료된 키워드
        output_dir (str): 출력 디렉토리
        
    Returns:
        bool: 업데이트 성공 여부
    """
    if not completed_keyword:
        logger.warning("빈 키워드는 처리할 수 없습니다.")
        return False
    
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    current_keyword_path = os.path.join(keywords_dir, "current_keyword.txt")

    # done 키워드 로드
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
    
    # 이미 처리된 키워드인지 확인
    if completed_keyword in done_keywords:
        logger.info(f"키워드 '{completed_keyword}'는 이미 처리 완료 상태입니다.")
        
        # todo 목록에서도 제거 (중복 방지)
        if os.path.exists(todo_path):
            todo_keywords = []
            with open(todo_path, 'r', encoding='utf-8') as f:
                todo_keywords = [line.strip() for line in f if line.strip()]
            
            if completed_keyword in todo_keywords:
                todo_keywords.remove(completed_keyword)
                with open(todo_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(todo_keywords))
                logger.info(f"중복된 완료 키워드를 todo 목록에서 제거했습니다: {completed_keyword}")
        
        return True
    
    # todo 키워드 로드
    todo_keywords = []
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = [line.strip() for line in f if line.strip()]
    
    # 완료된 키워드가 목록에 있으면 제거
    if completed_keyword in todo_keywords:
        todo_keywords.remove(completed_keyword)
        logger.info(f"키워드 완료 처리: {completed_keyword} (남은 키워드: {len(todo_keywords)}개)")
    
        # todo 키워드 파일 업데이트
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(todo_keywords))
    else:
        logger.warning(f"키워드 '{completed_keyword}'를 todo 목록에서 찾을 수 없습니다. 그래도 완료 목록에 추가합니다.")
    
    # done 키워드 파일에 추가
    with open(done_path, 'a', encoding='utf-8') as f:
        f.write(f"{completed_keyword}\n")
    
    # current_keyword.txt 파일 업데이트 (다음 키워드로)
    if todo_keywords:
        with open(current_keyword_path, 'w', encoding='utf-8') as f:
            f.write(todo_keywords[0])
    
    # done 키워드 목록 업데이트
    done_keywords.add(completed_keyword)
    logger.info(f"키워드 '{completed_keyword}'를 완료 목록에 추가했습니다. (총 {len(done_keywords)}개)")
    return True

def clean_keyword_files(output_dir):
    """
    키워드 파일 정리 - 중복 제거 및 done 목록 반영
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        tuple: (todo 키워드 수, done 키워드 수)
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    # done 키워드 로드
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
        
        # done 파일에서 중복 제거
        done_unique = list(done_keywords)
        with open(done_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(done_unique))
        
        logger.info(f"done 키워드 파일 정리 완료: {len(done_unique)}개")
    
    # todo 키워드 로드
    todo_keywords = []
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = [line.strip() for line in f if line.strip()]
        
        # 중복 제거 및 이미 처리된 키워드 제외
        unique_todo = []
        seen = set()
        
        for keyword in todo_keywords:
            if keyword and keyword not in seen and keyword not in done_keywords:
                unique_todo.append(keyword)
                seen.add(keyword)
        
        # 변경사항이 있으면 파일 업데이트
        if len(todo_keywords) != len(unique_todo):
            with open(todo_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(unique_todo))
            
            logger.info(f"todo 키워드 파일 정리 완료: {len(todo_keywords)} -> {len(unique_todo)}개")
        
        return len(unique_todo), len(done_keywords)
    
    return 0, len(done_keywords)

def check_keyword_status(keyword, output_dir):
    """
    키워드의 현재 상태 확인 (todo, done, 없음)
    
    Args:
        keyword (str): 확인할 키워드
        output_dir (str): 출력 디렉토리
        
    Returns:
        str: 'todo', 'done', 'none' 중 하나
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    # done 키워드 확인
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            if keyword in {line.strip() for line in f if line.strip()}:
                return 'done'
    
    # todo 키워드 확인
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            if keyword in {line.strip() for line in f if line.strip()}:
                return 'todo'
    
    return 'none'

def generate_initial_keywords():
    """
    초기 키워드 목록 생성
    
    Returns:
        list: 초기 키워드 목록
    """
    return [
        "타이레놀", "게보린", "아스피린", "부루펜", "판피린", "판콜", 
        "텐텐", "이가탄", "베아제", "훼스탈", "백초시럽", "판콜에이", 
        "신신파스", "포카시엘", "우루사", "인사돌", "센트럼", "삐콤씨", 
        "컨디션", "박카스", "아로나민", "아모잘탄", "엔테론", "듀파락"
    ]

# 2. 키워드 유효성 검사 함수 개선 (기존 is_generic_medicine_name 대체)
def is_generic_medicine_name(keyword):
    """
    일반적인 의약품 이름인지 확인 - 개선된 버전
    
    Args:
        keyword (str): 확인할 키워드
        
    Returns:
        bool: 일반적인 의약품 이름 여부
    """
    if not keyword or len(keyword) < 2:
        return False
    
    # 무의미한 패턴 제외
    invalid_patterns = [
        r'^중\s*$', r'^\s*중$', r'^mg\s*$', r'^\s*mg$',
        r'^g\s*$', r'^\s*g$', r'^ml\s*$', r'^\s*ml$',
        r'^각\s*$', r'^\s*각$', r'^및\s*$', r'^\s*및$',
        r'^염산$', r'^황산$', r'^ml 중$', r'^mL 중$', r'^L 중$'
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, keyword):
            return False
    
    # 의미없는 단일 문자 키워드 제외
    single_chars = ['L', 'A', 'B', 'C', 'E', 'g', 'l', 'p', 'd']
    if keyword in single_chars:
        return False
        
    # 너무 짧거나 긴 키워드 제외
    if len(keyword) < 2 or len(keyword) > 20:
        return False
        
    # 숫자만 있는 키워드 제외
    if keyword.isdigit():
        return False
    
    # 특수문자 비율이 높은 키워드 제외
    special_char_count = sum(1 for c in keyword if not c.isalnum() and c not in ' -')
    if special_char_count > len(keyword) * 0.2:
        return False
    
    # 숫자가 포함된 키워드는 특정 패턴만 허용
    has_digit = any(c.isdigit() for c in keyword)
    if has_digit:
        # '정', '캡슐', '시럽' 등이 포함된 경우는 허용 
        if any(term in keyword for term in ['정', '캡슐', '시럽', '주', '액', '크림', '겔', '로션']):
            return True
        # 그 외의 숫자 포함 키워드는 제외
        return False
    
    # 용매/성분만 있는 키워드 제외
    common_components = [
        '염산염', '황산염', '말레산염', '타르타르산염', '수화물', '질산염', 
        '아세트산염', '시트르산염', '포스페이트', '베실산염', '글루콘산염'
    ]
    
    for comp in common_components:
        if keyword.endswith(comp) and len(keyword) - len(comp) < 3:
            return False
    
    return True

def generate_extensive_initial_keywords(output_dir):
    """
    확장된 초기 키워드 생성 - 더 다양한 의약품 분류와 일반 의약품 포함
    디렉토리 경로 변경 적용
    
    Args:
        output_dir (str): 출력 디렉토리
    
    Returns:
        int: 추가된 키워드 개수
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    # done 키워드 로드
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
    
    # todo 키워드 로드
    todo_keywords = set()
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = {line.strip() for line in f if line.strip()}
    
    # 확장된 초기 키워드 목록
    extensive_keywords = [
        # 일반 의약품 분류
        "진통제", "해열제", "감기약", "소화제", "제산제", "지사제", "변비약", 
        "비타민", "종합비타민", "구충제", "항히스타민제", "항생제", "소염진통제",
        
        # 처방약 분류
        "고혈압약", "당뇨약", "콜레스테롤약", "항응고제", "항우울제", "항불안제",
        "수면제", "갑상선약", "관절염약", "천식약", "간질약", "빈혈약",
        
        # 의약품 투여 형태
        "정제", "캡슐", "주사제", "연고", "크림", "점안액", "점이액", 
        "좌제", "시럽", "패치", "겔", "스프레이",
        
        # 다빈도 일반의약품 브랜드
        "타이레놀", "게보린", "판콜", "베아제", "인사돌", "텐텐", "판피린",
        "부루펜", "아스피린", "판콜에이", "신신파스", "이가탄", "훼스탈", 
        "백초시럽", "센트룸", "삐콤씨", "컨디션", "박카스", "라니티딘",
        
        # 다빈도 처방약 성분
        "아목시실린", "세티리진", "로라타딘", "디클로페낙", "메트포민",
        "심바스타틴", "아토르바스타틴", "암로디핀", "오메프라졸", "라니티딘",
        "독시사이클린", "세파클러", "아세트아미노펜", "이부프로펜", "리도카인",
        
        # 일반 의약품 효능군
        "두통약", "치통약", "생리통약", "근육통약", "관절통약", "소화촉진제",
        "비염약", "기침약", "가래약", "멀미약", "숙취해소제", "알레르기약",
        "피부연고", "습포제", "구내염약", "안약", "피로회복제", "변비약"
    ]
    
    # 이미 처리된 키워드와 처리 예정 키워드 제외
    existing_keywords = done_keywords.union(todo_keywords)
    new_keywords = [kw for kw in extensive_keywords if kw not in existing_keywords]
    
    # 새 키워드를 todo 파일에 추가
    if new_keywords:
        updated_todo = todo_keywords.union(new_keywords)
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(updated_todo))
        
        logger.info(f"{len(new_keywords)}개 확장 초기 키워드 추가됨 (총 {len(updated_todo)}개)")
        return len(new_keywords)
    
    logger.info("추가할 새 키워드가 없습니다.")
    return 0

def ensure_keywords_available(output_dir, json_dir=None):
    """
    키워드가 있는지 확인하고, 없으면 핵심 키워드만 생성
    불필요한 키워드 생성 방지
    
    Args:
        output_dir (str): 출력 디렉토리
        json_dir (str): 사용하지 않음 (호환성 유지)
    
    Returns:
        bool: 키워드가 생성되었는지 여부
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    
    # 1. 키워드 파일 정리 (중복 제거 및 일관성 확보)
    clean_keyword_files(output_dir)
    
    # 2. todo 키워드가 있는지 확인
    todo_keywords = load_keywords(output_dir)
    logger.info(f"현재 처리할 키워드: {len(todo_keywords)}개")
    
    # 3. todo 키워드가 없으면 핵심 키워드만 생성
    if not todo_keywords:
        # 초기 키워드 생성
        initial_keywords = [
            # 검색 확률이 높은 주요 일반의약품만 선택
            "타이레놀", "게보린", "판콜", "부루펜", "판피린", 
            "우루사", "훼스탈", "아로나민", "베아제", "컨디션"
        ]
        
        # 파일에 저장
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(initial_keywords))
        
        logger.info(f"초기 키워드 {len(initial_keywords)}개 생성됨")
        return True
    
    logger.info(f"처리할 키워드가 {len(todo_keywords)}개 있습니다.")
    return True

def generate_medicine_keywords(output_dir, json_dir=None, max_new_keywords=20, similarity_threshold=0.8):
    """
    JSON 파일에서 의약품 관련 키워드를 추출
    
    Args:
        output_dir (str): 출력 디렉토리
        json_dir (str, optional): JSON 파일 디렉토리
        max_new_keywords (int): 최대 생성할 새 키워드 수
        similarity_threshold (float): 사용하지 않음 (호환성 유지)
    
    Returns:
        int: 추가된 키워드 수
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    logger.info("JSON 파일에서 의약품 키워드 추출 중...")
    
    # JSON 파일 디렉토리 확인
    if not json_dir or not os.path.exists(json_dir):
        json_dir = os.path.join(output_dir, "json")
        if not os.path.exists(json_dir):
            logger.warning(f"JSON 디렉토리를 찾을 수 없습니다: {json_dir}")
            return 0
    
    # JSON 파일 목록
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    
    if not json_files:
        logger.warning(f"JSON 디렉토리에 파일이 없습니다: {json_dir}")
        return 0
    
    logger.info(f"{len(json_files)}개 JSON 파일에서 키워드 추출 시작")
    
    # done 키워드 로드
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
    
    # todo 키워드 로드
    todo_keywords = set()
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = {line.strip() for line in f if line.strip()}
    
    # 기존 키워드
    existing_keywords = done_keywords.union(todo_keywords)
    logger.info(f"중복 검사를 위한 기존 키워드: {len(existing_keywords)}개 (done: {len(done_keywords)}, todo: {len(todo_keywords)})")
    
    # 새 키워드 후보
    new_keyword_candidates = set()
    
    # 필드 매핑 (JSON 필드명이 다를 경우를 위한 매핑)
    field_mapping = {
        # 원래 JSON 필드명: 표준화된 필드명
        'classification': 'classification',  # 분류
        'category': 'category',              # 구분
        'category_name': 'category', 
        'company': 'company',                # 업체명
        'company_name': 'company',
        'appearance': 'appearance',          # 성상
        'shape_type': 'shape_type',          # 제형
        'shape_info': 'shape_type'
    }
    
    # JSON 파일에서 키워드 추출
    from tqdm import tqdm
    for json_file in tqdm(json_files[:min(500, len(json_files))], desc="키워드 추출 중"):
        try:
            file_path = os.path.join(json_dir, json_file)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # 각 필드에서 키워드 추출
                for orig_field, std_field in field_mapping.items():
                    if orig_field in data and data[orig_field] and data[orig_field] != "정보 없음":
                        field_value = data[orig_field]
                        
                        # 1. 분류 필드 특별 처리
                        if std_field == 'classification':
                            # 분류 코드와 이름 분리 (예: [01140]해열.진통.소염제)
                            class_matches = re.findall(r'\[([^\]]+)\]([^,\[]+)', field_value)
                            if class_matches:
                                for code, name in class_matches:
                                    # 코드와 이름 각각 추가
                                    if code.strip() and len(code.strip()) >= 2:
                                        new_keyword_candidates.add(code.strip())
                                    if name.strip() and len(name.strip()) >= 2:
                                        new_keyword_candidates.add(name.strip())
                            
                            # 카테고리 계층 분리 (> 또는 . 기준)
                            classes = re.split(r'[/>.]', field_value)
                            for cls in classes:
                                # 괄호 제거 및 공백 제거
                                cls = re.sub(r'\[[^\]]*\]', '', cls).strip()
                                if cls and len(cls) >= 2:
                                    new_keyword_candidates.add(cls)
                        
                        # 2. 구분 필드 처리 (일반의약품, 전문의약품 등)
                        elif std_field == 'category':
                            # 복합 카테고리 분리
                            categories = re.split(r'[/,]', field_value)
                            for cat in categories:
                                cat = cat.strip()
                                if cat and len(cat) >= 2:
                                    new_keyword_candidates.add(cat)
                        
                        # 3. 업체명 필드 처리
                        elif std_field == 'company':
                            # 회사명에서 괄호 내용 제거 (예: (주)휴온스 -> 휴온스)
                            company_name = re.sub(r'\([^)]*\)', '', field_value).strip()
                            
                            # 회사명이 여러 단어로 구성된 경우 각 부분 추출 (예: 한국얀센제약 -> 한국얀센, 제약)
                            company_parts = re.findall(r'([가-힣A-Za-z]{2,})', company_name)
                            
                            for part in company_parts:
                                if part and len(part) >= 2:
                                    # '주식회사', '제약', '약품' 등 일반 단어는 제외
                                    common_words = ['주식회사', '제약', '약품', '바이오', '팜', '케미칼']
                                    if part not in common_words:
                                        new_keyword_candidates.add(part)
                            
                            # 전체 회사명도 추가
                            if company_name and len(company_name) >= 2:
                                new_keyword_candidates.add(company_name)
                        
                        # 4. 성상 필드 처리
                        elif std_field == 'appearance':
                            # 색상 추출 (성상에서 색상 정보 추출)
                            color_patterns = [
                                r'(흰색|백색|노란색|노랑|황색|주황색|빨간색|적색|분홍색|핑크색|보라색|자주색|파란색|청색|녹색|초록색|갈색|회색|검정색|투명)',
                                r'([가-힣]+(?:색|빛))'
                            ]
                            
                            for pattern in color_patterns:
                                colors = re.findall(pattern, field_value)
                                for color in colors:
                                    if isinstance(color, str) and color and len(color) >= 2:
                                        new_keyword_candidates.add(color)
                                    elif isinstance(color, tuple):
                                        for c in color:
                                            if c and len(c) >= 2:
                                                new_keyword_candidates.add(c)
                            
                            # 제형 정보 추출 (성상에서 제형 정보 추출)
                            shape_patterns = [
                                r'(정제|캡슐|시럽|액제|주사제|연고|크림|겔|좌제|산제|과립제|트로키|패치|스프레이)',
                                r'([가-힣]+(?:형|모양))'
                            ]
                            
                            for pattern in shape_patterns:
                                shapes = re.findall(pattern, field_value)
                                for shape in shapes:
                                    if isinstance(shape, str) and shape and len(shape) >= 2:
                                        new_keyword_candidates.add(shape)
                                    elif isinstance(shape, tuple):
                                        for s in shape:
                                            if s and len(s) >= 2:
                                                new_keyword_candidates.add(s)
                        
                        # 5. 제형 필드 처리
                        elif std_field == 'shape_type':
                            # 제형 추출
                            shape_types = re.split(r'[/,]', field_value)
                            for shape_type in shape_types:
                                shape_type = shape_type.strip()
                                if shape_type and len(shape_type) >= 2:
                                    new_keyword_candidates.add(shape_type)
        
        except Exception as e:
            logger.warning(f"파일 {json_file} 처리 중 오류: {str(e)}")
    
    # 키워드 필터링
    filtered_keywords = []
    for keyword in new_keyword_candidates:
        # 숫자나 단위가 포함된 키워드 제외
        if re.search(r'\d+\s*(?:mg|ml|g|mcg|μg|%|정|캡슐)', keyword):
            continue
            
        # 너무 짧거나 긴 키워드 제외
        if len(keyword) < 2 or len(keyword) > 20:
            continue
            
        # 특수문자 포함 키워드 제외 (일부 허용)
        if re.search(r'[^\w\s가-힣.-]', keyword):
            continue
            
        # 키워드 정규화
        keyword = normalize_keyword(keyword)
        if keyword and len(keyword) >= 2:
            filtered_keywords.append(keyword)
    
    # 중복 제거
    filtered_keywords = list(set(filtered_keywords))
    
    # 최대 키워드 수 제한
    filtered_keywords = filtered_keywords[:max_new_keywords]
    
    # 기존 키워드와 중복 제외
    truly_new_keywords = [kw for kw in filtered_keywords if kw not in existing_keywords]
    
    # todo 파일에 추가
    if truly_new_keywords:
        # 현재 todo 키워드 불러오기
        current_todo = []
        if os.path.exists(todo_path):
            with open(todo_path, 'r', encoding='utf-8') as f:
                current_todo = [line.strip() for line in f if line.strip()]
        
        # 새 키워드 추가
        updated_todo = current_todo + truly_new_keywords
        
        # 파일에 저장
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(updated_todo))
        
        logger.info(f"{len(truly_new_keywords)}개 키워드가 JSON 파일에서 추출되어 추가됨 (총 {len(updated_todo)}개)")
        return len(truly_new_keywords)
    else:
        logger.info("JSON 파일에서 추가할 새 키워드가 없습니다.")
        return 0
    
def check_keyword_status(keyword, output_dir):
    """
    키워드의 현재 상태 확인 (todo, done, 없음)
    
    Args:
        keyword (str): 확인할 키워드
        output_dir (str): 출력 디렉토리
        
    Returns:
        str: 'todo', 'done', 'none' 중 하나
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    todo_path = os.path.join(keywords_dir, "keywords_todo.txt")
    done_path = os.path.join(keywords_dir, "keywords_done.txt")
    
    # done 키워드 확인
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            if keyword in {line.strip() for line in f if line.strip()}:
                return 'done'
    
    # todo 키워드 확인
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            if keyword in {line.strip() for line in f if line.strip()}:
                return 'todo'
    
    return 'none'

def test_keyword_management(output_dir):
    """
    키워드 관리 기능 테스트 - 문제 발견을 위한 진단 도구
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        bool: 테스트 성공 여부
    """
    logger.info("키워드 관리 시스템 테스트 시작...")
    
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
    # 파일 존재 확인
    if not os.path.exists(todo_path):
        logger.error(f"keywords_todo.txt 파일이 없습니다: {todo_path}")
        return False
    
    if not os.path.exists(done_path):
        logger.warning(f"keywords_done.txt 파일이 없습니다: {done_path}")
        # done 파일 생성
        with open(done_path, 'w', encoding='utf-8') as f:
            f.write("")
        logger.info("빈 keywords_done.txt 파일을 생성했습니다.")
    
    # 중복 테스트
    todo_keywords = []
    with open(todo_path, 'r', encoding='utf-8') as f:
        todo_keywords = [line.strip() for line in f if line.strip()]
    
    done_keywords = []
    with open(done_path, 'r', encoding='utf-8') as f:
        done_keywords = [line.strip() for line in f if line.strip()]
    
    # 내부 중복 확인
    todo_duplicates = set([kw for kw in todo_keywords if todo_keywords.count(kw) > 1])
    done_duplicates = set([kw for kw in done_keywords if done_keywords.count(kw) > 1])
    
    if todo_duplicates:
        logger.warning(f"todo 목록에 중복된 키워드가 있습니다: {len(todo_duplicates)}개")
        logger.debug(f"중복 키워드: {', '.join(list(todo_duplicates)[:5])}..." if len(todo_duplicates) > 5 else f"중복 키워드: {', '.join(todo_duplicates)}")
    
    if done_duplicates:
        logger.warning(f"done 목록에 중복된 키워드가 있습니다: {len(done_duplicates)}개")
        logger.debug(f"중복 키워드: {', '.join(list(done_duplicates)[:5])}..." if len(done_duplicates) > 5 else f"중복 키워드: {', '.join(done_duplicates)}")
    
    # todo와 done 사이 중복 확인
    common_keywords = set(todo_keywords).intersection(set(done_keywords))
    if common_keywords:
        logger.warning(f"todo와 done 목록 사이에 중복된 키워드가 있습니다: {len(common_keywords)}개")
        logger.debug(f"중복 키워드: {', '.join(list(common_keywords)[:5])}..." if len(common_keywords) > 5 else f"중복 키워드: {', '.join(common_keywords)}")
    
    # 테스트 결과: 중복 없는 정상 상태면 True
    test_result = not (todo_duplicates or done_duplicates or common_keywords)
    
    if test_result:
        logger.info("키워드 관리 시스템 테스트 성공 - 중복 없음")
    else:
        logger.warning("키워드 관리 시스템 테스트 완료 - 문제 발견됨")
        logger.info("clean_keyword_files() 함수를 실행하여 문제를 해결하세요.")
    
    return test_result

# 3. 알파벳/한글 기반 체계적 키워드 생성 전략 (새 함수 추가)
def alphabetical_search_strategy(output_dir):
    """
    확률 기반 알파벳/한글 자모별 순차적 검색 전략
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        list: 생성된 검색 키워드 목록
    """
    # 키워드 디렉토리 경로 변경
    keywords_dir = os.path.join(output_dir, "keywords")
    os.makedirs(keywords_dir, exist_ok=True)
    
    # 진행 상황 파일
    progress_path = os.path.join(keywords_dir, "search_progress.json")
    
    # 검색 범위 정의
    search_chars = {
        'korean': ['가', '나', '다', '라', '마', '바', '사', '아', '자', '차', '카', '타', '파', '하'],
        'english': list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
        'korean_extensions': ['강', '경', '고', '구', '기', '두', '리', '미', '백', '복', '성', '신', '영', '원', '제', '진', '현']
    }
    
    # 의약품 접미사/조합
    medicine_suffixes = ['정', '캡슐', '주', '시럽', '크림', '액', '겔', '파스', '연고', '약']
    
    # 진행 상황 로드 또는 초기화
    if os.path.exists(progress_path):
        with open(progress_path, 'r', encoding='utf-8') as f:
            progress = json.load(f)
    else:
        progress = {
            'korean': {char: False for char in search_chars['korean']},
            'english': {char: False for char in search_chars['english']},
            'korean_extensions': {char: False for char in search_chars['korean_extensions']},
            'medicine_combinations': {f"{char}{suffix}": False 
                                     for char in search_chars['korean'] 
                                     for suffix in medicine_suffixes}
        }
    
    # 의약품 제조사 목록 (상위 제약사 키워드)
    manufacturers = [
        '한미', '동아', '유한양행', '종근당', '녹십자', '일동', '대웅',
        '광동', '보령', '삼진', '제일', '셀트리온', '한국얀센'
    ]
    
    # 일반 분류 키워드
    categories = [
        '항생제', '진통제', '해열제', '소화제', '항히스타민제',
        '당뇨약', '고혈압약', '콜레스테롤약', '항응고제', '항우울제', 
        '수면제', '항불안제', '갑상선약', '관절염약', '천식약'
    ]
    
    # 결과 키워드 목록
    next_chars = []
    
    # 1. 모든 카테고리에서 50%의 확률로 선택
    selection_categories = []
    if random.random() < 0.5:
        selection_categories.append('korean')
    if random.random() < 0.5:
        selection_categories.append('english')
    if random.random() < 0.5:
        selection_categories.append('korean_extensions')
    if random.random() < 0.5:
        selection_categories.append('medicine_combinations')
    
    # 적어도 하나의 카테고리는 선택되도록
    if not selection_categories:
        selection_categories = [random.choice(['korean', 'english', 'korean_extensions', 'medicine_combinations'])]
    
    # 2. 선택된 각 카테고리에서 아직 완료되지 않은 키워드 추출
    for category in selection_categories:
        if category == 'medicine_combinations':
            # 조합에서 완료되지 않은 것들
            incomplete_combinations = [combo for combo, status in progress[category].items() if not status]
            if incomplete_combinations:
                selected = random.sample(incomplete_combinations, min(3, len(incomplete_combinations)))
                for combo in selected:
                    next_chars.append(combo)
                    progress[category][combo] = True
        else:
            # 기본 문자에서 완료되지 않은 것들
            incomplete_chars = [char for char in search_chars[category] 
                              if char in progress[category] and not progress[category][char]]
            if incomplete_chars:
                selected = random.sample(incomplete_chars, min(2, len(incomplete_chars)))
                for char in selected:
                    next_chars.append(char)
                    progress[category][char] = True
    
    # 3. 무작위로 제조사 + 분류 조합 추가 (새로운 조합)
    if random.random() < 0.7:  # 70% 확률로 추가
        for _ in range(min(2, len(manufacturers))):
            manufacturer = random.choice(manufacturers)
            category = random.choice(categories)
            combo = f"{manufacturer} {category}"
            next_chars.append(combo)
    
    # 4. 모든 키워드가 소진된 경우 특수 키워드 생성
    if not next_chars:
        # 제조사와 카테고리 조합으로 새 키워드 생성
        for _ in range(5):
            manufacturer = random.choice(manufacturers)
            category = random.choice(categories)
            # 조합 방식 다양화
            if random.random() < 0.5:
                combo = f"{manufacturer} {category}"
            else:
                combo = f"{category} {manufacturer}"
            next_chars.append(combo)
        
        # 특정 의약품 형태 키워드 추가
        for _ in range(3):
            korean_char = random.choice(search_chars['korean'])
            suffix = random.choice(medicine_suffixes)
            next_chars.append(f"{korean_char}{suffix}")
    
    # 진행 상황 저장
    with open(progress_path, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    
    logger.info(f"알파벳/한글 검색 전략으로 {len(next_chars)}개 키워드 생성됨")
    return next_chars


# 주요 사용 예시
if __name__ == "__main__":
    # 로그 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 콘솔 출력
            logging.FileHandler('keyword_manager.log')  # 파일 출력
        ]
    )
    
    # 예시 디렉토리
    output_dir = "./output"
    json_dir = "./json_data"
    
    # 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)
    
    # 키워드 파일 정리
    clean_keyword_files(output_dir)
    
    # 키워드 로드
    todo_keywords = load_keywords(output_dir)
    logger.info(f"처리할 키워드: {len(todo_keywords)}개")
    
    # 키워드 관리 테스트
    test_keyword_management(output_dir)
    
    # 추가 키워드 생성 (JSON 파일이 있는 경우)
    if os.path.exists(json_dir) and os.listdir(json_dir):
        added_count = generate_medicine_keywords(output_dir, json_dir, max_new_keywords=50)
        logger.info(f"새 키워드 {added_count}개 추가 완료")