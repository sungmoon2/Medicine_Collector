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
from tqdm import tqdm

# 로거 설정
logger = logging.getLogger(__name__)
# 콘솔 로그 추가
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

def normalize_keyword(keyword):
    """
    키워드 정규화 - 일관된 비교를 위한 전처리
    
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
    
    # 3. 숫자와 단위 제거 (mg, ml, g 등)
    norm_keyword = re.sub(r'\d+(\.\d+)?(mg|ml|g|mcg|μg|%|/\w+)', '', norm_keyword)
    
    # 4. 기타 특수문자 제거
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
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
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
    
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")

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
    
    # done 키워드 목록 업데이트
    done_keywords.add(completed_keyword)
    logger.info(f"키워드 '{completed_keyword}'를 완료 목록에 추가했습니다. (총 {len(done_keywords)}개)")
    return True

def check_keyword_status(keyword, output_dir):
    """
    키워드의 현재 상태 확인 (todo, done, 없음)
    
    Args:
        keyword (str): 확인할 키워드
        output_dir (str): 출력 디렉토리
        
    Returns:
        str: 'todo', 'done', 'none' 중 하나
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
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

def is_generic_medicine_name(keyword):
    """
    일반적인 의약품 이름인지 확인 (너무 구체적인 제품명, 성분명 등 제외)
    
    Args:
        keyword (str): 확인할 키워드
        
    Returns:
        bool: 일반적인 의약품 이름 여부
    """
    # 너무 짧거나 긴 키워드 제외
    if len(keyword) < 2 or len(keyword) > 15:
        return False
    
    # 숫자만 있는 키워드 제외
    if keyword.isdigit():
        return False
    
    # 숫자가 포함된 키워드 제외 (대부분 용량 표시)
    if any(c.isdigit() for c in keyword):
        return False
    
    # 특수문자 비율이 높은 키워드 제외
    special_char_count = sum(1 for c in keyword if not c.isalnum() and c not in ' -')
    if special_char_count > len(keyword) * 0.1:
        return False
    
    # 너무 많은 단어로 구성된 키워드 제외 (3개 이상 단어는 대부분 상세 제품명)
    words = keyword.split()
    if len(words) > 2:
        return False
    
    # 한 글자 단어만으로 구성된 키워드 제외
    if all(len(word) <= 1 for word in words):
        return False
    
    # 제조사 + 의약품명 패턴 제외
    company_patterns = [
        '(주)', '주식회사', '제약', '약품', '바이오', '팜', '케미칼', 
        '한미', '동아', '유한', '종근당', '녹십자', '일동', '대웅'
    ]
    
    for pattern in company_patterns:
        if pattern in keyword:
            return False
    
    return True

def generate_extensive_initial_keywords(output_dir):
    """
    확장된 초기 키워드 생성 - 더 다양한 의약품 분류와 일반 의약품 포함
    
    Args:
        output_dir (str): 출력 디렉토리
    
    Returns:
        int: 추가된 키워드 개수
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
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

def extract_keywords_from_existing_json(json_dir, output_dir, max_keywords=200):
    """
    기존 JSON 파일에서 직접 키워드 추출
    
    Args:
        json_dir (str): JSON 파일 디렉토리
        output_dir (str): 출력 디렉토리
        max_keywords (int): 최대 추출할 키워드 수
    
    Returns:
        int: 추가된 키워드 수
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
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
    
    # 추출 대상 필드
    target_fields = [
        'korean_name', 'english_name', 'classification', 'components',
        'category', 'efficacy'
    ]
    
    # JSON 파일에서 키워드 추출
    for json_file in tqdm(json_files[:min(500, len(json_files))], desc="키워드 추출 중"):
        try:
            file_path = os.path.join(json_dir, json_file)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # 각 필드에서 키워드 추출
                for field in target_fields:
                    if field in data and data[field] and data[field] != "정보 없음":
                        field_value = data[field]
                        
                        # 성분 필드 특별 처리
                        if field == 'components':
                            # 성분명 추출 패턴
                            components = re.findall(r'([가-힣a-zA-Z]+(?:\s[가-힣a-zA-Z]+)?)', field_value)
                            for comp in components:
                                comp = comp.strip()
                                if len(comp) >= 2 and comp not in existing_keywords:
                                    new_keyword_candidates.add(comp)
                        
                        # 분류 필드 특별 처리
                        elif field == 'classification':
                            # 분류명 추출
                            classes = re.split(r'[/,>]', field_value)
                            for cls in classes:
                                cls = cls.strip()
                                if len(cls) >= 2 and cls not in existing_keywords:
                                    new_keyword_candidates.add(cls)
                        
                        # 한글명, 영문명은 직접 추가
                        elif field in ['korean_name', 'english_name']:
                            # 괄호 제거
                            name = re.sub(r'\([^)]*\)', '', field_value).strip()
                            if len(name) >= 2 and name not in existing_keywords:
                                new_keyword_candidates.add(name)
                                
                            # 괄호 내용도 별도 키워드로 추출
                            parenthesis_content = re.findall(r'\(([^)]+)\)', field_value)
                            for content in parenthesis_content:
                                content = content.strip()
                                if len(content) >= 2 and content not in existing_keywords:
                                    new_keyword_candidates.add(content)
        
        except Exception as e:
            logger.warning(f"파일 {json_file} 처리 중 오류: {e}")
    
    # 키워드 필터링
    filtered_keywords = []
    for keyword in new_keyword_candidates:
        # 숫자나 단위가 포함된 키워드 제외
        if re.search(r'\d+\s*(?:mg|ml|g|mcg|μg|%|정|캡슐)', keyword):
            continue
            
        # 너무 짧거나 긴 키워드 제외
        if len(keyword) < 2 or len(keyword) > 20:
            continue
            
        # 특수문자 포함 키워드 제외
        if re.search(r'[^\w\s가-힣]', keyword):
            continue
            
        filtered_keywords.append(keyword)
    
    # 최대 키워드 수 제한
    filtered_keywords = filtered_keywords[:max_keywords]
    
    # 최종 중복 검사 (done 및 todo 목록 다시 로드)
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
    
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = {line.strip() for line in f if line.strip()}
    
    existing_keywords = done_keywords.union(todo_keywords)
    truly_new_keywords = [kw for kw in filtered_keywords if kw not in existing_keywords]
    
    # todo 파일에 추가
    if truly_new_keywords:
        updated_todo = todo_keywords.union(truly_new_keywords)
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(updated_todo))
        
        logger.info(f"{len(truly_new_keywords)}개 키워드가 JSON 파일에서 추출되어 추가됨 (총 {len(updated_todo)}개 todo)")
        return len(truly_new_keywords)
    
    logger.info("JSON 파일에서 추가할 새 키워드가 없습니다.")
    return 0

def ensure_keywords_available(output_dir, json_dir):
    """
    키워드가 있는지 확인하고, 없으면 다양한 방법으로 키워드 생성
    (done.txt 키워드는 재사용하지 않음)
    
    Args:
        output_dir (str): 출력 디렉토리
        json_dir (str): JSON 파일 디렉토리
    
    Returns:
        bool: 키워드가 생성되었는지 여부
    """
    # 1. 키워드 파일 정리 (중복 제거 및 일관성 확보)
    clean_keyword_files(output_dir)
    
    # 2. todo 키워드가 있는지 확인
    todo_keywords = load_keywords(output_dir)
    logger.info(f"현재 처리할 키워드: {len(todo_keywords)}개")
    
    # 3. todo 키워드가 없으면 다양한 방법으로 키워드 생성
    if not todo_keywords:
        # 3.1. 확장된 초기 키워드 생성
        added_initial = generate_extensive_initial_keywords(output_dir)
        
        # 3.2. 기존 JSON 파일에서 키워드 추출
        if added_initial == 0 and os.path.exists(json_dir):
            extract_keywords_from_existing_json(json_dir, output_dir)
    
    # 4. 최종 키워드 수 확인
    final_todo = load_keywords(output_dir)
    logger.info(f"최종 처리할 키워드: {len(final_todo)}개")
    
    return len(final_todo) > 0

def generate_medicine_keywords(output_dir, json_dir=None, max_new_keywords=100, similarity_threshold=0.8):
    """
    수집된 의약품 데이터를 분석하여 추가 키워드 생성 - 개선된 버전
    더 많은 키워드가 생성되도록 조건 완화
    
    Args:
        output_dir (str): 출력 디렉토리
        json_dir (str, optional): JSON 파일 디렉토리
        max_new_keywords (int): 최대 생성할 새 키워드 수
        similarity_threshold (float): 유사도 임계값 (0.0~1.0)
    
    Returns:
        int: 추가된 키워드 수
    """
    # 우선 기존 알고리즘으로 시도
    logger.info("추가 키워드 생성 시작...")
    
    # 기존 키워드 로드 (todo 및 done 모두)
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
    existing_keywords = set()
    normalized_existing_keywords = set()
    
    # 이미 처리된 키워드 (done_keywords)
    done_keywords = set()
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            done_keywords = {line.strip() for line in f if line.strip()}
        existing_keywords.update(done_keywords)
        # 정규화된 키워드도 저장
        normalized_existing_keywords.update(normalize_keyword(kw) for kw in done_keywords if kw)
        logger.info(f"이미 처리된 키워드: {len(done_keywords)}개")
    
    # 처리 예정 키워드 (todo_keywords)
    todo_keywords = set()
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            todo_keywords = {line.strip() for line in f if line.strip()}
        existing_keywords.update(todo_keywords)
        # 정규화된 키워드도 저장
        normalized_existing_keywords.update(normalize_keyword(kw) for kw in todo_keywords if kw)
        logger.info(f"처리 예정 키워드: {len(todo_keywords)}개")
    
    logger.info(f"기존 키워드 {len(existing_keywords)}개 로드 완료")
    
    # 먼저 확장된 초기 키워드로 시도
    new_count = generate_extensive_initial_keywords(output_dir)
    if new_count > 0:
        logger.info(f"확장된 초기 키워드 생성 성공: {new_count}개")
        return new_count
    
    # 그 다음 JSON 파일에서 키워드 추출
    if json_dir and os.path.exists(json_dir):
        new_count = extract_keywords_from_existing_json(json_dir, output_dir, max_new_keywords)
        if new_count > 0:
            logger.info(f"JSON 파일에서 키워드 추출 성공: {new_count}개")
            return new_count
    
    # 기존 알고리즘으로 계속 시도
    if not json_dir or not os.path.exists(json_dir) or not os.listdir(json_dir):
        logger.warning(f"JSON 디렉토리가 비어있거나 존재하지 않습니다: {json_dir}")
        
        # 초기 키워드 목록 확장 (더 많은 일반 의약품)
        initial_keywords = generate_extensive_initial_keywords(output_dir)
        if initial_keywords > 0:
            return initial_keywords
    
    # 최종적으로 기존 알고리즘 실행
    # JSON 파일 목록 (최신 파일 우선)
    if json_dir and os.path.exists(json_dir):
        json_files = sorted(
            glob.glob(os.path.join(json_dir, "*.json")),
            key=os.path.getmtime,
            reverse=True
        )
        
        # 이미 처리된 키워드로 검색된 의약품 데이터에서는 새 키워드를 추출하지 않도록 필터링
        processed_json_files = []
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 검색 키워드 또는 원본 쿼리가 이미 처리된 키워드인 경우 스킵
                    search_keyword = data.get('search_keyword', '')
                    origin_query = data.get('origin_query', '')
                    
                    if ((search_keyword and search_keyword in done_keywords) or 
                        (origin_query and origin_query in done_keywords)):
                        continue
                    
                    # 정규화된 키워드 비교도 추가
                    norm_search = normalize_keyword(search_keyword) if search_keyword else ''
                    norm_origin = normalize_keyword(origin_query) if origin_query else ''
                    
                    if ((norm_search and norm_search in normalized_existing_keywords) or 
                        (norm_origin and norm_origin in normalized_existing_keywords)):
                        continue
                    
                    processed_json_files.append(json_file)
            except Exception as e:
                logger.warning(f"JSON 파일 {json_file} 확인 중 오류: {e}")
        
        logger.info(f"처리된 키워드에서 검색된 JSON 제외 후: {len(processed_json_files)}/{len(json_files)}개")
        
        # 수집된 데이터에서 키워드 추출
        files_to_process = min(500, len(processed_json_files))  # 최대 500개 파일 처리
        logger.info(f"키워드 추출을 위해 최근 {files_to_process}개 JSON 파일 분석 중...")
        
        component_keywords = set()  # 성분명 키워드용
        brand_name_keywords = set()  # 브랜드명 키워드용
        category_keywords = set()  # 카테고리 키워드용
        
        for json_file in tqdm(processed_json_files[:files_to_process], desc="키워드 추출"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # 1. 의약품 이름에서 키워드 추출
                    if 'korean_name' in data and data['korean_name']:
                        name = data['korean_name']
                        
                        # 기본 이름 추출
                        clean_name = re.sub(r'\([^)]*\)', '', name).strip()
                        if clean_name:
                            brand_name_keywords.add(clean_name)
                        
                        # 괄호 안의 내용 추출 (성분명일 가능성)
                        components_in_name = re.findall(r'\(([^)]+)\)', name)
                        for comp_text in components_in_name:
                            # 용량 표시, 숫자, 단위 제외
                            if not re.search(r'\d+\s*[mg킬로그램캡슐정%]+', comp_text):
                                comps = comp_text.split(',')
                                for comp in comps:
                                    comp = comp.strip()
                                    if len(comp) >= 2 and '/' not in comp:
                                        component_keywords.add(comp)
                    
                    # 2. 카테고리에서 키워드 추출
                    if 'classification' in data and data['classification']:
                        classification = data['classification']
                        # 괄호 제거 및 코드 제거
                        clean_class = re.sub(r'\[[^\]]*\]', '', classification).strip()
                        if clean_class:
                            categories = clean_class.split('>')
                            for cat in categories:
                                cat = cat.strip()
                                if len(cat) >= 2:
                                    category_keywords.add(cat)
                    
                    # 3. 성분 정보에서 키워드 추출
                    if 'components' in data and data['components'] and data['components'] != "정보 없음":
                        components_text = data['components']
                        
                        # 주요 성분 추출 
                        comp_names = re.findall(r'([가-힣a-zA-Z]{2,})', components_text)
                        for comp in comp_names:
                            comp = comp.strip()
                            if len(comp) >= 3 and not any(char.isdigit() for char in comp):
                                component_keywords.add(comp)
                    
                    # 4. 제조사+브랜드 조합 추출
                    if 'company' in data and data['company'] and 'korean_name' in data and data['korean_name']:
                        company = data['company']
                        if company not in ["정보 없음", ""]:
                            # 제조사명만 추출 (괄호 내용 제거)
                            clean_company = re.sub(r'\([^)]*\)', '', company).strip()
                            if clean_company:
                                # 제조사명의 첫 부분만 사용 (주식회사, (주) 등 제거)
                                company_parts = clean_company.split()
                                if company_parts:
                                    company_name = company_parts[0].replace('(주)', '').strip()
                                    if company_name and len(company_name) >= 2:
                                        # 브랜드 시리즈 추출
                                        name = data['korean_name']
                                        # 제품명이 제조사명으로 시작하는 경우, 다음 부분을 시리즈명으로 간주
                                        if name.startswith(company_name):
                                            series_name = name[len(company_name):].strip()
                                            if series_name and len(series_name) >= 2:
                                                brand_name_keywords.add(series_name)
            
            except Exception as e:
                logger.warning(f"JSON 파일 {json_file} 처리 중 오류: {e}")
        
        # 모든 후보 키워드 합치기
        new_keyword_candidates = set()
        new_keyword_candidates.update(brand_name_keywords)
        new_keyword_candidates.update(component_keywords)
        new_keyword_candidates.update(category_keywords)
        
        logger.info(f"추출된 키워드 후보: {len(new_keyword_candidates)}개")
        logger.info(f"- 브랜드명: {len(brand_name_keywords)}개")
        logger.info(f"- 성분명: {len(component_keywords)}개")
        logger.info(f"- 카테고리: {len(category_keywords)}개")
        
        # 중복 키워드 필터
        new_keywords = new_keyword_candidates - existing_keywords
        logger.info(f"기존 키워드 제외 후: {len(new_keywords)}개")
        
        # 키워드 품질 필터링 (완화된 버전)
        def is_generic_medicine_name(keyword):
            """완화된 필터링 조건으로 의약품 키워드 확인"""
            # 길이가 너무 짧거나 긴 키워드 제외
            if len(keyword) < 2 or len(keyword) > 25:  # 길이 제한 완화
                return False
            
            # 숫자만 있는 키워드 제외
            if keyword.isdigit():
                return False
            
            # 특수문자 비율이 높은 키워드 제외
            special_char_count = sum(1 for c in keyword if not c.isalnum() and c not in ' -')
            if special_char_count > len(keyword) * 0.3:  # 허용 비율 증가
                return False
            
            # 키워드에 숫자가 포함된 경우 특정 패턴일 때만 허용
            has_digit = any(c.isdigit() for c in keyword)
            if has_digit:
                # '정', '캡슐', '시럽' 등이 포함된 경우는 허용 
                if any(term in keyword for term in ['정', '캡슐', '시럽', '주', '액', '크림', '겔', '로션']):
                    return True
                # 그 외의 숫자 포함 키워드는 제외
                return False
            
            # 너무 많은 단어로 구성된 키워드 제외
            words = keyword.split()
            if len(words) > 3:  # 허용 단어 수 증가
                return False
            
            # 한 글자 단어만 있는 경우 제외
            if all(len(word) <= 1 for word in words):
                return False
            
            return True
        
        filtered_keywords = []
        for keyword in new_keywords:
            if is_generic_medicine_name(keyword):
                filtered_keywords.append(keyword)
        
        logger.info(f"필터링 후 남은 키워드: {len(filtered_keywords)}개")
        
        # 유사도 기반 중복 제거 (완화된 버전)
        def is_similar(keyword1, keyword2, threshold=0.6):
            """두 키워드의 유사도 체크 - 완화된 버전"""
            if not keyword1 or not keyword2:
                return False
                
            # 정확히 일치
            if keyword1 == keyword2:
                return True
            
            # 키워드 정규화
            k1 = normalize_keyword(keyword1).lower()
            k2 = normalize_keyword(keyword2).lower()
            
            # 정규화 후 일치
            if k1 == k2:
                return True
            
            # 포함 관계 체크 (매우 엄격한 기준)
            if k1 in k2 or k2 in k1:
                min_len = min(len(k1), len(k2))
                max_len = max(len(k1), len(k2))
                # 최소 길이가 최대 길이의 90% 이상인 경우만 유사하다고 판단
                if min_len / max_len > 0.9:
                    return True
            
            # 단어 단위 체크 (더 관대한 기준)
            words1 = set(k1.split())
            words2 = set(k2.split())
            
            # 단어 포함 관계 (완화된 기준)
            if len(words1) > 0 and len(words2) > 0:
                # 단어가 하나이고 그 단어가 다른 키워드에 포함된 경우
                if len(words1) == 1 and words1.issubset(words2):
                    return True
                if len(words2) == 1 and words2.issubset(words1):
                    return True
            
            # 공통 단어 비율 (완화된 기준)
            common_words = words1.intersection(words2)
            all_words = words1.union(words2)
            if common_words and len(common_words) / len(all_words) > threshold:
                return True
            
            return False
        
        # 기존 키워드와 유사한 것 제외, 새 키워드들 사이에서도 중복 제거
        unique_keywords = []
        
        # 더 낮은 유사도 임계값 적용 (덜 엄격한 필터링)
        lower_threshold = min(0.6, similarity_threshold)
        
        # 기존 키워드 목록
        existing_keywords_list = list(existing_keywords)
        
        for new_kw in filtered_keywords:
            # 기존 키워드와 유사한지 체크 (매우 유사한 것만 제외)
            is_too_similar = False
            for existing_kw in existing_keywords_list:
                if is_similar(new_kw, existing_kw, 0.95):  # 95% 이상 유사한 것만 제외
                    is_too_similar = True
                    break
            
            if is_too_similar:
                continue
            
            # 이미 선택된 새 키워드와 유사한지 체크 (보통 유사한 것도 제외)
            is_too_similar = False
            for selected_kw in unique_keywords:
                if is_similar(new_kw, selected_kw, lower_threshold):
                    is_too_similar = True
                    break
            
            if not is_too_similar:
                unique_keywords.append(new_kw)
        
        logger.info(f"유사도 필터링 후 남은 키워드: {len(unique_keywords)}개")
        
        # 최대 키워드 수 제한
        selected_keywords = unique_keywords[:max_new_keywords]
        
        # 키워드 파일에 추가
        if selected_keywords:
            # 중복 방지를 위해 현재 todo 키워드 다시 로드
            current_todo = set()
            if os.path.exists(todo_path):
                with open(todo_path, 'r', encoding='utf-8') as f:
                    current_todo = {line.strip() for line in f if line.strip()}
            
            # 기존 todo 키워드와 새 키워드 합치기 (중복 제거)
            combined_keywords = current_todo.union(selected_keywords)
            
            # done 키워드 제외
            final_keywords = [kw for kw in combined_keywords if kw not in done_keywords]
            
            # 최종 키워드 목록을 파일에 저장
            with open(todo_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(final_keywords))
            
            logger.info(f"총 {len(selected_keywords)}개 키워드가 추가되었습니다. todo 목록 총 {len(final_keywords)}개")
        else:
            logger.info("추가할 새 키워드가 없습니다.")
        
        return len(selected_keywords)
    
    # 키워드를 생성할 수 없는 경우
    logger.info("키워드 생성을 위한 적절한 소스 데이터가 없습니다.")
    return 0

def clean_keyword_files(output_dir):
    """
    키워드 파일 정리 - 중복 제거 및 done 목록 반영
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        tuple: (todo 키워드 수, done 키워드 수)
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
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