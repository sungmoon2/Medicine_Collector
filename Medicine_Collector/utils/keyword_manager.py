#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
키워드 관리 기능
"""

import os
import re
import json
import glob
import logging
from tqdm import tqdm

# 로거 설정
logger = logging.getLogger(__name__)

def load_keywords(output_dir):
    """
    키워드 파일 로드
    
    Args:
        output_dir (str): 출력 디렉토리
        
    Returns:
        list: 키워드 목록
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")

    # todo 키워드 파일이 없으면 초기 키워드 생성
    if not os.path.exists(todo_path):
        initial_keywords = generate_initial_keywords()
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(initial_keywords))
        return initial_keywords

    # 기존 todo 키워드 로드
    with open(todo_path, 'r', encoding='utf-8') as f:
        todo_keywords = [line.strip() for line in f if line.strip()]

    return todo_keywords

def update_keyword_progress(completed_keyword, output_dir):
    """
    완료된 키워드 관리
    
    Args:
        completed_keyword (str): 완료된 키워드
        output_dir (str): 출력 디렉토리
    """
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")

    # todo 키워드 로드
    todo_keywords = []
    with open(todo_path, 'r', encoding='utf-8') as f:
        todo_keywords = [line.strip() for line in f if line.strip()]
    
    # 완료된 키워드가 목록에 있으면 제거
    if completed_keyword in todo_keywords:
        todo_keywords.remove(completed_keyword)
        logger.info(f"키워드 완료 처리: {completed_keyword} (남은 키워드: {len(todo_keywords)}개)")
    
        # todo 키워드 파일 업데이트
        with open(todo_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(todo_keywords))
    
        # done 키워드 파일에 추가 (중복 체크)
        done_keywords = set()
        if os.path.exists(done_path):
            with open(done_path, 'r', encoding='utf-8') as f:
                done_keywords = set(line.strip() for line in f if line.strip())
        
        if completed_keyword not in done_keywords:
            with open(done_path, 'a', encoding='utf-8') as f:
                f.write(f"{completed_keyword}\n")
    else:
        logger.warning(f"키워드 '{completed_keyword}'를 todo 목록에서 찾을 수 없습니다.")

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

def generate_medicine_keywords(output_dir, json_dir, max_new_keywords=100, similarity_threshold=0.8):
    """
    수집된 의약품 데이터를 분석하여 추가 키워드 생성
    
    Args:
        output_dir (str): 출력 디렉토리
        json_dir (str): JSON 파일 디렉토리
        max_new_keywords (int): 최대 생성할 새 키워드 수
        similarity_threshold (float): 유사도 임계값 (0.0~1.0)
    
    Returns:
        int: 추가된 키워드 수
    """
    logger.info("추가 키워드 생성 시작...")
    
    # 기존 키워드 로드
    todo_path = os.path.join(output_dir, "keywords_todo.txt")
    done_path = os.path.join(output_dir, "keywords_done.txt")
    
    existing_keywords = set()
    
    # 처리 예정 키워드
    if os.path.exists(todo_path):
        with open(todo_path, 'r', encoding='utf-8') as f:
            existing_keywords.update(line.strip() for line in f if line.strip())
    
    # 이미 처리된 키워드
    if os.path.exists(done_path):
        with open(done_path, 'r', encoding='utf-8') as f:
            existing_keywords.update(line.strip() for line in f if line.strip())
    
    logger.info(f"기존 키워드 {len(existing_keywords)}개 로드 완료")
    
    # 새 키워드 후보 추출
    new_keyword_candidates = set()
    
    # 1. JSON 파일에서 의약품 데이터 추출
    # JSON 디렉토리가 비어있거나 존재하지 않는 경우
    if not os.path.exists(json_dir) or not os.listdir(json_dir):
        logger.warning(f"JSON 디렉토리가 비어있거나 존재하지 않습니다: {json_dir}")
        
        # 초기 키워드 기본 리스트로 파일에 추가
        initial_keywords = generate_initial_keywords()
        
        # 초기 키워드를 todo 파일에 추가
        with open(todo_path, 'w', encoding='utf-8') as f:
            for keyword in initial_keywords:
                f.write(f"{keyword}\n")
        
        logger.info(f"초기 키워드 {len(initial_keywords)}개가 추가되었습니다.")
        return len(initial_keywords)
    
    # JSON 파일 목록 (최신 파일 우선)
    json_files = sorted(
        glob.glob(os.path.join(json_dir, "*.json")),
        key=os.path.getmtime,
        reverse=True
    )
    
    # 수집된 최근 데이터에서 키워드 추출 (최대 500개 파일)
    files_to_process = min(500, len(json_files))
    logger.info(f"키워드 추출을 위해 최근 {files_to_process}개 JSON 파일 분석 중...")
    
    for json_file in tqdm(json_files[:files_to_process], desc="키워드 추출"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # 의약품 이름에서 키워드 추출
                if 'korean_name' in data and data['korean_name']:
                    name = data['korean_name']
                    # 기본 이름 추가
                    new_keyword_candidates.add(name)
                    
                    # 괄호 제거한 이름 추가
                    clean_name = re.sub(r'\([^)]*\)', '', name).strip()
                    if clean_name and clean_name != name:
                        new_keyword_candidates.add(clean_name)
                
                # 회사명 + 의약품명 조합 (유명 제약사만)
                if 'company' in data and data['company'] and 'korean_name' in data and data['korean_name']:
                    company = data['company'].strip()
                    name = data['korean_name'].strip()
                    
                    # 주요 제약사 필터링
                    major_companies = [
                        '동아제약', '유한양행', '녹십자', '한미약품', '종근당', '대웅제약', '일동제약',
                        '보령제약', 'SK케미칼', '삼성바이오로직스', '셀트리온', 'JW중외제약', '한독',
                        '동화약품', '삼진제약', '경동제약', '광동제약', '영진약품', '일양약품'
                    ]
                    
                    for major_company in major_companies:
                        if major_company in company:
                            combined = f"{company} {name}"
                            new_keyword_candidates.add(combined)
                            break
                
                # 카테고리에서 키워드 추출
                if 'category' in data and data['category']:
                    category = data['category']
                    # 대괄호 안의 코드 제거
                    category_name = re.sub(r'\[[^\]]*\]', '', category).strip()
                    if category_name:
                        new_keyword_candidates.add(category_name)
                
                # 성분 정보에서 키워드 추출
                if 'components' in data and data['components']:
                    components_text = data['components']
                    # 주성분 추출 (첫 번째 줄이나 문장)
                    first_line = components_text.split('\n')[0] if '\n' in components_text else components_text
                    first_sentence = first_line.split('.')[0] if '.' in first_line else first_line
                    
                    # 성분명 패턴 추출
                    component_names = re.findall(r'([가-힣a-zA-Z0-9]+(?:\s[가-힣a-zA-Z0-9]+){0,2})', first_sentence)
                    for comp in component_names:
                        if len(comp) > 2 and not any(char.isdigit() for char in comp):
                            new_keyword_candidates.add(comp)
                
        except Exception as e:
            logger.warning(f"JSON 파일 {json_file} 처리 중 오류: {e}")
    
    # 2. 키워드 필터링 및 선별
    logger.info(f"추출된 키워드 후보: {len(new_keyword_candidates)}개")
    
    # 중복 키워드 필터
    new_keywords = new_keyword_candidates - existing_keywords
    logger.info(f"기존 키워드 제외 후: {len(new_keywords)}개")
    
    # 키워드 품질 필터링
    filtered_keywords = []
    for keyword in new_keywords:
        # 길이가 너무 짧거나 긴 키워드 제외
        if len(keyword) < 2 or len(keyword) > 30:
            continue
        
        # 숫자만 있는 키워드 제외
        if keyword.isdigit():
            continue
        
        # 특수문자 포함 비율이 높은 키워드 제외
        special_char_count = sum(1 for c in keyword if not c.isalnum() and c not in ' -')
        if special_char_count > len(keyword) * 0.3:
            continue
        
        # 유효한 키워드 추가
        filtered_keywords.append(keyword)
    
    logger.info(f"필터링 후 남은 키워드: {len(filtered_keywords)}개")
    
    # 유사도 기반 중복 제거 (기존 키워드와 너무 유사한 키워드 제외)
    def is_similar(keyword1, keyword2, threshold=0.8):
        """두 키워드의 유사도 체크"""
        if keyword1 == keyword2:
            return True
        
        # 간단한 포함 관계 체크
        if keyword1 in keyword2 or keyword2 in keyword1:
            min_len = min(len(keyword1), len(keyword2))
            max_len = max(len(keyword1), len(keyword2))
            if min_len / max_len > threshold:
                return True
        
        # 자소 분리 없는 간단한 유사도 체크 (옵션)
        common_chars = set(keyword1).intersection(set(keyword2))
        total_chars = set(keyword1).union(set(keyword2))
        if len(common_chars) / len(total_chars) > threshold:
            return True
        
        return False
    
    # 기존 키워드와 유사한 것 제외, 새 키워드들 사이에서도 중복 제거
    unique_keywords = []
    for new_kw in filtered_keywords:
        # 기존 키워드와 유사한지 체크
        if any(is_similar(new_kw, existing_kw, similarity_threshold) for existing_kw in existing_keywords):
            continue
        
        # 이미 선택된 새 키워드와 유사한지 체크
        if any(is_similar(new_kw, selected_kw, similarity_threshold) for selected_kw in unique_keywords):
            continue
        
        unique_keywords.append(new_kw)
    
    logger.info(f"유사도 필터링 후 남은 키워드: {len(unique_keywords)}개")
    
    # 최대 키워드 수 제한
    selected_keywords = unique_keywords[:max_new_keywords]
    
    # 키워드 파일에 추가
    if selected_keywords:
        with open(todo_path, 'a', encoding='utf-8') as f:
            for keyword in selected_keywords:
                f.write(f"{keyword}\n")
        
        logger.info(f"총 {len(selected_keywords)}개 키워드가 추가되었습니다.")
    else:
        logger.info("추가할 새 키워드가 없습니다.")
    
    return len(selected_keywords)