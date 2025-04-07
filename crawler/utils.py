#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
파일 관리, docId 추출, 로깅 등의 유틸리티 함수
"""

import os
import re
import json
import logging
import random
from pathlib import Path
from datetime import datetime

# 로깅 설정
def setup_logger(log_file):
    """로깅 설정"""
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger('medicine_crawler')
    logger.setLevel(logging.INFO)
    
    # 파일 핸들러
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포맷 설정
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def extract_docids_from_json_files(json_dir):
    """
    JSON 파일명에서 docId를 추출
    
    Args:
        json_dir (str): JSON 파일 디렉토리
        
    Returns:
        set: 추출된 docId 집합
    """
    docids = set()
    
    # 디렉토리가 존재하는지 확인
    if not os.path.exists(json_dir):
        return docids
    
    # 파일명 패턴: M{docId}_{약품명}.json
    pattern = re.compile(r'M(\d+)_.*\.json')
    
    # 모든 JSON 파일 순회
    for filename in os.listdir(json_dir):
        if filename.endswith('.json'):
            match = pattern.match(filename)
            if match:
                docid = match.group(1)
                docids.add(docid)
    
    return docids

def load_processed_docids(processed_file):
    """
    처리된 docId 목록 로드
    
    Args:
        processed_file (str): 처리된 docId 목록 파일 경로
        
    Returns:
        set: 처리된 docId 집합
    """
    docids = set()
    
    if os.path.exists(processed_file):
        with open(processed_file, 'r', encoding='utf-8') as f:
            for line in f:
                docid = line.strip()
                if docid.startswith('M'):
                    docid = docid[1:]  # 'M' 제거
                if docid and docid.isdigit():
                    docids.add(docid)
    
    return docids

def save_processed_docid(docid, processed_file):
    """
    처리된 docId를 파일에 추가
    
    Args:
        docid (str): 추가할 docId
        processed_file (str): 처리된 docId 목록 파일 경로
    """
    # 디렉토리 생성
    os.makedirs(os.path.dirname(processed_file), exist_ok=True)
    
    # 'M' 접두사 제거
    if docid.startswith('M'):
        docid = docid[1:]
    
    with open(processed_file, 'a', encoding='utf-8') as f:
        f.write(f"{docid}\n")

def calculate_missing_docids(start_docid, end_docid, processed_docids):
    """
    누락된 docId 계산
    
    Args:
        start_docid (int): 시작 docId
        end_docid (int): 종료 docId
        processed_docids (set): 처리된 docId 집합
        
    Returns:
        list: 누락된 docId 목록
    """
    all_docids = set(str(i) for i in range(start_docid, end_docid + 1))
    missing_docids = list(all_docids - processed_docids)
    
    # 정수로 변환하여 정렬 후 다시 문자열로 변환
    missing_docids = [str(docid) for docid in sorted(int(docid) for docid in missing_docids)]
    
    return missing_docids

def save_missing_docids(missing_docids, missing_file):
    """
    누락된 docId 목록 저장
    
    Args:
        missing_docids (list): 누락된 docId 목록
        missing_file (str): 저장할 파일 경로
    """
    # 디렉토리 생성
    os.makedirs(os.path.dirname(missing_file), exist_ok=True)
    
    with open(missing_file, 'w', encoding='utf-8') as f:
        for docid in missing_docids:
            f.write(f"{docid}\n")

def sanitize_filename(filename):
    """
    안전한 파일명 생성
    
    Args:
        filename (str): 원본 파일명
        
    Returns:
        str: 안전한 파일명
    """
    # 파일명에 사용할 수 없는 문자 제거
    invalid_chars = r'[<>:"/\\|?*]'
    safe_filename = re.sub(invalid_chars, '_', filename)
    
    # 길이 제한
    if len(safe_filename) > 100:
        safe_filename = safe_filename[:100]
    
    return safe_filename

def save_medicine_data(medicine_data, json_dir):
    """
    의약품 데이터를 JSON 파일로 저장
    
    Args:
        medicine_data (dict): 의약품 데이터
        json_dir (str): JSON 저장 디렉토리
        
    Returns:
        str: 저장된 파일 경로 또는 None (실패 시)
    """
    try:
        # 필수 데이터 확인
        if not medicine_data or 'id' not in medicine_data or not medicine_data['id']:
            return None
        
        # 한글 이름 확인
        med_name = medicine_data.get('korean_name', medicine_data.get('title', 'unknown'))
        if not med_name or med_name == 'unknown':
            for field in ['name', 'title', 'korean_name', 'medicine_name']:
                if field in medicine_data and medicine_data[field]:
                    med_name = medicine_data[field]
                    break
        
        # 디렉토리 생성
        os.makedirs(json_dir, exist_ok=True)
        
        # 파일명 생성
        filename = f"{medicine_data['id']}_{sanitize_filename(med_name)}.json"
        file_path = os.path.join(json_dir, filename)
        
        # JSON 파일 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(medicine_data, f, ensure_ascii=False, indent=2)
        
        return file_path
    
    except Exception as e:
        logging.getLogger('medicine_crawler').error(f"데이터 저장 중 오류: {str(e)}")
        return None

def shuffle_docids(docids):
    """
    docId 목록을 무작위로 섞기
    
    Args:
        docids (list): docId 목록
        
    Returns:
        list: 섞인 docId 목록
    """
    shuffled = docids.copy()
    random.shuffle(shuffled)
    return shuffled

def load_missing_docids(missing_file):
    """
    누락된 docId 목록 로드
    
    Args:
        missing_file (str): 누락된 docId 목록 파일 경로
        
    Returns:
        list: 누락된 docId 목록
    """
    docids = []
    
    if os.path.exists(missing_file):
        with open(missing_file, 'r', encoding='utf-8') as f:
            for line in f:
                docid = line.strip()
                if docid and docid.isdigit():
                    docids.append(docid)
    
    return docids

def save_invalid_docid(docid, invalid_file):
    """
    유효하지 않은 docId 저장
    
    Args:
        docid (str): 유효하지 않은 docId
        invalid_file (str): 저장할 파일 경로
    """
    # 디렉토리 생성
    os.makedirs(os.path.dirname(invalid_file), exist_ok=True)
    
    with open(invalid_file, 'a', encoding='utf-8') as f:
        f.write(f"{docid}\n")