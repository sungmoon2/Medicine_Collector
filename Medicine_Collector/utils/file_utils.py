#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
파일 처리 유틸리티
"""

import os
import re
import json
import csv
import glob
import logging
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)

def sanitize_filename(filename):
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

def generate_medicine_id(medicine_data):
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

def is_duplicate_medicine(medicine_data, output_dir):
    """
    중복 의약품 검사
    
    Args:
        medicine_data (dict): 의약품 데이터
        output_dir (str): 출력 디렉토리
    
    Returns:
        bool: 중복 여부
    """
    # 고유 식별자 기준 중복 체크 (예: URL, ID)
    existing_ids_path = os.path.join(output_dir, "processed_medicine_ids.txt")
    
    # 의약품 고유 ID 생성
    medicine_id = generate_medicine_id(medicine_data)
    
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

def save_medicine_data(medicine_data, json_dir, output_dir):
    """
    의약품 데이터 저장 (개선된 버전)
    
    Args:
        medicine_data (dict): 저장할 의약품 데이터
        json_dir (str): JSON 저장 디렉토리
        output_dir (str): 출력 디렉토리
    
    Returns:
        tuple: (성공 여부, 파일 경로)
    """
    try:
        # 중복 검사
        if is_duplicate_medicine(medicine_data, output_dir):
            logger.info(f"중복 의약품 스킵: {medicine_data.get('korean_name', '이름 없음')}")
            return False, None
        
        if not medicine_data or "korean_name" not in medicine_data:
            return False, None
        
        # 1. 고유 ID 생성
        medicine_id = generate_medicine_id(medicine_data)
        medicine_data["id"] = medicine_id
        
        # 2. JSON 파일로 저장
        json_filename = f"{medicine_id}_{sanitize_filename(medicine_data['korean_name'])}.json"
        json_path = os.path.join(json_dir, json_filename)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(medicine_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"의약품 데이터 저장 완료: {medicine_data['korean_name']} (ID: {medicine_id})")
        return True, json_path
            
    except Exception as e:
        # 상세한 오류 로깅
        logger.error(f"데이터 저장 중 오류: {e}")
        logger.error(f"오류 발생 데이터: {json.dumps(medicine_data, ensure_ascii=False, indent=2)}")
        
        # 오류 데이터 별도 저장
        error_log_path = os.path.join(output_dir, "error_logs", f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)
        
        with open(error_log_path, 'w', encoding='utf-8') as f:
            json.dump({
                "error": str(e),
                "medicine_data": medicine_data
            }, f, ensure_ascii=False, indent=2)
        
        return False, None

def export_to_csv(stats, json_dir, output_path, batch_size=500):
    """
    수집된 의약품 데이터를 CSV로 내보내기 (성능 개선 버전)
    
    Args:
        stats (dict): 수집 통계
        json_dir (str): JSON 파일 디렉토리
        output_path (str): 출력 파일 경로
        batch_size (int): 한 번에 처리할 JSON 파일 수 (메모리 효율성)
            
    Returns:
        str: CSV 파일 경로
    """
    if not stats['medicine_items']:
        logger.warning("내보낼 의약품 데이터가 없습니다.")
        return None
    
    try:
        # 모든 JSON 파일 목록 가져오기
        json_files = []
        if stats['medicine_items']:
            json_files = [item['path'] for item in stats['medicine_items'] if os.path.exists(item['path'])]
        else:
            # stats에 정보가 없는 경우 디렉토리에서 직접 검색
            json_files = glob.glob(os.path.join(json_dir, "*.json"))
        
        if not json_files:
            logger.warning("내보낼 JSON 파일이 없습니다.")
            return None
        
        total_files = len(json_files)
        logger.info(f"CSV 내보내기: 총 {total_files}개 파일 처리 중...")
        
        # 모든 키 수집 (첫 번째 배치만 샘플링)
        all_keys = set()
        sample_size = min(100, total_files)  # 최대 100개 파일만 샘플링
        
        for i in range(sample_size):
            try:
                with open(json_files[i], 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_keys.update(data.keys())
            except Exception as e:
                logger.warning(f"키 수집 중 오류 (파일: {json_files[i]}): {e}")
        
        # MySQL 친화적인 열 순서 설정
        # 중요 필드가 먼저 오도록 정렬
        ordered_keys = [
            'id', 'korean_name', 'english_name', 'category', 'type', 'company',
            'classification', 'medicine_type', 'insurance_code', 'approval_number',
            'components', 'components_amount', 'efficacy', 'dosage', 'precautions', 
            'side_effects', 'interactions', 'storage', 'expiration',
            'appearance', 'shape', 'color', 'size', 'identification', 'division_line',
            'image_url', 'url', 'extracted_time', 'collection_time'
        ]
        
        # 나머지 키는 알파벳 순으로 추가
        remaining_keys = sorted(list(all_keys - set(ordered_keys)))
        ordered_keys.extend(remaining_keys)
        
        # 배치 처리로 CSV 파일 작성
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_keys)
            writer.writeheader()
            
            # 배치 단위로 처리
            for i in range(0, total_files, batch_size):
                batch_files = json_files[i:i+batch_size]
                batch_data = []
                
                for json_file in batch_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as jf:
                            medicine_data = json.load(jf)
                            batch_data.append(medicine_data)
                    except Exception as e:
                        logger.warning(f"JSON 파일 로드 중 오류 (파일: {json_file}): {e}")
                
                # 불필요한 큰 텍스트 필드 요약 (메모리 효율성)
                for data in batch_data:
                    for field in ['components', 'efficacy', 'precautions', 'dosage']:
                        if field in data and isinstance(data[field], str) and len(data[field]) > 1000:
                            # 길이가 1000자를 초과하는 경우 요약
                            data[field] = data[field][:997] + '...'
                
                # 배치 데이터 쓰기
                for data in batch_data:
                    # 누락된 필드는 빈 문자열로 처리
                    row_data = {k: data.get(k, '') for k in ordered_keys}
                    writer.writerow(row_data)
                
                # 메모리 확보를 위해 배치 데이터 해제
                batch_data = None
                
                # 진행 상황 보고
                progress = min(100, round((i + len(batch_files)) / total_files * 100))
                logger.info(f"CSV 내보내기 진행률: {progress}% ({i + len(batch_files)}/{total_files})")
        
        logger.info(f"CSV 내보내기 완료: {output_path}")
        return output_path
            
    except Exception as e:
        logger.error(f"CSV 내보내기 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None