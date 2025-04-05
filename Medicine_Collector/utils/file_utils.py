#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
파일 처리 유틸리티 - 개선된 버전
- 일관된 데이터 구조 보장
- 누락된 필드 "정보 없음"으로 표시
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

# 의약품 데이터 표준 필드 정의
MEDICINE_FIELDS = {
    # 기본 정보
    'id': '',                    # 고유 ID
    'title': '',                 # 제목
    'link': '',                  # 원본 링크
    'description': '',           # 설명
    'url': '',                   # URL
    'category': '',              # 카테고리
    
    # 의약품 기본 정보
    'korean_name': '',           # 한글 이름
    'english_name': '',          # 영문 이름
    'classification': '',        # 분류
    'medicine_type': '',         # 의약품 타입
    'company': '',               # 제조/수입사
    'insurance_code': '',        # 보험 코드
    'approval_number': '',       # 품목 허가번호
    
    # 성분 및 효능
    'components': '',            # 성분
    'components_amount': '',     # 성분 함량
    'efficacy': '',              # 효능효과
    'dosage': '',                # 용법용량
    
    # 외형 정보
    'appearance': '',            # 외형
    'shape': '',                 # 모양
    'shape_type': '',            # 제형
    'color': '',                 # 색상
    'size': '',                  # 크기
    'identification': '',        # 식별 표시
    'division_line': '',         # 분할선
    
    # 보관 및 주의사항
    'storage_conditions': '',    # 보관 조건
    'expiration': '',            # 유효기간
    'precautions': '',           # 주의사항
    'side_effects': '',          # 부작용
    'interactions': '',          # 상호작용
    
    # 이미지 정보
    'image_url': '',             # 이미지 URL
    'image_alt': '',             # 이미지 대체 텍스트
    'image_quality': '',         # 이미지 품질
    'image_width': '',           # 이미지 너비
    'image_height': '',          # 이미지 높이
    'original_width': '',        # 원본 이미지 너비
    'original_height': '',       # 원본 이미지 높이
    
    # 메타데이터
    'search_keyword': '',        # 검색 키워드
    'origin_query': '',          # 원본 쿼리
    'crawled_at': '',            # 크롤링 시간
    'extracted_time': '',        # 데이터 추출 시간
    'collection_time': ''        # 수집 시간
}

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
    url = medicine_data.get('url', '') or medicine_data.get('link', '')
    doc_id_match = re.search(r'docId=([^&]+)', url)
    
    if doc_id_match:
        return f"M{doc_id_match.group(1)}"
    
    # URL이 없으면 이름 + 회사 기반으로 ID 생성
    name = medicine_data.get('korean_name', '') or medicine_data.get('title', '')
    company = medicine_data.get('company', '')
    
    id_base = f"{name}_{company}_{datetime.now().strftime('%Y%m%d')}"
    return f"MC{abs(hash(id_base)) % 10000000:07d}"

def standardize_medicine_data(medicine_data):
    """
    의약품 데이터 표준화 - 일관된 구조와 "정보 없음" 기본값
    
    Args:
        medicine_data (dict): 원본 의약품 데이터
        
    Returns:
        dict: 표준화된 의약품 데이터
    """
    standardized_data = MEDICINE_FIELDS.copy()
    
    # 기존 데이터 복사
    for key, value in medicine_data.items():
        if key in standardized_data:
            # 내용이 있으면 그대로 사용
            standardized_data[key] = value
    
    # 누락된 필드 "정보 없음"으로 채우기
    for key in standardized_data:
        if not standardized_data[key] and key in medicine_data:
            standardized_data[key] = "정보 없음"
        elif not standardized_data[key] and key not in medicine_data:
            standardized_data[key] = "정보 없음"
    
    # 필수 필드 확인
    if not standardized_data['id']:
        standardized_data['id'] = generate_medicine_id(medicine_data)
    
    # 날짜 정보 채우기
    current_time = datetime.now().isoformat()
    if not standardized_data['extracted_time']:
        standardized_data['extracted_time'] = current_time
    if not standardized_data['collection_time']:
        standardized_data['collection_time'] = current_time
    
    return standardized_data

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
    medicine_id = medicine_data.get('id') or generate_medicine_id(medicine_data)
    
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
    - 일관된 데이터 구조
    - 누락된 필드 "정보 없음"으로 표시
    
    Args:
        medicine_data (dict): 저장할 의약품 데이터
        json_dir (str): JSON 저장 디렉토리
        output_dir (str): 출력 디렉토리
    
    Returns:
        tuple: (성공 여부, 파일 경로)
    """
    try:
        if not medicine_data:
            logger.warning("저장할 의약품 데이터가 없습니다.")
            return False, None
        
        # 0. 데이터 표준화
        medicine_data = standardize_medicine_data(medicine_data)
        
        # 1. 중복 검사
        if is_duplicate_medicine(medicine_data, output_dir):
            logger.info(f"중복 의약품 스킵: {medicine_data.get('korean_name', '이름 없음')} (ID: {medicine_data.get('id')})")
            return False, None
        
        # 2. 고유 ID 확인
        medicine_id = medicine_data.get('id') or generate_medicine_id(medicine_data)
        medicine_data["id"] = medicine_id
        
        # 3. JSON 파일로 저장
        medicine_name = medicine_data.get('korean_name') or medicine_data.get('title', '이름없음')
        json_filename = f"{medicine_id}_{sanitize_filename(medicine_name)}.json"
        json_path = os.path.join(json_dir, json_filename)
        
        # 디렉토리 생성
        os.makedirs(json_dir, exist_ok=True)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(medicine_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"의약품 데이터 저장 완료: {medicine_name} (ID: {medicine_id})")
        return True, json_path
            
    except Exception as e:
        # 상세한 오류 로깅
        logger.error(f"데이터 저장 중 오류: {e}")
        
        # 오류 데이터 별도 저장
        error_log_dir = os.path.join(output_dir, "error_logs")
        os.makedirs(error_log_dir, exist_ok=True)
        
        error_log_path = os.path.join(error_log_dir, f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(error_log_path, 'w', encoding='utf-8') as f:
            json.dump({
                "error": str(e),
                "medicine_data": medicine_data
            }, f, ensure_ascii=False, indent=2)
        
        return False, None

def load_and_standardize_json(json_path):
    """
    저장된 JSON 파일을 로드하고 표준화
    
    Args:
        json_path (str): JSON 파일 경로
        
    Returns:
        dict: 표준화된 의약품 데이터
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            medicine_data = json.load(f)
        
        # 데이터 표준화
        standardized_data = standardize_medicine_data(medicine_data)
        
        # 파일 업데이트 (표준화된 형식으로)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(standardized_data, f, ensure_ascii=False, indent=2)
        
        return standardized_data
        
    except Exception as e:
        logger.error(f"JSON 파일 로드 중 오류 ({json_path}): {e}")
        return None

def standardize_all_json_files(json_dir):
    """
    디렉토리의 모든 JSON 파일을 표준화
    
    Args:
        json_dir (str): JSON 디렉토리
        
    Returns:
        tuple: (성공 개수, 실패 개수)
    """
    json_files = glob.glob(os.path.join(json_dir, "*.json"))
    success_count = 0
    error_count = 0
    
    logger.info(f"총 {len(json_files)}개 JSON 파일 표준화 시작...")
    
    for idx, json_path in enumerate(json_files, 1):
        try:
            if load_and_standardize_json(json_path):
                success_count += 1
            else:
                error_count += 1
                
            # 진행 상황 보고
            if idx % 100 == 0 or idx == len(json_files):
                logger.info(f"JSON 표준화 진행률: {idx}/{len(json_files)} ({idx/len(json_files)*100:.1f}%)")
                
        except Exception as e:
            logger.error(f"파일 표준화 중 오류 ({json_path}): {e}")
            error_count += 1
    
    logger.info(f"JSON 파일 표준화 완료: 성공 {success_count}개, 실패 {error_count}개")
    return success_count, error_count

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
    json_files = []
    
    # stats에서 파일 목록 가져오기
    if isinstance(stats, dict) and 'medicine_items' in stats and stats['medicine_items']:
        json_files = [item['path'] for item in stats['medicine_items'] if os.path.exists(item['path'])]
    
    # stats에 정보가 없으면 디렉토리에서 직접 검색
    if not json_files:
        json_files = glob.glob(os.path.join(json_dir, "*.json"))
    
    if not json_files:
        logger.warning("내보낼 JSON 파일이 없습니다.")
        return None
    
    try:
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
        
        # CSV 디렉토리 경로 확인 및 생성
        csv_dir = os.path.join(os.path.dirname(os.path.dirname(output_path)), "csv")
        os.makedirs(csv_dir, exist_ok=True)
        
        # 출력 경로 수정 - csv 디렉토리에 저장
        output_path = os.path.join(csv_dir, os.path.basename(output_path))
        
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
                        medicine_data = load_and_standardize_json(json_file)
                        if medicine_data:
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
                    # 누락된 필드는 "정보 없음"으로 처리
                    row_data = {k: (data.get(k, "정보 없음") if data.get(k) else "정보 없음") for k in ordered_keys}
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
    
# 기존 JSON 파일 표준화 실행 함수
def run_standardization(json_dir):
    """
    모든 JSON 파일을 표준화하는 함수
    
    Args:
        json_dir (str): JSON 디렉토리
    """
    logger.info("기존 JSON 파일 표준화 시작...")
    success, failed = standardize_all_json_files(json_dir)
    logger.info(f"JSON 파일 표준화 완료: 성공 {success}개, 실패 {failed}개")

# 스크립트 직접 실행 시
if __name__ == "__main__":
    import argparse
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 인자 파싱
    parser = argparse.ArgumentParser(description='의약품 JSON 파일 표준화')
    parser.add_argument('--json-dir', type=str, required=True, help='JSON 파일 디렉토리')
    parser.add_argument('--csv-export', type=str, help='CSV 내보내기 파일 경로 (선택)')
    
    args = parser.parse_args()
    
    # JSON 파일 표준화
    run_standardization(args.json_dir)
    
    # CSV 내보내기 (요청 시)
    if args.csv_export:
        export_to_csv({}, args.json_dir, args.csv_export)