#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 지식백과 의약품사전 docId 기반 데이터 누락 보완 크롤러

이 스크립트는 네이버 지식백과의 의약품사전에서 docId 기반으로
누락된 의약품 데이터를 크롤링하여 JSON 형태로 저장합니다.

Usage:
    python main.py [--start START_DOCID] [--end END_DOCID] [--limit LIMIT]
"""

import os
import sys
import time
import argparse
import random
from pathlib import Path
from tqdm import tqdm

# 로컬 모듈
from utils import (
    setup_logger, extract_docids_from_json_files, load_processed_docids,
    save_processed_docid, calculate_missing_docids, save_missing_docids,
    save_medicine_data, shuffle_docids, load_missing_docids
)
from fetcher import MedicineFetcher

def parse_arguments():
    """
    명령행 인자 파싱
    
    Returns:
        argparse.Namespace: 파싱된 인자
    """
    parser = argparse.ArgumentParser(description='네이버 지식백과 의약품사전 docId 기반 데이터 누락 보완 크롤러')
    parser.add_argument('--start', type=int, default=2120920, help='시작 docId (기본값: 2120920)')
    parser.add_argument('--end', type=int, default=6730030, help='종료 docId (기본값: 6730030)')
    parser.add_argument('--limit', type=int, help='최대 크롤링 항목 수 (기본값: 무제한)')
    parser.add_argument('--delay-min', type=float, default=1.0, help='최소 지연 시간(초) (기본값: 1.0)')
    parser.add_argument('--delay-max', type=float, default=3.0, help='최대 지연 시간(초) (기본값: 3.0)')
    parser.add_argument('--use-missing', action='store_true', help='이미 생성된 missing_docids.txt 사용 (기본값: False)')
    parser.add_argument('--resume', action='store_true', help='중단된 지점부터 이어서 크롤링 (기본값: False)')
    
    return parser.parse_args()

def init_directories():
    """
    필요한 디렉토리 초기화
    
    Returns:
        dict: 디렉토리 경로
    """
    # 기본 경로
    base_dir = 'collected_data'
    
    # 필요한 디렉토리 생성
    directories = {
        'base': base_dir,
        'json': os.path.join(base_dir, 'json'),
        'logs': os.path.join(base_dir, 'logs')
    }
    
    for path in directories.values():
        os.makedirs(path, exist_ok=True)
    
    return directories

def main():
    """
    메인 함수
    """
    # 인자 파싱
    args = parse_arguments()
    
    # 디렉토리 초기화
    directories = init_directories()
    
    # 로거 설정
    logger = setup_logger(os.path.join(directories['base'], 'crawl_log.txt'))
    logger.info(f"네이버 의약품사전 크롤러 시작 - 버전 1.0.0")
    
    # 파일 경로 설정
    processed_file = os.path.join(directories['base'], 'Processed_medicine_ids.txt')
    missing_file = os.path.join(directories['base'], 'missing_docids.txt')
    invalid_file = os.path.join(directories['base'], 'invalid_docids.txt')  # 유효하지 않은 docId 저장 파일

    # 잘못된 docId 목록 로드
    invalid_docids = set()
    if os.path.exists(invalid_file):
        with open(invalid_file, 'r', encoding='utf-8') as f:
            invalid_docids = {line.strip() for line in f if line.strip()}
        logger.info(f"유효하지 않은 docId 목록 로드: {len(invalid_docids)}개")
    
    # 1. 기존 JSON 파일에서 docId 추출
    logger.info(f"JSON 디렉토리에서 기존 docId 추출 중...")
    processed_docids = extract_docids_from_json_files(directories['json'])
    logger.info(f"JSON 파일에서 추출된 docId: {len(processed_docids)}개")
    
    # 2. 처리된 docId 목록 로드
    additional_processed = load_processed_docids(processed_file)
    logger.info(f"처리된 docId 목록에서 추가 로드: {len(additional_processed)}개")
    
    # 3. 모든 처리된 docId 병합
    processed_docids.update(additional_processed)
    logger.info(f"총 처리된 docId: {len(processed_docids)}개")
    
    # 4. 누락된 docId 계산 또는 로드
    missing_docids = []
    if args.use_missing and os.path.exists(missing_file):
        logger.info("기존 missing_docids.txt 파일 사용")
        missing_docids = load_missing_docids(missing_file)
        logger.info(f"누락된 docId 로드 완료: {len(missing_docids)}개")
    else:
        logger.info(f"누락된 docId 계산 중 (범위: {args.start} ~ {args.end})...")
        missing_docids = calculate_missing_docids(args.start, args.end, processed_docids)
        logger.info(f"누락된 docId 계산 완료: {len(missing_docids)}개")
        
        # missing_docids.txt 파일 저장
        save_missing_docids(missing_docids, missing_file)
        logger.info(f"누락된 docId 목록 저장 완료: {missing_file}")
    
    # 이미 유효하지 않은 것으로 확인된 docId 제외
    missing_docids = [docid for docid in missing_docids if docid not in invalid_docids]
    logger.info(f"유효하지 않은 docId 제외 후 처리할 docId: {len(missing_docids)}개")
    
    # 5. 이어서 크롤링할 경우 상태 파일 확인
    resume_file = os.path.join(directories['base'], 'crawl_resume.txt')
    if args.resume and os.path.exists(resume_file):
        with open(resume_file, 'r', encoding='utf-8') as f:
            last_index_str = f.read().strip()
            if last_index_str and last_index_str.isdigit():
                last_index = int(last_index_str)
                logger.info(f"이어서 크롤링: 인덱스 {last_index}부터 시작 (총 {len(missing_docids)}개 중)")
                # 이미 처리한 항목 건너뛰기
                if last_index < len(missing_docids):
                    missing_docids = missing_docids[last_index:]
    
    # 6. docId 무작위 섞기
    logger.info("Bot 차단 방지를 위해 docId 무작위 섞는 중...")
    shuffled_docids = shuffle_docids(missing_docids)
    logger.info("docId 무작위 섞기 완료")
    
    # 7. 크롤링 제한 설정
    if args.limit and args.limit > 0:
        shuffled_docids = shuffled_docids[:args.limit]
        logger.info(f"크롤링 제한: {args.limit}개")
    
    # 8. MedicineFetcher 초기화
    fetcher = MedicineFetcher(
        max_retries=3,
        delay_range=(args.delay_min, args.delay_max)
    )
    
    # 9. 크롤링 시작
    logger.info(f"의약품 데이터 크롤링 시작: {len(shuffled_docids)}개 항목")
    success_count = 0
    invalid_count = 0
    
    try:
        for idx, docid in enumerate(tqdm(shuffled_docids, desc="크롤링 진행")):
            try:
                # 이미 잘못된 docId로 확인된 경우 건너뛰기
                if docid in invalid_docids:
                    logger.info(f"[{idx+1}/{len(shuffled_docids)}] docId {docid}은 이미 잘못된 docId로 확인됨, 건너뜀")
                    continue
                
                # 크롤링 시도
                medicine_data = fetcher.fetch_medicine_data(docid)
                
                if medicine_data:
                    # 데이터 저장
                    file_path = save_medicine_data(medicine_data, directories['json'])
                    
                    if file_path:
                        # 처리된 docId 저장
                        save_processed_docid(docid, processed_file)
                        success_count += 1
                        logger.info(f"[{idx+1}/{len(shuffled_docids)}] docId {docid} 저장 성공: {file_path}")
                        
                        # missing_docids.txt 업데이트
                        update_missing_docids(missing_file, docid)
                    else:
                        logger.error(f"[{idx+1}/{len(shuffled_docids)}] docId {docid} 데이터 저장 실패")
                else:
                    # 잘못된 docId로 표시
                    invalid_docids.add(docid)
                    with open(invalid_file, 'a', encoding='utf-8') as f:
                        f.write(f"{docid}\n")
                    invalid_count += 1
                    logger.warning(f"[{idx+1}/{len(shuffled_docids)}] docId {docid}는 유효하지 않음, invalid_docids.txt에 추가")
                    
                    # missing_docids.txt 업데이트
                    update_missing_docids(missing_file, docid)
                
                # 진행 상태 저장 (10개 단위)
                if idx % 10 == 0:
                    with open(resume_file, 'w', encoding='utf-8') as f:
                        f.write(str(idx))
                
            except Exception as e:
                logger.error(f"docId {docid} 처리 중 오류: {str(e)}")
            
            # 랜덤 지연
            time.sleep(random.uniform(args.delay_min, args.delay_max))
    
    except KeyboardInterrupt:
        # Ctrl+C로 중단된 경우
        logger.info("사용자에 의해 크롤링이 중단되었습니다.")
        # 현재 진행 상태 저장
        with open(resume_file, 'w', encoding='utf-8') as f:
            f.write(str(idx))
        logger.info(f"진행 상태 저장됨: {idx}/{len(shuffled_docids)}")
    
    except Exception as e:
        logger.error(f"크롤링 중 예외 발생: {str(e)}")
    
    finally:
        # 결과 요약
        total_processed = success_count + invalid_count
        logger.info(f"크롤링 완료 - 총 처리: {total_processed}/{len(shuffled_docids)}개")
        logger.info(f"성공: {success_count}개 ({success_count/len(shuffled_docids)*100:.1f}%)")
        logger.info(f"유효하지 않음: {invalid_count}개 ({invalid_count/len(shuffled_docids)*100:.1f}%)")
        
        print(f"\n크롤링 완료:")
        print(f"- 총 처리: {total_processed}/{len(shuffled_docids)}개 ({total_processed/len(shuffled_docids)*100:.1f}%)")
        print(f"- 성공: {success_count}개 ({success_count/len(shuffled_docids)*100:.1f}%)")
        print(f"- 유효하지 않음: {invalid_count}개 ({invalid_count/len(shuffled_docids)*100:.1f}%)")
        print(f"로그 파일: {os.path.join(directories['base'], 'crawl_log.txt')}")
        
if __name__ == "__main__":
    main()