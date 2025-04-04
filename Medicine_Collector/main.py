#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 검색 API를 활용한 의약품 정보 수집기

이 스크립트는 네이버 검색 API를 사용하여 의약품 정보를 검색하고,
검색 결과에서 의약품사전 페이지를 찾아 데이터를 추출합니다.
추출된 데이터는 JSON 형태로 저장되며, 수집 결과는 HTML 파일로 생성됩니다.

* 자동 키워드 생성 기능 추가 - 모든 키워드 처리 후 새 키워드를 자동으로 생성하고 계속 수집
"""

import os
import sys
import time
import argparse
import logging
import signal
import threading
from datetime import datetime
from dotenv import load_dotenv

from collector import MedicineCollector
from utils.safety import setup_signal_handlers
from utils.keyword_manager import (
    generate_medicine_keywords, load_keywords, clean_keyword_files,
    generate_extensive_initial_keywords, extract_keywords_from_existing_json,
    ensure_keywords_available
)

# 전역 종료 플래그
shutdown_requested = False
shutdown_event = threading.Event()

# .env 파일 로드
load_dotenv()

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

def print_banner():
    """프로그램 시작 배너 출력"""
    banner = r"""
    ======================================================================
    네이버 검색 API 기반 의약품 정보 수집기 v1.1
    - 자동 키워드 생성 기능 추가
    ======================================================================
    """
    print(banner)

# 메인 함수
def main():
    """
    메인 함수 (자동 키워드 생성 및 연속 수집 기능 추가)
    """
    try:
        # 배너 출력
        print_banner()
        
        # 안전한 종료 메커니즘 설정
        setup_signal_handlers()
        
        # 명령행 인자 파싱
        parser = argparse.ArgumentParser(description='네이버 검색 API를 활용한 의약품 정보 수집기')
        parser.add_argument('--client-id', help='네이버 API 클라이언트 ID')
        parser.add_argument('--client-secret', help='네이버 API 클라이언트 시크릿')
        parser.add_argument('--output-dir', default='collected_data', help='데이터 저장 디렉토리')
        parser.add_argument('--max-items', type=int, help='최대 수집 항목 수')
        parser.add_argument('--keywords', help='검색할 키워드 (쉼표로 구분)')
        parser.add_argument('--auto-continue', action='store_true', default=True,
                           help='모든 키워드 처리 후 새 키워드 생성하여 계속 수집 (기본값: True)')
        parser.add_argument('--max-iterations', type=int, default=0,
                           help='최대 수집 반복 횟수 (0=무제한)')
        parser.add_argument('--max-new-keywords', type=int, default=50,
                           help='생성할 최대 새 키워드 수 (기본값: 50)')
        parser.add_argument('--similarity-threshold', type=float, default=0.6,
                           help='키워드 유사도 임계값 (기본값: 0.6)')
        args = parser.parse_args()
        
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
        
        # 자동 수집 처리 루프
        continue_collection = True
        iteration = 1
        total_processed = 0
        total_success = 0
        total_collection_time = 0
        
        while continue_collection and not shutdown_requested:
            logger.info(f"수집 반복 #{iteration} 시작")
            
            # 키워드 로드 또는 생성
            if args.keywords and iteration == 1:
                # 첫 반복에서만 명령행 키워드 사용
                keywords = [k.strip() for k in args.keywords.split(',')]
                logger.info(f"명령행에서 {len(keywords)}개 키워드를 로드했습니다.")
            else:
                # 키워드 로드
                keywords = collector.load_keywords()
                logger.info(f"파일에서 {len(keywords)}개 키워드를 로드했습니다.")
            
            # 키워드가 없으면 새로 생성
            if not keywords:
                logger.info("처리할 키워드가 없습니다. 새 키워드 생성 시도...")
                new_count = collector.generate_medicine_keywords(
                    max_new_keywords=args.max_new_keywords,
                    similarity_threshold=args.similarity_threshold
                )
                logger.info(f"새 키워드 생성 완료: {new_count}개 추가됨")
                
                # 새 키워드 로드
                keywords = collector.load_keywords()
                logger.info(f"처리할 키워드 (재로드): {len(keywords)}개")
                
                # 그래도 키워드가 없으면 종료
                if not keywords:
                    logger.info("추가할 새 키워드가 없습니다. 수집 종료.")
                    break
            
            # 결과 추적
            iteration_start_time = time.time()
            
            # 수집 실행
            stats = collector.collect_medicines(keywords, args.max_items)
            
            # 실행 시간 계산
            elapsed_time = time.time() - iteration_start_time
            total_collection_time += elapsed_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # 반복 요약 로그
            logger.info("-" * 60)
            logger.info(f"수집 반복 #{iteration} 완료")
            logger.info(f"검색 횟수: {stats['total_searches']}회")
            logger.info(f"발견한 항목: {stats['total_found']}개")
            logger.info(f"저장된 항목: {stats['total_saved']}개")
            logger.info(f"실패한 항목: {stats['failed_items']}개")
            logger.info(f"실행 시간: {int(hours)}시간 {int(minutes)}분 {int(seconds)}초")
            logger.info("-" * 60)
            
            # 통계 업데이트
            total_processed += stats['total_searches']
            total_success += stats['total_saved']
            
            # 종료 조건 확인
            if shutdown_requested:
                logger.info("종료 요청으로 수집을 중단합니다.")
                break
                
            if args.max_iterations > 0 and iteration >= args.max_iterations:
                logger.info(f"최대 반복 횟수({args.max_iterations}회)에 도달했습니다. 수집 종료.")
                break
            
            # 자동 계속 실행 여부 확인
            if args.auto_continue:
                # 새 키워드 생성 시도
                logger.info("새 키워드 생성 시도...")
                new_count = collector.generate_medicine_keywords(
                    max_new_keywords=args.max_new_keywords,
                    similarity_threshold=args.similarity_threshold
                )
                logger.info(f"새 키워드 생성 완료: {new_count}개 추가됨")
                
                # 새 키워드가 없으면 종료
                if new_count == 0:
                    logger.info("추가할 새 키워드가 없습니다. 수집 종료.")
                    continue_collection = False
                else:
                    # 새 키워드가 있으면 계속 실행
                    iteration += 1
                    # 잠시 대기 (API 제한 대응)
                    logger.info("다음 반복 전 잠시 대기 중...")
                    time.sleep(5)
            else:
                continue_collection = False
        
        # 최종 요약 로그
        logger.info("=" * 80)
        logger.info(f"의약품 정보 수집 완료")
        logger.info(f"총 수집 반복: {iteration}회")
        logger.info(f"총 검색 횟수: {total_processed}회")
        logger.info(f"총 저장된 항목: {total_success}개")
        
        # 총 실행 시간
        hours, remainder = divmod(total_collection_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.info(f"총 실행 시간: {int(hours)}시간 {int(minutes)}분 {int(seconds)}초")
        logger.info("=" * 80)
        
        # 최종 CSV 내보내기
        try:
            csv_path = collector.export_to_csv()
            if csv_path:
                logger.info(f"CSV 내보내기 완료: {csv_path}")
        except Exception as e:
            logger.error(f"CSV 내보내기 중 오류: {e}")
        
        # 결과 요약
        print("\n의약품 정보 수집 완료:")
        print(f"총 수집 반복: {iteration}회")
        print(f"총 검색 횟수: {total_processed}회")
        print(f"총 저장된 항목: {total_success}개")
        
        if 'csv_path' in locals():
            print(f"CSV 파일: {csv_path}")
        
        # HTML 보고서 경로 안내
        html_files = [f for f in os.listdir(collector.html_dir) if f.endswith('.html')]
        if html_files:
            print(f"\nHTML 보고서가 {collector.html_dir} 디렉토리에 생성되었습니다.")
            for html_file in html_files:
                print(f"- {os.path.join(collector.html_dir, html_file)}")
        
        return 0
            
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}", exc_info=True)
        print(f"\n\n오류 발생: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())