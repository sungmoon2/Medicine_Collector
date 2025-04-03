#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 검색 API를 활용한 의약품 정보 수집기

이 스크립트는 네이버 검색 API를 사용하여 의약품 정보를 검색하고,
검색 결과에서 의약품사전 페이지를 찾아 데이터를 추출합니다.
추출된 데이터는 JSON 형태로 저장되며, 수집 결과는 HTML 파일로 생성됩니다.
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
from utils.safety import setup_signal_handlers, sigint_handler, force_exit_handler, watchdog_thread
from utils.keyword_manager import generate_medicine_keywords

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
    네이버 검색 API 기반 의약품 정보 수집기 v1.0
    ======================================================================
    """
    print(banner)

# 메인 함수
def main():
    """
    메인 함수 (종료 기능 강화)
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
        
        # 키워드 설정
        if args.keywords:
            keywords = [k.strip() for k in args.keywords.split(',')]
        else:
            print("검색 키워드를 생성합니다...")
            new_keyword_count = collector.generate_medicine_keywords()
            keywords = collector.load_keywords()
            print(f"총 {len(keywords)}개 키워드가 생성되었습니다.")
        
        # 수집 실행
        stats = collector.collect_medicines(keywords, args.max_items)
        
        # 결과 요약
        print("\n의약품 정보 수집 완료:")
        print(f"총 검색 횟수: {stats['total_searches']}회")
        print(f"발견한 항목: {stats['total_found']}개")
        print(f"저장된 항목: {stats['total_saved']}개")
        print(f"실패한 항목: {stats['failed_items']}개")
        
        if 'csv_path' in stats:
            print(f"CSV 파일: {stats['csv_path']}")
        
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