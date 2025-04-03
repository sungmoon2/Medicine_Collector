#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MedicineCollector 클래스 기본 구조
"""

import os
import re
import json
import time
import random
import logging
import threading
import concurrent.futures
from datetime import datetime
from tqdm import tqdm
from collections import OrderedDict

from api.naver_api import search_api, filter_medicine_items
from parser.html_parser import is_medicine_page, fetch_medicine_data
from utils.file_utils import save_medicine_data, is_duplicate_medicine, export_to_csv, generate_medicine_id, sanitize_filename
from utils.keyword_manager import load_keywords, update_keyword_progress, generate_medicine_keywords
from utils.checkpoint import save_checkpoint, load_checkpoint
from utils.html_report import init_html_report, add_to_html_report, finalize_html_report
from utils.safety import safe_regex_search, safe_regex_group

# 로거 설정
logger = logging.getLogger(__name__)

# 전역 종료 플래그
shutdown_requested = False
shutdown_event = threading.Event()

class MedicineCollector:
    """네이버 검색 API를 사용한 의약품 정보 수집 클래스"""
    
    def __init__(self, client_id, client_secret, output_dir="collected_data"):
        """
        초기화
        
        Args:
            client_id: 네이버 API 클라이언트 ID
            client_secret: 네이버 API 클라이언트 시크릿
            output_dir: 데이터 저장 디렉토리
        """
        # API 인증 정보
        self.client_id = client_id
        self.client_secret = client_secret
        
        # 저장 경로
        self.output_dir = output_dir
        self.data_dir = os.path.join(output_dir, "data")
        self.html_dir = os.path.join(output_dir, "html")
        self.json_dir = os.path.join(output_dir, "json")
        
        # 디렉토리 생성
        for dir_path in [self.output_dir, self.data_dir, self.html_dir, self.json_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # 사용자 에이전트 목록 (config.settings에서 가져옴)
        from config.settings import USER_AGENTS
        self.user_agents = USER_AGENTS
        
        # 수집 통계
        self.stats = {
            'total_searches': 0,
            'total_found': 0,
            'total_saved': 0,
            'failed_items': 0,
            'medicine_items': []
        }
        
        # 현재 HTML 저장 파일 및 아이템 카운터
        self.current_html_file = None
        self.current_html_count = 0
        self.html_item_limit = 100  # HTML 파일당 최대 항목 수
        
        logger.info("의약품 데이터 수집기 초기화 완료")
    
    def search_api(self, keyword, display=100, start=1, max_retries=3):
        """
        네이버 검색 API를 사용하여 검색
        
        Args:
            keyword: 검색 키워드
            display: 결과 개수 (최대 100)
            start: 시작 인덱스
            max_retries: 최대 재시도 횟수
            
        Returns:
            dict: 검색 결과
        """
        result = search_api(
            keyword, 
            display, 
            start, 
            max_retries, 
            self.client_id, 
            self.client_secret, 
            self.user_agents, 
            self.output_dir
        )
        
        # 통계 업데이트
        if result and 'items' in result and not 'error' in result:
            self.stats['total_searches'] += 1
            
        return result
    
    def filter_medicine_items(self, search_result):
        """
        검색 결과에서 의약품사전 항목 필터링
        
        Args:
            search_result: 검색 API 응답
            
        Returns:
            list: 필터링된 의약품사전 항목
        """
        return filter_medicine_items(search_result)
    
    def fetch_medicine_data(self, item, max_retries=3):
        """
        검색 결과 항목에서 의약품 페이지 데이터 추출
        
        Args:
            item: 검색 결과 항목
            max_retries: 최대 재시도 횟수
                
        Returns:
            dict: 추출된 의약품 데이터
        """
        # 기본 메타데이터 구성
        medicine_data = {
            "title": re.sub(r'<.*?>', '', item.get("title", "")),
            "link": item.get("link", ""),
            "description": re.sub(r'<.*?>', '', item.get("description", "")),
            "category": item.get("category", ""),
            "collection_time": datetime.now().isoformat()
        }
        
        # 이 클래스에서 필요한 핵심 속성만 전달
        data = fetch_medicine_data(
            item,
            medicine_data,
            max_retries,
            self.user_agents,
            self.output_dir
        )
        
        return data
    
    def is_duplicate_medicine(self, medicine_data):
        """
        중복 의약품 검사
        
        Args:
            medicine_data (dict): 의약품 데이터
        
        Returns:
            bool: 중복 여부
        """
        return is_duplicate_medicine(medicine_data, self.output_dir)

    def standardize_medicine_data(self, medicine_data):
        """
        의약품 데이터 표준화 - 일관된 구조와 "정보 없음" 기본값
        ID 필드를 맨 위에 배치하고 필드 순서 일관성 유지
        
        Args:
            medicine_data (dict): 원본 의약품 데이터
            
        Returns:
            OrderedDict: 표준화된 의약품 데이터 (순서 보장)
        """
        # 1. 원본 데이터에서 ID 추출 또는 생성
        medicine_id = medicine_data.get('id', '')
        if not medicine_id:
            medicine_id = generate_medicine_id(medicine_data)
        
        # 2. 정렬된 필드 순서 정의 (ID가 맨 앞에 오도록)
        field_order = [
            # ID는 항상 맨 앞에
            'id',
            
            # 기본 정보
            'korean_name',
            'english_name', 
            'title',
            'category',
            'classification',
            'company', 
            'insurance_code',
            
            # 성분 및 효능
            'components',
            'efficacy',
            'dosage',
            
            # 외형 정보
            'appearance',
            'shape_type',
            'shape',
            'color',
            'size',
            'identification',
            
            # 보관 및 주의사항
            'storage_conditions',
            'expiration',
            'precautions',
            
            # 이미지 정보
            'image_quality',
            'image_url',
            'image_width',
            'image_height',
            'original_width',
            'original_height',
            'image_alt',
            
            # 메타데이터
            'link',
            'url',
            'description',
            'extracted_time',
            'collection_time'
        ]
        
        # 3. 표준화된 데이터를 OrderedDict로 생성 (순서 유지)
        standardized_data = OrderedDict()
        
        # 4. ID 먼저 설정
        standardized_data['id'] = medicine_id
        
        # 5. 정의된 순서대로 필드 채우기
        for field in field_order[1:]:  # ID는 이미 설정했으므로 건너뛰기
            if field in medicine_data and medicine_data[field]:
                # 원본 데이터에 값이 있으면 그대로 사용
                standardized_data[field] = medicine_data[field]
            else:
                # 값이 없으면 특별 필드는 빈 문자열, 나머지는 "정보 없음"
                if field in ['image_url', 'link', 'url', 'extracted_time', 'collection_time', 
                            'image_width', 'image_height', 'original_width', 'original_height', 
                            'image_quality', 'image_alt']:
                    standardized_data[field] = ""
                else:
                    standardized_data[field] = "정보 없음"
        
        # 6. 원본 데이터에서 추가 필드가 있다면 마지막에 추가 (순서 유지 없이)
        for key, value in medicine_data.items():
            if key not in standardized_data:
                standardized_data[key] = value
        
        # 7. 시간 정보 추가 (비어있는 경우)
        current_time = datetime.now().isoformat()
        if not standardized_data.get('extracted_time'):
            standardized_data['extracted_time'] = current_time
        if not standardized_data.get('collection_time'):
            standardized_data['collection_time'] = current_time
        
        return standardized_data

    def save_medicine_data(self, medicine_data):
        """
        의약품 데이터 저장 (개선된 버전)
        - 순서가 보장된 JSON 저장
        - 누락된 필드 "정보 없음"으로 표시
        - ID를 파일명 맨 앞에 배치
        
        Args:
            medicine_data (dict): 저장할 의약품 데이터
        
        Returns:
            tuple: (성공 여부, 파일 경로)
        """
        try:
            if not medicine_data:
                logger.warning("저장할 의약품 데이터가 없습니다.")
                return False, None
            
            # 로깅에 사용할 기본 정보 추출
            medicine_id = medicine_data.get('id', 'ID 없음')
            medicine_name = medicine_data.get('korean_name', medicine_data.get('title', '이름 없음'))
            
            # 0. 데이터 표준화 (OrderedDict 사용)
            medicine_data = self.standardize_medicine_data(medicine_data)
            
            # 1. 중복 검사
            if is_duplicate_medicine(medicine_data, self.output_dir):
                logger.info(f"[ID: {medicine_id}] 중복 의약품 '{medicine_name}' - 건너뜁니다.")
                return False, None
            
            # 2. 고유 ID 확인
            medicine_id = medicine_data['id']  # 표준화 함수에서 이미 설정됨
            
            # 3. JSON 파일로 저장
            json_filename = f"{medicine_id}_{sanitize_filename(medicine_name)}.json"
            json_path = os.path.join(self.json_dir, json_filename)
            
            # 디렉토리 생성
            os.makedirs(self.json_dir, exist_ok=True)
            
            # OrderedDict를 JSON으로 저장
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(medicine_data, f, ensure_ascii=False, indent=2)
            
            # 필드 정보 요약
            field_count = len(medicine_data.keys())
            important_fields = []
            
            # 주요 필드 유무 확인
            if 'components' in medicine_data and medicine_data['components'] and medicine_data['components'] != "정보 없음":
                important_fields.append("성분")
            if 'efficacy' in medicine_data and medicine_data['efficacy'] and medicine_data['efficacy'] != "정보 없음":
                important_fields.append("효능효과")
            if 'dosage' in medicine_data and medicine_data['dosage'] and medicine_data['dosage'] != "정보 없음":
                important_fields.append("용법용량")
            if 'precautions' in medicine_data and medicine_data['precautions'] and medicine_data['precautions'] != "정보 없음":
                important_fields.append("주의사항")
            if 'image_url' in medicine_data and medicine_data['image_url']:
                important_fields.append("이미지")
            
            fields_info = ", ".join(important_fields) if important_fields else "기본 정보만"
            
            # 저장 성공 로그
            logger.info(f"[ID: {medicine_id}] '{medicine_name}' 저장 완료 ({field_count}개 필드, {fields_info})")
            
            # 통계 업데이트
            self.stats['total_saved'] += 1
            self.stats['medicine_items'].append({
                'id': medicine_id,
                'name': medicine_data.get('korean_name', ''),
                'path': json_path
            })
            
            # HTML 보고서에 데이터 추가
            add_to_html_report(medicine_data, self.current_html_file, self.current_html_count)
            self.current_html_count += 1
            
            # HTML 파일 아이템 수 제한 체크
            if self.current_html_count >= self.html_item_limit:
                self._init_new_html_report()
            
            return True, json_path
                
        except Exception as e:
            # 오류 정보 추출
            med_id = medicine_data.get('id', 'ID 없음') if medicine_data else 'ID 없음'
            med_name = medicine_data.get('korean_name', medicine_data.get('title', '이름 없음')) if medicine_data else '이름 없음'
            
            # 상세한 오류 로깅
            logger.error(f"[ID: {med_id}] '{med_name}' 저장 실패: {e}")
            
            # 오류 데이터 별도 저장
            error_log_dir = os.path.join(self.output_dir, "error_logs")
            os.makedirs(error_log_dir, exist_ok=True)
            
            error_log_path = os.path.join(error_log_dir, f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            with open(error_log_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "error": str(e),
                    "medicine_id": med_id,
                    "medicine_name": med_name,
                    "timestamp": datetime.now().isoformat(),
                    "medicine_data": dict(medicine_data) if medicine_data else {}
                }, f, ensure_ascii=False, indent=2)
            
            # 실패 통계 업데이트
            self.stats['failed_items'] += 1
            
            return False, None
    
    def _init_new_html_report(self):
        """새 HTML 보고서 초기화"""
        self.current_html_file = init_html_report(self.html_dir)
        self.current_html_count = 0
    
    def export_to_csv(self, output_path=None, batch_size=500):
        """
        수집된 의약품 데이터를 CSV로 내보내기
        
        Args:
            output_path: 출력 파일 경로 (없으면 자동 생성)
            batch_size: 한 번에 처리할 JSON 파일 수 (메모리 효율성)
                
        Returns:
            str: CSV 파일 경로
        """
        if output_path is None:
            output_path = os.path.join(self.output_dir, f"medicine_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            
        return export_to_csv(
            self.stats, 
            self.json_dir, 
            output_path, 
            batch_size
        )
        
    def load_keywords(self):
        """
        키워드 파일 로드
        
        Returns:
            list: 키워드 목록
        """
        return load_keywords(self.output_dir)

    def update_keyword_progress(self, completed_keyword):
        """
        완료된 키워드 관리
        
        Args:
            completed_keyword (str): 완료된 키워드
        """
        update_keyword_progress(completed_keyword, self.output_dir)

    def save_checkpoint(self, keyword, processed_count=0):
        """
        현재 진행 상태 체크포인트 저장
        
        Args:
            keyword (str): 현재 처리 중인 키워드
            processed_count (int): 현재 키워드에서 처리한 항목 수
        """
        save_checkpoint(
            keyword, 
            processed_count, 
            self.stats, 
            self.output_dir
        )

    def load_checkpoint(self):
        """
        마지막 체크포인트 로드
        
        Returns:
            dict or None: 체크포인트 데이터
        """
        return load_checkpoint(self.output_dir)

    def generate_medicine_keywords(self, max_new_keywords=100, similarity_threshold=0.8):
        """
        수집된 의약품 데이터를 분석하여 추가 키워드 생성
        
        Args:
            max_new_keywords (int): 최대 생성할 새 키워드 수
            similarity_threshold (float): 유사도 임계값 (0.0~1.0)
        
        Returns:
            int: 추가된 키워드 수
        """
        return generate_medicine_keywords(
            self.output_dir,
            self.json_dir,
            max_new_keywords,
            similarity_threshold
        )

    def collect_medicines(self, keywords=None, max_items=None, max_workers=4, timeout=10):
        """
        키워드 목록으로 의약품 정보 수집 (안전하게 종료되는 병렬 처리 버전)
        
        Args:
            keywords: 검색할 키워드 목록 (None이면 파일에서 로드)
            max_items: 최대 수집 항목 수
            max_workers: 병렬 처리를 위한 최대 작업자 수
            timeout: future 작업 대기 타임아웃 (초)
                    
        Returns:
            dict: 수집 통계
        """
        # 전역 종료 플래그 참조
        global shutdown_requested, shutdown_event
        if 'shutdown_requested' not in globals():
            shutdown_requested = False
            shutdown_event = threading.Event()
        
        # 작업 상태 추적을 위한 변수들
        completed_count = 0
        keyword_queue = []
        executor = None
        
        # 키워드가 제공되지 않으면 파일에서 로드
        if keywords is None:
            keywords = self.load_keywords()
        
        if not keywords:
            logger.warning("검색할 키워드가 없습니다.")
            return self.stats
        
        logger.info(f"총 {len(keywords)}개 키워드로 검색 시작")
        
        # 체크포인트 로드
        checkpoint = self.load_checkpoint()
        
        # 통계 초기화
        self.stats = {
            'total_searches': checkpoint.get('total_searches', 0) if checkpoint else 0,
            'total_found': checkpoint.get('total_found', 0) if checkpoint else 0,
            'total_saved': checkpoint.get('total_saved', 0) if checkpoint else 0,
            'failed_items': checkpoint.get('failed_items', 0) if checkpoint else 0,
            'medicine_items': [],
            'start_time': datetime.now()
        }
        
        # HTML 보고서 초기화
        self._init_new_html_report()
        
        # 체크포인트가 있으면 이어서 진행
        current_index = 0
        if checkpoint and 'current_keyword' in checkpoint:
            # 이미 처리된 키워드와 현재 진행 중인 키워드 확인
            current_keyword = checkpoint['current_keyword']
            
            # 키워드 리스트에서 현재 키워드의 인덱스 찾기
            for i, keyword in enumerate(keywords):
                if keyword == current_keyword:
                    current_index = i
                    break
            
            logger.info(f"체크포인트에서 이어서 진행: {current_keyword} (인덱스: {current_index})")
        
        # 진행할 키워드 목록
        keywords_to_process = keywords[current_index:]
        if not keywords_to_process:
            logger.info("처리할 새 키워드가 없습니다. 모든 키워드가 이미 처리되었습니다.")
            return self.stats
        
        # 현재 진행 중인 키워드 표시
        with open(os.path.join(self.output_dir, "current_keyword.txt"), 'w', encoding='utf-8') as f:
            f.write(keywords_to_process[0])
        
        # 진행 상황 동기화를 위한 잠금
        stats_lock = threading.Lock()
        
        # 키워드별 처리 결과
        keyword_results = {}
        
        # 자원 정리를 위한 cleanup 함수
        def cleanup_resources():
            nonlocal executor, futures
            
            logger.info("자원 정리 중...")
            
            # 미완료 작업 취소
            for future in list(futures.keys()):
                if not future.done():
                    future.cancel()
            
            # 쓰레드풀 안전하게 종료
            if executor:
                logger.info("쓰레드풀 종료 중...")
                executor.shutdown(wait=False)
            
            # HTML 보고서 마무리
            finalize_html_report(self.current_html_file)
            
            # 모든 작업이 취소됨을 보장
            shutdown_event.set()
            logger.info("자원 정리 완료")
        
        # 종료 요청 감지 함수
        def check_shutdown():
            if shutdown_requested or (hasattr(threading, 'current_thread') and 
                                    getattr(threading.current_thread(), "_stop_requested", False)):
                logger.info("종료 요청이 감지되었습니다.")
                return True
            return False
        
        try:
            # 병렬 처리 설정
            # 시스템 리소스를 고려한 작업자 수 조정
            effective_workers = min(max_workers, len(keywords_to_process), os.cpu_count() or 4)
            logger.info(f"병렬 처리 작업자 수: {effective_workers}")
            
            # 작업 분배 방식 개선
            # 1. 병렬 처리할 최대 키워드 수 제한 (메모리 고려)
            max_parallel_keywords = min(10, len(keywords_to_process))
            
            # 2. 병렬 처리 설정
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers, 
                                                            thread_name_prefix="Medicine")
            
            # 처리할 키워드 큐
            keyword_queue = list(keywords_to_process[:max_parallel_keywords])
            futures = {}
            
            # 초기 작업 제출
            for keyword in keyword_queue:
                if check_shutdown():
                    break
                future = executor.submit(self._process_keyword, keyword, max_items, stats_lock)
                futures[future] = keyword
            
            # 작업 완료 처리 및 새 작업 제출
            completed_count = 0
            while futures and not check_shutdown():
                try:
                    # 짧은 타임아웃으로 완료된 작업 처리
                    done, pending = concurrent.futures.wait(
                        futures.keys(), 
                        return_when=concurrent.futures.FIRST_COMPLETED,
                        timeout=timeout  # 짧은 타임아웃으로 자주 체크
                    )
                    
                    # 타임아웃 체크 - 종료 요청 확인
                    if not done:
                        logger.debug(f"작업 완료 대기 중 {timeout}초 타임아웃, 종료 요청 확인 중...")
                        # 셧다운 플래그 확인
                        if check_shutdown():
                            logger.info("종료 요청이 감지되었습니다. 모든 작업을 취소합니다.")
                            break
                        # 계속 대기
                        continue
                    
                    # 완료된 작업 결과 처리
                    for future in done:
                        if check_shutdown():
                            break
                            
                        keyword = futures.pop(future, None)
                        if not keyword:
                            continue
                        
                        try:
                            result = future.result()
                            keyword_results[keyword] = result
                            
                            # 키워드 진행 상태 업데이트
                            self.update_keyword_progress(keyword)
                            completed_count += 1
                            
                            # 진행률 출력
                            progress = min(100, round(completed_count / len(keywords_to_process) * 100))
                            elapsed = (datetime.now() - self.stats['start_time']).total_seconds() / 60
                            if completed_count > 0 and elapsed > 0:
                                estimated_total = elapsed * len(keywords_to_process) / completed_count
                                remaining = max(0, estimated_total - elapsed)
                                logger.info(f"진행률: {progress}% ({completed_count}/{len(keywords_to_process)}) - "
                                        f"경과: {elapsed:.1f}분, 남은 시간: {remaining:.1f}분")
                            
                            # 최대 항목 수 체크
                            with stats_lock:
                                if max_items and self.stats['total_saved'] >= max_items:
                                    logger.info(f"최대 항목 수 {max_items}개에 도달했습니다.")
                                    # 종료 플래그 설정
                                    shutdown_requested = True
                                    break
                        
                        except concurrent.futures.CancelledError:
                            logger.info(f"키워드 '{keyword}' 처리가 취소되었습니다.")
                        except Exception as e:
                            logger.error(f"키워드 '{keyword}' 처리 중 오류: {e}")
                            keyword_results[keyword] = {"error": str(e)}
                    
                    # 종료 요청 확인
                    if check_shutdown():
                        break
                    
                    # 새 키워드 추가 (완료된 작업 수만큼)
                    current_queue_size = len(futures)
                    if current_queue_size < max_parallel_keywords:
                        # 추가할 작업 수 계산
                        to_add = max_parallel_keywords - current_queue_size
                        next_index = current_index + max_parallel_keywords + completed_count
                        new_keywords = keywords_to_process[max_parallel_keywords:next_index]
                        
                        # 최대 to_add개 추가
                        new_keywords = new_keywords[:to_add]
                        
                        for keyword in new_keywords:
                            if check_shutdown():
                                break
                            
                            if keyword not in keyword_results and keyword not in [futures.get(f) for f in futures]:
                                # 현재 진행 중인 키워드 표시
                                with open(os.path.join(self.output_dir, "current_keyword.txt"), 'w', encoding='utf-8') as f:
                                    f.write(keyword)
                                
                                future = executor.submit(self._process_keyword, keyword, max_items, stats_lock)
                                futures[future] = keyword
                
                except concurrent.futures.TimeoutError:
                    logger.debug("작업 대기 타임아웃, 종료 요청 확인 중...")
                    # 종료 요청 확인
                    if check_shutdown():
                        break
                
                except Exception as e:
                    logger.error(f"작업 처리 중 예외 발생: {e}")
                    # 종료 요청 확인
                    if check_shutdown():
                        break
                    
                    # 잠시 대기 후 계속
                    time.sleep(1)
            
            # 작업 완료 또는 종료 요청으로 루프 종료
            if check_shutdown():
                logger.info("종료 요청으로 남은 작업을 취소합니다.")
            else:
                logger.info("모든 작업이 완료되었습니다.")
            
        except KeyboardInterrupt:
            logger.info("키보드 인터럽트가 감지되었습니다. 작업을 종료합니다.")
            shutdown_requested = True
        except Exception as e:
            logger.error(f"수집 중 오류 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        finally:
            try:
                # 자원 정리
                cleanup_resources()
                
                # 최종 체크포인트 저장
                if futures:
                    remaining_keywords = list(set(futures.values()))
                    if remaining_keywords:
                        logger.info(f"남은 키워드 {len(remaining_keywords)}개에 대한 체크포인트 저장")
                        self.save_checkpoint(remaining_keywords[0])
                
                # CSV 파일 생성
                try:
                    csv_path = self.export_to_csv()
                    if csv_path:
                        self.stats['csv_path'] = csv_path
                except Exception as e:
                    logger.error(f"CSV 파일 생성 중 오류: {e}")
                    
                # 최종 통계 출력
                logger.info("의약품 수집 완료 또는 중단됨")
                logger.info(f"총 검색 횟수: {self.stats['total_searches']}회")
                logger.info(f"발견한 항목: {self.stats['total_found']}개")
                logger.info(f"저장된 항목: {self.stats['total_saved']}개")
                logger.info(f"실패한 항목: {self.stats['failed_items']}개")
                
                # 총 소요 시간
                elapsed_time = (datetime.now() - self.stats['start_time']).total_seconds() / 60
                logger.info(f"총 소요 시간: {elapsed_time:.1f}분")
                
                # 모든 키워드 처리가 완료된 경우에만 체크포인트 파일 제거
                if not shutdown_requested and completed_count == len(keywords_to_process):
                    checkpoint_path = os.path.join(self.output_dir, "checkpoint.json")
                    if os.path.exists(checkpoint_path):
                        os.remove(checkpoint_path)
                        logger.info("모든 키워드 처리 완료, 체크포인트 파일 제거")
                        
            except Exception as cleanup_error:
                logger.error(f"종료 처리 중 오류: {cleanup_error}")
        
        return self.stats

    def _process_keyword(self, keyword, max_items, stats_lock):
        """
        단일 키워드 처리 (병렬 처리를 위해 분리된 함수) - 안전한 종료 지원
        
        Args:
            keyword: 처리할 키워드
            max_items: 최대 항목 수
            stats_lock: 통계 업데이트를 위한 잠금
            
        Returns:
            dict: 키워드 처리 결과
        """
        # 전역 종료 플래그 참조
        global shutdown_requested
        if 'shutdown_requested' not in globals():
            shutdown_requested = False
        
        logger.info(f"키워드 '{keyword}' 검색 시작")
        
        # 초기 체크포인트 저장
        self.save_checkpoint(keyword)
        
        # 키워드별 통계
        keyword_stats = {
            'keyword': keyword,
            'searches': 0,
            'found': 0,
            'saved': 0,
            'failed': 0,
            'items': []
        }
        
        try:
            # 종료 요청 확인
            if shutdown_requested:
                logger.info(f"종료 요청으로 키워드 '{keyword}' 처리를 중단합니다.")
                return keyword_stats
            
            # API 검색
            search_result = self.search_api(keyword)
            keyword_stats['searches'] += 1
            
            # 의약품 항목 필터링
            medicine_items = self.filter_medicine_items(search_result)
            
            if not medicine_items:
                logger.info(f"키워드 '{keyword}'에 대한 의약품 항목이 없습니다.")
                return keyword_stats
            
            # 필터링된 항목 수 기록
            item_count = len(medicine_items)
            keyword_stats['found'] = item_count
            
            with stats_lock:
                self.stats['total_found'] += item_count
            
            logger.info(f"키워드 '{keyword}'에서 {item_count}개 의약품 항목 발견")
            
            # 각 항목 처리
            for item_index, item in enumerate(medicine_items):
                # 종료 요청 확인
                if shutdown_requested:
                    logger.info(f"종료 요청으로 키워드 '{keyword}' 처리를 중단합니다. (처리 항목: {item_index}/{item_count})")
                    # 현재 진행 상황 체크포인트 저장
                    self.save_checkpoint(keyword, item_index)
                    break
                
                # 최대 항목 수 체크
                with stats_lock:
                    if max_items and self.stats['total_saved'] >= max_items:
                        logger.info(f"최대 항목 수 {max_items}개에 도달했습니다.")
                        break
                
                try:
                    # 진행 상황 체크포인트 업데이트 (10개 단위)
                    if item_index > 0 and item_index % 10 == 0:
                        self.save_checkpoint(keyword, item_index)
                    
                    # 의약품 데이터 가져오기
                    medicine_data = self.fetch_medicine_data(item)
                    
                    if medicine_data:
                        # 데이터 저장
                        success, path = self.save_medicine_data(medicine_data)
                        
                        if success:
                            keyword_stats['saved'] += 1
                            keyword_stats['items'].append({
                                'id': medicine_data.get('id', ''),
                                'name': medicine_data.get('korean_name', ''),
                                'path': path
                            })
                        else:
                            keyword_stats['failed'] += 1
                            with stats_lock:
                                self.stats['failed_items'] += 1
                            logger.warning(f"데이터 저장 실패: {item.get('title', '제목 없음')}")
                    else:
                        keyword_stats['failed'] += 1
                        with stats_lock:
                            self.stats['failed_items'] += 1
                        logger.warning(f"데이터 가져오기 실패: {item.get('title', '제목 없음')}")
                
                except Exception as e:
                    keyword_stats['failed'] += 1
                    with stats_lock:
                        self.stats['failed_items'] += 1
                    logger.error(f"항목 처리 중 오류: {e}")
                
                # 종료 요청 확인
                if shutdown_requested:
                    logger.info(f"종료 요청으로 키워드 '{keyword}' 처리를 중단합니다. (처리 항목: {item_index+1}/{item_count})")
                    # 현재 진행 상황 체크포인트 저장
                    self.save_checkpoint(keyword, item_index)
                    break
                
                # 요청 간 지연 (개별 항목 요청 속도 제한)
                # 병렬 처리 시 서버 부하 줄이기 위해 더 긴 간격 유지
                time.sleep(random.uniform(1.0, 2.0))
        
        except Exception as e:
            logger.error(f"키워드 '{keyword}' 처리 중 오류: {e}")
            # 체크포인트 저장
            self.save_checkpoint(keyword)
        
        logger.info(f"키워드 '{keyword}' 처리 완료: 발견 {keyword_stats['found']}개, 저장 {keyword_stats['saved']}개")
        return keyword_stats