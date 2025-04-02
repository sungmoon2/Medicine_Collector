#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
의약품 JSON 파일을 MySQL 데이터베이스로 변환하는 도구

이 스크립트는 'medicine_collector.py'로 수집된 JSON 파일들을 읽어
MySQL 데이터베이스 구조에 맞게 데이터를 삽입합니다.
"""

import os
import sys
import json
import glob
import re
import pymysql
import logging
import argparse
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("json_to_mysql.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MedicineJsonToMySQL:
    """JSON 파일에서 MySQL로 의약품 데이터를 변환하는 클래스"""
    
    def __init__(self, host, user, password, database, port=3306, charset='utf8mb4'):
        """
        초기화
        
        Args:
            host: MySQL 호스트
            user: MySQL 사용자
            password: MySQL 비밀번호
            database: MySQL 데이터베이스 이름
            port: MySQL 포트 (기본값: 3306)
            charset: 문자셋 (기본값: utf8mb4)
        """
        # MySQL 연결 정보
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        
        # 데이터베이스 연결
        self.conn = None
        self.cursor = None
        
        # 통계
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0
        }
        
        # 카테고리 매핑 캐시 (코드 → ID)
        self.category_cache = {}
        
        logger.info("MySQL 변환기 초기화 완료")
    
    def connect(self):
        """MySQL 데이터베이스에 연결"""
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
            logger.info(f"MySQL 데이터베이스 '{self.database}'에 연결되었습니다.")
            return True
        
        except Exception as e:
            logger.error(f"MySQL 연결 오류: {e}")
            return False
    
    def disconnect(self):
        """MySQL 데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            logger.info("MySQL 연결이 종료되었습니다.")
    
    def process_json_dir(self, json_dir, batch_size=100):
        """
        디렉토리 내 모든 JSON 파일 처리
        
        Args:
            json_dir: JSON 파일이 있는 디렉토리 경로
            batch_size: 일괄 처리할 파일 수 (기본값: 100)
            
        Returns:
            dict: 처리 통계
        """
        if not os.path.exists(json_dir) or not os.path.isdir(json_dir):
            logger.error(f"유효하지 않은 디렉토리: {json_dir}")
            return self.stats
        
        # JSON 파일 목록 가져오기
        json_files = glob.glob(os.path.join(json_dir, "*.json"))
        total_files = len(json_files)
        
        if total_files == 0:
            logger.warning(f"디렉토리 '{json_dir}'에 JSON 파일이 없습니다.")
            return self.stats
        
        logger.info(f"총 {total_files}개의 JSON 파일을 처리합니다.")
        
        # 데이터베이스 연결
        if not self.connect():
            return self.stats
        
        try:
            # 배치 처리를 위한 준비
            batch_count = 0
            current_batch = []
            
            # 모든 파일 처리
            for json_file in tqdm(json_files, desc="JSON 파일 처리"):
                try:
                    # JSON 파일 로드
                    with open(json_file, 'r', encoding='utf-8') as f:
                        medicine_data = json.load(f)
                    
                    # 배치에 추가
                    current_batch.append(medicine_data)
                    batch_count += 1
                    
                    # 배치 크기에 도달하면 처리
                    if batch_count >= batch_size:
                        self._process_batch(current_batch)
                        current_batch = []
                        batch_count = 0
                
                except Exception as e:
                    logger.error(f"파일 처리 오류 ({json_file}): {e}")
                    self.stats['error_count'] += 1
            
            # 남은 배치 처리
            if current_batch:
                self._process_batch(current_batch)
            
            # 커밋
            self.conn.commit()
            
            # 통계 출력
            logger.info("JSON 파일 처리 완료")
            logger.info(f"처리된 파일: {self.stats['total_processed']}개")
            logger.info(f"성공: {self.stats['success_count']}개")
            logger.info(f"오류: {self.stats['error_count']}개")
            logger.info(f"중복 스킵: {self.stats['skipped_count']}개")
        
        except Exception as e:
            logger.error(f"일괄 처리 중 오류: {e}")
            self.conn.rollback()
        
        finally:
            # 연결 종료
            self.disconnect()
        
        return self.stats
    
    def _process_batch(self, batch_data):
        """
        배치 데이터 처리
        
        Args:
            batch_data: 처리할 의약품 데이터 배치
        """
        for medicine_data in batch_data:
            self.stats['total_processed'] += 1
            
            try:
                # 데이터 정제 및 변환
                transformed_data = self._transform_medicine_data(medicine_data)
                
                # medicine_info 테이블에 삽입
                success = self._insert_medicine_info(transformed_data)
                
                if success:
                    # 성공 시 관련 테이블에 추가 데이터 삽입
                    medicine_id = transformed_data.get('id')
                    
                    # 분할선 정보가 있으면 삽입
                    if 'division_info' in medicine_data and medicine_data['division_info']:
                        self._insert_division_info(medicine_id, medicine_data['division_info'])
                    
                    # 이미지 메타데이터가 있으면 삽입
                    if all(k in medicine_data for k in ['image_url', 'image_width', 'image_height', 'image_alt']):
                        self._insert_image_metadata(medicine_id, medicine_data)
                    
                    self.stats['success_count'] += 1
                else:
                    self.stats['skipped_count'] += 1
            
            except Exception as e:
                logger.error(f"데이터 처리 오류 ({medicine_data.get('korean_name', '이름 없음')}): {e}")
                self.stats['error_count'] += 1
    
    def _transform_medicine_data(self, medicine_data):
        """
        의약품 데이터 변환 및 정제
        
        Args:
            medicine_data: 원본 의약품 데이터
            
        Returns:
            dict: 변환된 의약품 데이터
        """
        # MySQL 테이블 구조에 맞게 필드 매핑
        transformed = {
            'id': medicine_data.get('id', ''),
            'name_kr': medicine_data.get('korean_name', ''),
            'name_en': medicine_data.get('english_name', ''),
            'company': medicine_data.get('company', ''),
            'type': medicine_data.get('type', ''),
            'category': self._clean_category(medicine_data.get('category', '')),
            'insurance_code': medicine_data.get('insurance_code', ''),
            'appearance': medicine_data.get('appearance', ''),
            'shape': medicine_data.get('shape', ''),
            'color': medicine_data.get('color', ''),
            'size': medicine_data.get('size', ''),
            'identification': medicine_data.get('identification', ''),
            'components': medicine_data.get('components', ''),
            'efficacy': medicine_data.get('efficacy', ''),
            'precautions': medicine_data.get('precautions', ''),
            'dosage': medicine_data.get('dosage', ''),
            'storage': medicine_data.get('storage', ''),
            'expiration': medicine_data.get('expiration', ''),
            'image_url': medicine_data.get('image_url', ''),
            'source_url': medicine_data.get('url', ''),
            'extracted_time': self._parse_datetime(medicine_data.get('extracted_time', '')),
            'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 누락된 ID 필드 처리
        if not transformed['id']:
            transformed['id'] = self._generate_medicine_id(medicine_data)
        
        # URL 필드 정리
        if not transformed['source_url'] and 'link' in medicine_data:
            transformed['source_url'] = medicine_data['link']
        
        return transformed
    
    def _clean_category(self, category):
        """
        카테고리 정보 정리
        
        Args:
            category: 원본 카테고리 문자열
            
        Returns:
            str: 정리된 카테고리 문자열
        """
        if not category:
            return ''
        
        # 대괄호 코드 추출 (예: [01140]해열.진통.소염제)
        code_match = re.match(r'\[([^\]]+)\](.*)', category)
        if code_match:
            code = code_match.group(1)
            name = code_match.group(2).strip()
            
            # 카테고리 테이블에 저장 (필요시)
            try:
                if code not in self.category_cache:
                    # 카테고리 존재 여부 확인
                    self.cursor.execute(
                        "SELECT id FROM medicine_categories WHERE category_code = %s",
                        (code,)
                    )
                    result = self.cursor.fetchone()
                    
                    if result:
                        self.category_cache[code] = result['id']
                    else:
                        # 새 카테고리 삽입
                        self.cursor.execute(
                            "INSERT INTO medicine_categories (category_code, category_name) VALUES (%s, %s)",
                            (code, name)
                        )
                        self.category_cache[code] = self.cursor.lastrowid
            except Exception as e:
                logger.warning(f"카테고리 처리 오류: {e}")
            
            # 원본 카테고리 반환
            return category
        
        return category
    
    def _parse_datetime(self, dt_str):
        """
        날짜/시간 문자열을 MySQL datetime 형식으로 변환
        
        Args:
            dt_str: 날짜/시간 문자열
            
        Returns:
            str: MySQL datetime 형식 문자열
        """
        if not dt_str:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # ISO 형식 변환 시도 (2023-09-17T14:30:15.123456)
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                # 다른 일반적인 형식 시도
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S']:
                    try:
                        dt = datetime.strptime(dt_str, fmt)
                        return dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        continue
            except:
                pass
        
        # 변환 실패 시 현재 시간 반환
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _generate_medicine_id(self, medicine_data):
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
    
    def _insert_medicine_info(self, medicine_data):
        """
        medicine_info 테이블에 데이터 삽입
        
        Args:
            medicine_data: 삽입할 의약품 데이터
            
        Returns:
            bool: 성공 여부
        """
        try:
            # 이미 존재하는지 확인
            self.cursor.execute(
                "SELECT id FROM medicine_info WHERE id = %s",
                (medicine_data['id'],)
            )
            
            if self.cursor.fetchone():
                # 기존 데이터 업데이트 (필요시)
                #self.cursor.execute(
                #    "UPDATE medicine_info SET ... WHERE id = %s",
                #    (medicine_data['id'],)
                #)
                logger.info(f"이미 존재하는 의약품 ID: {medicine_data['id']}")
                return False
            
            # 새 데이터 삽입
            sql = """
            INSERT INTO medicine_info (
                id, name_kr, name_en, company, type, category, 
                insurance_code, appearance, shape, color, size, 
                identification, components, efficacy, precautions, 
                dosage, storage, expiration, image_url, source_url, 
                extracted_time, updated_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s
            )
            """
            
            self.cursor.execute(sql, (
                medicine_data['id'],
                medicine_data['name_kr'],
                medicine_data['name_en'],
                medicine_data['company'],
                medicine_data['type'],
                medicine_data['category'],
                medicine_data['insurance_code'],
                medicine_data['appearance'],
                medicine_data['shape'],
                medicine_data['color'],
                medicine_data['size'],
                medicine_data['identification'],
                medicine_data['components'],
                medicine_data['efficacy'],
                medicine_data['precautions'],
                medicine_data['dosage'],
                medicine_data['storage'],
                medicine_data['expiration'],
                medicine_data['image_url'],
                medicine_data['source_url'],
                medicine_data['extracted_time'],
                medicine_data['updated_time']
            ))
            
            return True
        
        except Exception as e:
            logger.error(f"medicine_info 테이블 삽입 오류: {e}")
            return False
    
    def _insert_division_info(self, medicine_id, division_info):
        """
        medicine_division 테이블에 데이터 삽입
        
        Args:
            medicine_id: 의약품 ID
            division_info: 분할선 정보
        """
        try:
            # 데이터 형식 확인
            if not isinstance(division_info, dict):
                return
            
            description = division_info.get('division_description', '')
            if not description:
                return
            
            # 중복 확인
            self.cursor.execute(
                "SELECT id FROM medicine_division WHERE medicine_id = %s AND division_description = %s",
                (medicine_id, description)
            )
            
            if self.cursor.fetchone():
                return
            
            # 데이터 삽입
            self.cursor.execute(
                "INSERT INTO medicine_division (medicine_id, division_description) VALUES (%s, %s)",
                (medicine_id, description)
            )
        
        except Exception as e:
            logger.warning(f"분할선 정보 삽입 오류: {e}")
    
    def _insert_image_metadata(self, medicine_id, medicine_data):
        """
        medicine_images 테이블에 이미지 메타데이터 삽입
        
        Args:
            medicine_id: 의약품 ID
            medicine_data: 의약품 데이터
        """
        try:
            image_url = medicine_data.get('image_url', '')
            if not image_url:
                return
            
            # 중복 확인
            self.cursor.execute(
                "SELECT id FROM medicine_images WHERE medicine_id = %s AND image_url = %s",
                (medicine_id, image_url)
            )
            
            if self.cursor.fetchone():
                return
            
            # 데이터 삽입
            self.cursor.execute(
                "INSERT INTO medicine_images (medicine_id, image_url, image_width, image_height, image_alt) VALUES (%s, %s, %s, %s, %s)",
                (
                    medicine_id,
                    image_url,
                    medicine_data.get('image_width', ''),
                    medicine_data.get('image_height', ''),
                    medicine_data.get('image_alt', '')
                )
            )
        
        except Exception as e:
            logger.warning(f"이미지 메타데이터 삽입 오류: {e}")
    
    @staticmethod
    def print_banner():
        """프로그램 시작 배너 출력"""
        banner = r"""
        ======================================================================
        의약품 JSON → MySQL 변환기 v1.0
        ======================================================================
        """
        print(banner)

def main():
    """
    메인 함수
    """
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='의약품 JSON 파일을 MySQL 데이터베이스로 변환')
    parser.add_argument('--json-dir', required=True, help='JSON 파일이 있는 디렉토리')
    parser.add_argument('--host', default='localhost', help='MySQL 호스트 (기본값: localhost)')
    parser.add_argument('--port', type=int, default=3306, help='MySQL 포트 (기본값: 3306)')
    parser.add_argument('--user', help='MySQL 사용자')
    parser.add_argument('--password', help='MySQL 비밀번호')
    parser.add_argument('--database', help='MySQL 데이터베이스 이름')
    parser.add_argument('--batch-size', type=int, default=100, help='배치 처리 크기 (기본값: 100)')
    args = parser.parse_args()
    
    # 배너 출력
    MedicineJsonToMySQL.print_banner()
    
    # 환경 변수 로드 (MySQL 설정을 .env 파일에서 가져올 수 있음)
    load_dotenv()
    
    # 연결 정보 설정
    host = args.host or os.environ.get('MYSQL_HOST', 'localhost')
    port = args.port or int(os.environ.get('MYSQL_PORT', 3306))
    user = args.user or os.environ.get('MYSQL_USER')
    password = args.password or os.environ.get('MYSQL_PASSWORD')
    database = args.database or os.environ.get('MYSQL_DATABASE')
    
    # 필수 정보 확인
    if not all([user, password, database]):
        print("오류: MySQL 연결 정보가 부족합니다.")
        print("--user, --password, --database 옵션을 제공하거나")
        print("환경 변수 MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE를 설정하세요.")
        return 1
    
    # 변환기 초기화 및 실행
    converter = MedicineJsonToMySQL(host, user, password, database, port)
    
    try:
        stats = converter.process_json_dir(args.json_dir, args.batch_size)
        
        # 결과 요약
        print("\nJSON → MySQL 변환 완료:")
        print(f"처리된 파일: {stats['total_processed']}개")
        print(f"성공: {stats['success_count']}개")
        print(f"오류: {stats['error_count']}개")
        print(f"중복 스킵: {stats['skipped_count']}개")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.")
        return 1
        
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}", exc_info=True)
        print(f"\n\n오류 발생: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())