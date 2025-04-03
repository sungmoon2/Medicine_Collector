#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
의약품 JSON 파일을 통합 MySQL 데이터베이스 테이블로 변환하는 도구

이 스크립트는 단일 medicine_info 테이블에 모든 정보를 통합합니다.
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

# 로깅 설정 (기존과 동일)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("json_to_mysql_merged.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MedicineJsonToMySQLMerged:
    """JSON 파일에서 통합 MySQL 테이블로 의약품 데이터를 변환하는 클래스"""
    
    def __init__(self, host, user, password, database, port=3306, charset='utf8mb4'):
        """
        초기화
        
        Args:
            host: MySQL 호스트
            user: MySQL 사용자
            password: MySQL 비밀번호
            database: MySQL 데이터베이스 이름
        """
        # MySQL 연결 정보
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'charset': charset,
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        # 통계
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'error_count': 0,
            'skipped_count': 0
        }
        
        # 카테고리 캐시
        self.category_cache = {}
        
        logger.info("통합 MySQL 변환기 초기화 완료")
    
    def _create_merged_table(self, cursor):
        """
        통합된 medicine_info 테이블 생성
        """
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicine_info (
            id VARCHAR(20) PRIMARY KEY,
            name_kr VARCHAR(200) NOT NULL,
            name_en VARCHAR(200),
            company VARCHAR(100),
            type VARCHAR(50),
            category VARCHAR(100),
            category_id INT,
            insurance_code VARCHAR(30),
            appearance TEXT,
            shape VARCHAR(50),
            color VARCHAR(50),
            size VARCHAR(50),
            identification VARCHAR(100),
            components TEXT,
            efficacy TEXT,
            precautions TEXT,
            dosage TEXT,
            storage TEXT,
            expiration TEXT,
            
            # 분할선 정보 추가
            division_description TEXT,
            division_type VARCHAR(50),
            
            # 이미지 메타데이터 추가
            image_url VARCHAR(500),
            image_width VARCHAR(10),
            image_height VARCHAR(10),
            image_alt VARCHAR(200),
            
            source_url VARCHAR(500),
            extracted_time DATETIME,
            updated_time DATETIME,
            
            # 인덱스 추가
            INDEX (name_kr),
            INDEX (company),
            INDEX (category),
            INDEX (category_id)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
    
    def process_medicine_data(self, json_dir, batch_size=100):
        """
        JSON 디렉토리의 모든 파일 처리
        """
        # 데이터베이스 연결
        try:
            connection = pymysql.connect(**self.db_config)
            
            with connection.cursor() as cursor:
                # 통합 테이블 생성
                self._create_merged_table(cursor)
                
                # JSON 파일 목록 가져오기
                json_files = glob.glob(os.path.join(json_dir, "*.json"))
                
                for json_file in tqdm(json_files, desc="JSON 파일 처리"):
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            medicine_data = json.load(f)
                        
                        # 데이터 변환 및 삽입
                        self._insert_medicine_data(cursor, medicine_data)
                        
                        # 100개마다 커밋
                        if self.stats['total_processed'] % batch_size == 0:
                            connection.commit()
                    
                    except Exception as e:
                        logger.error(f"파일 처리 오류 ({json_file}): {e}")
                        self.stats['error_count'] += 1
                
                # 최종 커밋
                connection.commit()
        
        except Exception as e:
            logger.error(f"데이터베이스 처리 중 오류: {e}")
            return False
        
        finally:
            if 'connection' in locals():
                connection.close()
        
        return True
    
    def _insert_medicine_data(self, cursor, medicine_data):
        """
        단일 의약품 데이터 삽입
        """
        self.stats['total_processed'] += 1
        
        try:
            # 데이터 변환
            transformed_data = self._transform_medicine_data(cursor, medicine_data)
            
            # 데이터 삽입 쿼리
            insert_query = """
            INSERT INTO medicine_info (
                id, name_kr, name_en, company, type, category, category_id,
                insurance_code, appearance, shape, color, size, 
                identification, components, efficacy, precautions, 
                dosage, storage, expiration, 
                division_description, division_type,
                image_url, image_width, image_height, image_alt,
                source_url, extracted_time, updated_time
            ) VALUES (
                %(id)s, %(name_kr)s, %(name_en)s, %(company)s, %(type)s, 
                %(category)s, %(category_id)s, %(insurance_code)s, 
                %(appearance)s, %(shape)s, %(color)s, %(size)s, 
                %(identification)s, %(components)s, %(efficacy)s, 
                %(precautions)s, %(dosage)s, %(storage)s, %(expiration)s,
                %(division_description)s, %(division_type)s,
                %(image_url)s, %(image_width)s, %(image_height)s, 
                %(image_alt)s, %(source_url)s, %(extracted_time)s, 
                %(updated_time)s
            ) ON DUPLICATE KEY UPDATE 
                name_kr = VALUES(name_kr),
                name_en = VALUES(name_en),
                company = VALUES(company),
                type = VALUES(type),
                category = VALUES(category),
                category_id = VALUES(category_id)
            """
            
            cursor.execute(insert_query, transformed_data)
            self.stats['success_count'] += 1
        
        except Exception as e:
            logger.error(f"데이터 삽입 오류: {e}")
            self.stats['error_count'] += 1
    
    def _transform_medicine_data(self, cursor, medicine_data):
        """
        의약품 데이터 변환 및 정제
        """
        # 카테고리 처리
        category = medicine_data.get('category', '')
        category_id = None
        if category:
            category_id = self._process_category(cursor, category)
        
        # 기본 데이터 변환
        transformed = {
            'id': self._generate_medicine_id(medicine_data),
            'name_kr': medicine_data.get('korean_name', ''),
            'name_en': medicine_data.get('english_name', ''),
            'company': medicine_data.get('company', ''),
            'type': medicine_data.get('type', ''),
            'category': category,
            'category_id': category_id,
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
            
            # 분할선 정보
            'division_description': medicine_data.get('division_info', {}).get('division_description', ''),
            'division_type': medicine_data.get('division_info', {}).get('division_type', ''),
            
            # 이미지 정보
            'image_url': medicine_data.get('image_url', ''),
            'image_width': medicine_data.get('image_width', ''),
            'image_height': medicine_data.get('image_height', ''),
            'image_alt': medicine_data.get('image_alt', ''),
            
            'source_url': medicine_data.get('url', ''),
            'extracted_time': self._parse_datetime(medicine_data.get('extracted_time', '')),
            'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return transformed
    
    def _process_category(self, cursor, category):
        """
        카테고리 처리 및 ID 반환
        """
        # 대괄호 코드 추출 (예: [01140]해열.진통.소염제)
        code_match = re.match(r'\[([^\]]+)\](.*)', category)
        if code_match:
            code = code_match.group(1)
            name = code_match.group(2).strip()
            
            # 캐시 확인
            if code in self.category_cache:
                return self.category_cache[code]
            
            # 데이터베이스에서 확인 또는 삽입
            try:
                cursor.execute(
                    "INSERT IGNORE INTO medicine_categories (category_code, category_name) VALUES (%s, %s)",
                    (code, name)
                )
                
                # 마지막으로 삽입된 ID 또는 기존 ID 가져오기
                cursor.execute(
                    "SELECT id FROM medicine_categories WHERE category_code = %s",
                    (code,)
                )
                result = cursor.fetchone()
                
                if result:
                    category_id = result['id']
                    self.category_cache[code] = category_id
                    return category_id
            
            except Exception as e:
                logger.warning(f"카테고리 처리 오류: {e}")
        
        return None
    
    def _parse_datetime(self, dt_str):
        """날짜/시간 문자열을 MySQL datetime 형식으로 변환"""
        if not dt_str:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # ISO 형식 변환 시도
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _generate_medicine_id(self, medicine_data):
        """의약품 고유 ID 생성"""
        # URL에서 docId 추출 시도
        url = medicine_data.get('url', '')
        doc_id_match = re.search(r'docId=(\d+)', url)
        
        if doc_id_match:
            return f"M{doc_id_match.group(1)}"
        
        # URL이 없으면 이름 + 회사 기반으로 ID 생성
        name = medicine_data.get('korean_name', '')
        company = medicine_data.get('company', '')
        
        id_base = f"{name}_{company}"
        return f"MC{abs(hash(id_base))% 10000000:07d}"
    
    def print_stats(self):
        """처리 통계 출력"""
        print("\n처리 통계:")
        print(f"총 처리된 파일: {self.stats['total_processed']}")
        print(f"성공적으로 삽입된 데이터: {self.stats['success_count']}")
        print(f"오류 발생 데이터: {self.stats['error_count']}")

def main():
    """
    메인 함수 - 스크립트 실행 진입점
    """
    # 배너 출력
    print("""
    ======================================================================
    의약품 데이터 통합 변환기 v1.0
    ======================================================================
    """)
    
    # .env 파일에서 환경 변수 로드
    load_dotenv()
    
    # MySQL 연결 설정
    # 환경 변수 또는 기본값 사용
    mysql_config = {
        'host': os.environ.get('MYSQL_HOST', 'localhost'),
        'user': os.environ.get('MYSQL_USER', 'root'),
        'password': os.environ.get('MYSQL_PASSWORD', '1234'),
        'database': os.environ.get('MYSQL_DATABASE', 'medicine_db'),
        'port': int(os.environ.get('MYSQL_PORT', 3306))
    }
    
    # JSON 데이터 디렉토리 설정
    # 현재 작업 디렉토리 기준 상대 경로
    JSON_DIR = os.path.join(os.getcwd(), 'collected_data', 'json')
    
    # 배치 크기 설정
    BATCH_SIZE = 100
    
    # 연결 정보 출력
    print("MySQL 연결 설정:")
    for key, value in mysql_config.items():
        if key != 'password':  # 보안을 위해 비밀번호는 표시하지 않음
            print(f"  {key}: {value}")
    
    print(f"\nJSON 디렉토리: {JSON_DIR}")
    print(f"배치 크기: {BATCH_SIZE}")
    
    # 통합 변환기 초기화
    converter = MedicineJsonToMySQLMerged(
        host=mysql_config['host'],
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database'],
        port=mysql_config['port']
    )
    
    # JSON 디렉토리 처리
    try:
        print("\nJSON 파일 처리를 시작합니다...")
        success = converter.process_medicine_data(JSON_DIR, BATCH_SIZE)
        
        # 결과 출력
        if success:
            converter.print_stats()
            print("\n데이터 통합 변환이 완료되었습니다.")
        else:
            print("\n데이터 통합 변환 중 오류가 발생했습니다.")
        
        # 로그 파일 경로 안내
        print(f"\n자세한 로그는 {os.path.abspath('json_to_mysql_merged.log')} 파일을 확인하세요.")
    
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.")
    
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}", exc_info=True)
        print(f"\n\n오류 발생: {e}")

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        logger.critical(f"치명적 오류 발생: {e}", exc_info=True)
        sys.exit(1)