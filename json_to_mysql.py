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
        logging.StreamHandler(sys.stdout)  # 콘솔에도 로그 출력
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
            logger.info(f"MySQL 데이터베이스 '{self.database}'에 연결 시도 중 (호스트: {self.host}, 포트: {self.port}, 사용자: {self.user})")
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
            print(f"MySQL 연결 오류: {e}")  # 콘솔에도 출력
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
            error_msg = f"유효하지 않은 디렉토리: {json_dir}"
            logger.error(error_msg)
            print(error_msg)  # 콘솔에도 출력
            return self.stats
        
        # JSON 파일 목록 가져오기
        json_files = glob.glob(os.path.join(json_dir, "*.json"))
        total_files = len(json_files)
        
        if total_files == 0:
            warning_msg = f"디렉토리 '{json_dir}'에 JSON 파일이 없습니다."
            logger.warning(warning_msg)
            print(warning_msg)  # 콘솔에도 출력
            return self.stats
        
        logger.info(f"총 {total_files}개의 JSON 파일을 처리합니다.")
        print(f"총 {total_files}개의 JSON 파일을 처리합니다.")  # 콘솔에도 출력
        
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
                    error_msg = f"파일 처리 오류 ({json_file}): {e}"
                    logger.error(error_msg)
                    print(error_msg)  # 콘솔에도 출력
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
            
            # 콘솔에도 출력
            print("\nJSON 파일 처리 완료")
            print(f"처리된 파일: {self.stats['total_processed']}개")
            print(f"성공: {self.stats['success_count']}개")
            print(f"오류: {self.stats['error_count']}개")
            print(f"중복 스킵: {self.stats['skipped_count']}개")
        
        except Exception as e:
            error_msg = f"일괄 처리 중 오류: {e}"
            logger.error(error_msg)
            print(error_msg)  # 콘솔에도 출력
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
                error_msg = f"데이터 처리 오류 ({medicine_data.get('korean_name', '이름 없음')}): {e}"
                logger.error(error_msg)
                print(error_msg)  # 콘솔에도 출력
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
            print(f"medicine_info 테이블 삽입 오류: {e}")  # 콘솔에도 출력
            return False
    
    def _insert_division_info(self, medicine_id, division_info):
        """
        medicine_division 테이블에 데이터 삽입 (개선된 버전)
        
        Args:
            medicine_id: 의약품 ID
            division_info: 분할선 정보
        """
        try:
            # 데이터 형식 확인
            if not isinstance(division_info, dict):
                return
            
            description = division_info.get('division_description', '')
            division_type = division_info.get('division_type', '')
            
            if not description:
                return
            
            # 중복 확인
            self.cursor.execute(
                "SELECT id FROM medicine_division WHERE medicine_id = %s",
                (medicine_id,)
            )
            
            if self.cursor.fetchone():
                # 이미 존재하는 경우 업데이트
                self.cursor.execute(
                    "UPDATE medicine_division SET division_description = %s, division_type = %s WHERE medicine_id = %s",
                    (description, division_type, medicine_id)
                )
            else:
                # 새로 삽입
                self.cursor.execute(
                    "INSERT INTO medicine_division (medicine_id, division_description, division_type) VALUES (%s, %s, %s)",
                    (medicine_id, description, division_type)
                )
        
        except Exception as e:
            logger.warning(f"분할선 정보 삽입 오류: {e}")
            print(f"분할선 정보 삽입 오류: {e}")
    
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
            print(f"이미지 메타데이터 삽입 오류: {e}")  # 콘솔에도 출력
    
    @staticmethod
    def print_banner():
        """프로그램 시작 배너 출력"""
        banner = r"""
        ======================================================================
        의약품 JSON → MySQL 변환기 v1.0
        ======================================================================
        """
        print(banner)

def check_mysql_tables():
    """
    필요한 MySQL 테이블이 있는지 확인하고 없으면 생성
    """
    try:
        # MySQL 연결
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cursor = conn.cursor()
        
        # medicine_info 테이블 확인/생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicine_info (
            id VARCHAR(20) PRIMARY KEY,
            name_kr VARCHAR(200) NOT NULL,
            name_en VARCHAR(200),
            company VARCHAR(100),
            type VARCHAR(50),
            category VARCHAR(100),
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
            image_url VARCHAR(500),
            source_url VARCHAR(500),
            extracted_time DATETIME,
            updated_time DATETIME,
            INDEX (name_kr),
            INDEX (company),
            INDEX (category)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        
        # medicine_categories 테이블 확인/생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicine_categories (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category_code VARCHAR(20) NOT NULL,
            category_name VARCHAR(100) NOT NULL,
            UNIQUE KEY (category_code),
            INDEX (category_name)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        
        # medicine_division 테이블 확인/생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicine_division (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medicine_id VARCHAR(20) NOT NULL,
            division_description TEXT,
            FOREIGN KEY (medicine_id) REFERENCES medicine_info(id) ON DELETE CASCADE,
            INDEX (medicine_id)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        
        # medicine_images 테이블 확인/생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS medicine_images (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medicine_id VARCHAR(20) NOT NULL,
            image_url VARCHAR(500) NOT NULL,
            image_width VARCHAR(10),
            image_height VARCHAR(10),
            image_alt VARCHAR(200),
            FOREIGN KEY (medicine_id) REFERENCES medicine_info(id) ON DELETE CASCADE,
            INDEX (medicine_id)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        
        conn.commit()
        
        print("MySQL 테이블이 확인/생성되었습니다.")
        return True
        
    except Exception as e:
        print(f"MySQL 테이블 확인/생성 중 오류: {e}")
        return False
        
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def main():
    """
    메인 함수 - IDE에서 Run 버튼으로 직접 실행 가능
    """
    # 배너 출력
    MedicineJsonToMySQL.print_banner()
    
    # .env 파일에서 환경 변수 로드
    load_dotenv()
    
    print("\n스크립트 시작: 환경 변수를 확인하고 있습니다...")
    
    # MySQL 연결 정보 설정 (하드코딩)
    global MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    
    # 1. 환경 변수에서 값 로드 시도
    MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
    MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '1234')
    MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'medicine_db')
    
    # 2. 하드코딩된 값으로 덮어쓰기 (필요시 수정)
    MYSQL_HOST = 'localhost'      # MySQL 호스트 주소
    MYSQL_PORT = 3306             # MySQL 포트
    MYSQL_USER = 'root'           # MySQL 사용자 이름
    MYSQL_PASSWORD = '1234'       # MySQL 비밀번호
    MYSQL_DATABASE = 'medicine_db'  # MySQL 데이터베이스 이름
    
    # JSON 디렉토리 설정 (하드코딩)
    # 현재 작업 디렉토리 기준 상대 경로 또는 절대 경로 사용
    JSON_DIR = os.path.join(os.getcwd(), 'collected_data', 'json')
    
    # 또는 절대 경로 지정 (필요시 수정)
    # JSON_DIR = r"C:\Users\qkrtj\medicine_collector\collected_data\json"
    
    # 배치 크기 설정
    BATCH_SIZE = 100
    
    # 사용자에게 현재 설정 보여주기
    print(f"MySQL 연결 정보:")
    print(f"  호스트: {MYSQL_HOST}")
    print(f"  포트: {MYSQL_PORT}")
    print(f"  사용자: {MYSQL_USER}")
    print(f"  데이터베이스: {MYSQL_DATABASE}")
    print(f"\nJSON 디렉토리: {JSON_DIR}")
    print(f"배치 크기: {BATCH_SIZE}")
    
    # MySQL 테이블 확인/생성
    print("\nMySQL 테이블을 확인/생성합니다...")
    if not check_mysql_tables():
        print("MySQL 테이블 확인/생성에 실패했습니다. 프로그램을 종료합니다.")
        return 1
    
    # 변환기 초기화 및 실행
    converter = MedicineJsonToMySQL(
        host=MYSQL_HOST,
        user=MYSQL_USER, 
        password=MYSQL_PASSWORD, 
        database=MYSQL_DATABASE, 
        port=MYSQL_PORT
    )
    
    print("\nJSON 파일 처리를 시작합니다...")
    
    try:
        stats = converter.process_json_dir(JSON_DIR, BATCH_SIZE)
        
        # 결과 요약
        print("\nJSON → MySQL 변환 완료:")
        print(f"처리된 파일: {stats['total_processed']}개")
        print(f"성공: {stats['success_count']}개")
        print(f"오류: {stats['error_count']}개")
        print(f"중복 스킵: {stats['skipped_count']}개")
        
        # 로그 파일 경로 안내
        print(f"\n자세한 로그는 {os.path.abspath('json_to_mysql.log')} 파일을 확인하세요.")
        
        print("\n완료되었습니다. 아무 키나 눌러 종료하세요...")
        return 0
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.")
        return 1
        
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}", exc_info=True)
        print(f"\n\n오류 발생: {e}")
        return 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        logger.critical(f"치명적 오류 발생: {e}", exc_info=True)
        sys.exit(1)
    finally:
        print("\n프로그램을 종료합니다. 엔터 키를 눌러주세요...")
        input()