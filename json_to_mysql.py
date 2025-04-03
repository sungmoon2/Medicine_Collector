#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
간소화된 데이터 마이그레이션 스크립트 - JSON 및 SQLite에서 MySQL로 데이터 이전
명령줄 인자 없이 실행 가능
"""

import os
import json
import sqlite3
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 기본 설정값
DEFAULT_CONFIG = {
    # MySQL 연결 설정
    'mysql_host': 'localhost',
    'mysql_user': 'root',
    'mysql_password': '1234',  # 기본 비밀번호, 변경 필요
    'mysql_database': 'medicine_db',
    
    # SQLite 설정
    'sqlite_path': 'medicine_database.sqlite',
    
    # JSON 설정
    'json_dir': './json_data',  # 현재 디렉토리의 json_data 폴더
    'table_name': 'abcdefg_medicine_data',  # 모든 의약품 데이터를 저장할 테이블 이름
    
    # 마이그레이션 타입 ('sqlite', 'json', 'both')
    'migration_type': 'both'
}

def connect_mysql(host, user, password, database):
    """
    MySQL 데이터베이스 연결
    
    Args:
        host (str): 호스트 이름
        user (str): 사용자 이름
        password (str): 비밀번호
        database (str): 데이터베이스 이름
        
    Returns:
        tuple: (연결 객체, 커서 객체)
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=1234,
            charset='utf8mb4',
            use_unicode=True
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # 데이터베이스가 없으면 생성
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE `{database}`")
            
            logger.info(f"MySQL 서버 {host}의 {database} 데이터베이스에 연결되었습니다.")
            return connection, cursor
            
    except Error as e:
        logger.error(f"MySQL 연결 오류: {e}")
        return None, None

def connect_sqlite(db_path):
    """
    SQLite 데이터베이스 연결
    
    Args:
        db_path (str): 데이터베이스 파일 경로
        
    Returns:
        tuple: (연결 객체, 커서 객체)
    """
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        logger.info(f"SQLite 데이터베이스 {db_path}에 연결되었습니다.")
        return connection, cursor
    except Error as e:
        logger.error(f"SQLite 연결 오류: {e}")
        return None, None

def get_sqlite_tables(cursor):
    """
    SQLite 데이터베이스의 모든 테이블 이름 가져오기
    
    Args:
        cursor (sqlite3.Cursor): SQLite 커서 객체
        
    Returns:
        list: 테이블 이름 목록
    """
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [table[0] for table in cursor.fetchall()]

def get_sqlite_table_schema(cursor, table_name):
    """
    SQLite 테이블 스키마 정보 가져오기
    
    Args:
        cursor (sqlite3.Cursor): SQLite 커서 객체
        table_name (str): 테이블 이름
        
    Returns:
        list: 컬럼 정보 목록
    """
    cursor.execute(f"PRAGMA table_info({table_name});")
    return cursor.fetchall()

def create_mysql_table(cursor, table_name, columns):
    """
    MySQL에 테이블 생성
    
    Args:
        cursor (mysql.connector.cursor): MySQL 커서 객체
        table_name (str): 테이블 이름
        columns (list): 컬럼 정보 목록
    """
    # SQLite에서 MySQL로 데이터 타입 변환
    type_mapping = {
        'INTEGER': 'INT',
        'TEXT': 'LONGTEXT',  # TEXT를 LONGTEXT로 변경하여 큰 텍스트 필드 지원
        'REAL': 'DOUBLE',
        'BLOB': 'BLOB',
        'NULL': 'NULL'
    }
    
    # 컬럼 정의 생성
    column_defs = []
    primary_key = None
    
    for col in columns:
        name = col[1]
        data_type = col[2].upper()
        not_null = "NOT NULL" if col[3] == 1 else "NULL"
        
        # 기본키 확인
        if col[5] == 1:
            primary_key = name
        
        # MySQL 데이터 타입으로 변환
        mysql_type = type_mapping.get(data_type, 'TEXT')
        
        column_defs.append(f"`{name}` {mysql_type} {not_null}")
    
    # 기본키 추가
    if primary_key:
        column_defs.append(f"PRIMARY KEY (`{primary_key}`)")
    else:
        # 기본키가 없으면 id 컬럼 추가
        column_defs.insert(0, "id INT AUTO_INCREMENT PRIMARY KEY")
    
    # 테이블 생성 쿼리
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(column_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    try:
        cursor.execute(create_table_query)
        logger.info(f"MySQL 테이블 '{table_name}' 생성 완료")
    except Error as e:
        logger.error(f"테이블 생성 오류: {e}")

def migrate_sqlite_to_mysql(sqlite_path, mysql_config):
    """
    SQLite 데이터베이스를 MySQL로 마이그레이션
    
    Args:
        sqlite_path (str): SQLite 데이터베이스 파일 경로
        mysql_config (dict): MySQL 연결 설정
    """
    logger.info(f"SQLite에서 MySQL로 마이그레이션 시작: {sqlite_path}")
    
    # SQLite 파일이 존재하는지 확인
    if not os.path.exists(sqlite_path):
        logger.error(f"SQLite 파일을 찾을 수 없습니다: {sqlite_path}")
        print(f"SQLite 파일을 찾을 수 없습니다: {sqlite_path}")
        return
    
    # SQLite 연결
    sqlite_conn, sqlite_cursor = connect_sqlite(sqlite_path)
    if not sqlite_conn:
        return
    
    # MySQL 연결
    mysql_conn, mysql_cursor = connect_mysql(**mysql_config)
    if not mysql_conn:
        sqlite_conn.close()
        return
    
    try:
        # SQLite 테이블 목록 가져오기
        tables = get_sqlite_tables(sqlite_cursor)
        logger.info(f"마이그레이션할 테이블: {tables}")
        
        # 각 테이블에 대해 마이그레이션 수행
        for table in tables:
            # 시스템 테이블 건너뛰기
            if table.startswith('sqlite_'):
                continue
                
            logger.info(f"테이블 '{table}' 마이그레이션 중...")
            
            # 테이블 스키마 가져오기
            columns = get_sqlite_table_schema(sqlite_cursor, table)
            
            # MySQL에 테이블 생성
            create_mysql_table(mysql_cursor, table, columns)
            
            # 데이터 가져오기
            sqlite_cursor.execute(f"SELECT * FROM {table}")
            rows = sqlite_cursor.fetchall()
            
            if not rows:
                logger.info(f"테이블 '{table}'에 데이터가 없습니다.")
                continue
                
            # 컬럼 이름 가져오기
            column_names = [column[1] for column in columns]
            
            # 데이터 삽입 쿼리 생성
            placeholders = ', '.join(['%s'] * len(column_names))
            insert_query = f"INSERT INTO `{table}` (`{'`, `'.join(column_names)}`) VALUES ({placeholders})"
            
            # 배치 삽입 준비
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                batch_values = []
                
                for row in batch:
                    # row가 sqlite3.Row 타입인 경우 dict로 변환
                    if isinstance(row, sqlite3.Row):
                        row_dict = dict(row)
                        row_values = [row_dict[col] for col in column_names]
                    else:
                        row_values = list(row)
                        
                    batch_values.append(row_values)
                
                # 데이터 삽입 실행
                mysql_cursor.executemany(insert_query, batch_values)
                mysql_conn.commit()
                
                logger.info(f"테이블 '{table}'에 {len(batch)} 행 삽입 완료 ({i+1}~{i+len(batch)}/{len(rows)})")
            
            logger.info(f"테이블 '{table}' 마이그레이션 완료")
    
    except Exception as e:
        logger.error(f"SQLite에서 MySQL로 마이그레이션 중 오류 발생: {e}")
    
    finally:
        # 연결 종료
        if sqlite_conn:
            sqlite_conn.close()
        if mysql_conn:
            mysql_conn.close()
        
        logger.info("SQLite에서 MySQL로 마이그레이션 종료")

def migrate_json_to_mysql(json_dir, mysql_config, table_name):
    """
    JSON 파일을 MySQL로 마이그레이션하고 새 컬럼 발견 시 테이블 구조 업데이트
    
    Args:
        json_dir (str): JSON 파일이 있는 디렉토리 경로
        mysql_config (dict): MySQL 연결 설정
        table_name (str): 테이블 이름
    """
    logger.info(f"JSON에서 MySQL로 마이그레이션 시작: {json_dir}")
    
    # 디렉토리가 존재하는지 확인
    if not os.path.exists(json_dir):
        logger.error(f"JSON 디렉토리를 찾을 수 없습니다: {json_dir}")
        print(f"JSON 디렉토리를 찾을 수 없습니다: {json_dir}")
        return
    
    # MySQL 연결
    mysql_conn, mysql_cursor = connect_mysql(**mysql_config)
    if not mysql_conn:
        return
    
    # 현재 테이블에 있는 컬럼 목록 추적 변수
    existing_columns = set()
    
    try:
        # 테이블이 이미 존재하는지 확인
        mysql_cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = mysql_cursor.fetchone() is not None
        
        if table_exists:
            # 기존 테이블의 컬럼 정보 가져오기
            mysql_cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            for column in mysql_cursor.fetchall():
                existing_columns.add(column[0])
            logger.info(f"기존 테이블 '{table_name}'에서 {len(existing_columns)}개 컬럼 발견")
        
        # JSON 파일 목록 가져오기
        json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
        logger.info(f"마이그레이션할 JSON 파일: {json_files}")
        
        if not json_files:
            logger.warning(f"JSON 파일을 찾을 수 없습니다: {json_dir}")
            print(f"JSON 파일을 찾을 수 없습니다: {json_dir}")
            return
        
        for json_file in json_files:
            file_path = os.path.join(json_dir, json_file)
            logger.info(f"파일 '{json_file}'을 테이블 '{table_name}'로 마이그레이션 중...")
            
            # JSON 파일 로드
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"JSON 파일 '{json_file}' 로드 중 오류: {e}")
                print(f"JSON 파일 '{json_file}' 로드 중 오류: {e}")
                continue
            
            if not data:
                logger.info(f"파일 '{json_file}'에 데이터가 없습니다.")
                continue
            
            # 데이터프레임으로 변환
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.json_normalize(data)
            else:
                logger.warning(f"파일 '{json_file}'의 데이터 형식을 처리할 수 없습니다.")
                continue
            
            # 컬럼 이름 정리
            df.columns = [col.replace('.', '_').replace(' ', '_').lower() for col in df.columns]
            
            if not table_exists:
                # 테이블이 없으면 새로 생성
                column_defs = []
                for col in df.columns:
                    dtype = df[col].dtype
                    
                    if pd.api.types.is_integer_dtype(dtype):
                        column_defs.append(f"`{col}` INT")
                    elif pd.api.types.is_float_dtype(dtype):
                        column_defs.append(f"`{col}` DOUBLE")
                    elif pd.api.types.is_bool_dtype(dtype):
                        column_defs.append(f"`{col}` BOOLEAN")
                    elif col == '사용상의주의사항' or '설명' in col or '정보' in col:
                        column_defs.append(f"`{col}` LONGTEXT")
                    else:
                        column_defs.append(f"`{col}` TEXT")
                    
                    existing_columns.add(col)
                
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {', '.join(column_defs)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """
                
                mysql_cursor.execute(create_table_query)
                mysql_conn.commit()
                logger.info(f"테이블 '{table_name}' 생성 완료")
                table_exists = True
            else:
                # 테이블이 이미 존재하면 새 컬럼만 추가
                for col in df.columns:
                    if col not in existing_columns:
                        dtype = df[col].dtype
                        
                        if pd.api.types.is_integer_dtype(dtype):
                            column_type = "INT"
                        elif pd.api.types.is_float_dtype(dtype):
                            column_type = "DOUBLE"
                        elif pd.api.types.is_bool_dtype(dtype):
                            column_type = "BOOLEAN"
                        elif col == '사용상의주의사항' or '설명' in col or '정보' in col:
                            column_type = "LONGTEXT"
                        else:
                            column_type = "TEXT"
                        
                        alter_query = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {column_type}"
                        
                        try:
                            mysql_cursor.execute(alter_query)
                            mysql_conn.commit()
                            logger.info(f"새 컬럼 '{col}' 추가됨")
                            existing_columns.add(col)
                        except Error as e:
                            logger.error(f"컬럼 추가 중 오류: {e}")
            
            # 데이터 삽입 준비
            # 현재 테이블에 있는 컬럼만 선택
            available_columns = [col for col in df.columns if col in existing_columns]
            if not available_columns:
                logger.warning(f"파일 '{json_file}'에서 테이블 '{table_name}'에 삽입할 컬럼이 없습니다.")
                continue
            
            # 삽입 쿼리 생성
            placeholders = ', '.join(['%s'] * len(available_columns))
            insert_query = f"INSERT INTO `{table_name}` (`{'`, `'.join(available_columns)}`) VALUES ({placeholders})"
            
            # NULL 값 처리
            df_insert = df[available_columns].where(pd.notnull(df[available_columns]), None)
            
            # 배치 삽입
            batch_size = 1000
            total_rows = len(df_insert)
            
            for i in range(0, total_rows, batch_size):
                batch = df_insert.iloc[i:i+batch_size]
                batch_values = [tuple(row) for row in batch.values]
                
                try:
                    mysql_cursor.executemany(insert_query, batch_values)
                    mysql_conn.commit()
                    logger.info(f"테이블 '{table_name}'에 {len(batch)} 행 삽입 완료 ({i+1}~{i+len(batch)}/{total_rows})")
                except Error as e:
                    logger.error(f"데이터 삽입 중 오류: {e}")
            
            logger.info(f"파일 '{json_file}'을 테이블 '{table_name}'로 마이그레이션 완료")
    
    except Exception as e:
        logger.error(f"JSON에서 MySQL로 마이그레이션 중 오류 발생: {e}")
    
    finally:
        # 연결 종료
        if mysql_conn:
            mysql_conn.close()
        
        logger.info("JSON에서 MySQL로 마이그레이션 종료")

def main():
    print("의약품 데이터 MySQL 마이그레이션 스크립트 실행 중...")
    print("=" * 50)
    
    # 기본 설정 사용
    config = DEFAULT_CONFIG
    
    # 현재 디렉토리에 JSON 디렉토리 설정
    json_dir = os.path.join(os.getcwd(), 'json_data')
    if os.path.exists(json_dir):
        config['json_dir'] = json_dir
    else:
        # 하위 디렉토리 검색
        for root, dirs, files in os.walk(os.getcwd()):
            for dir_name in dirs:
                if 'json' in dir_name.lower():
                    potential_dir = os.path.join(root, dir_name)
                    json_files = [f for f in os.listdir(potential_dir) if f.endswith('.json')]
                    if json_files:
                        config['json_dir'] = potential_dir
                        print(f"JSON 디렉토리를 찾았습니다: {potential_dir}")
                        break
    
    # SQLite 파일 찾기
    sqlite_path = os.path.join(os.getcwd(), 'medicine_database.sqlite')
    if os.path.exists(sqlite_path):
        config['sqlite_path'] = sqlite_path
    else:
        # SQLite 파일 검색
        for root, dirs, files in os.walk(os.getcwd()):
            for file in files:
                if file.endswith('.sqlite') or file.endswith('.db'):
                    config['sqlite_path'] = os.path.join(root, file)
                    print(f"SQLite 데이터베이스를 찾았습니다: {config['sqlite_path']}")
                    break
    
    # MySQL 연결 설정
    mysql_config = {
        'host': config['mysql_host'],
        'user': config['mysql_user'],
        'password': config['mysql_password'],
        'database': config['mysql_database']
    }
    
    # 설정 정보 출력
    print(f"MySQL 호스트: {mysql_config['host']}")
    print(f"MySQL 사용자: {mysql_config['user']}")
    print(f"MySQL 데이터베이스: {mysql_config['database']}")
    print(f"SQLite 경로: {config['sqlite_path']}")
    print(f"JSON 디렉토리: {config['json_dir']}")
    print(f"테이블 이름: {config['table_name']}")
    print("=" * 50)
    
    # 사용자 확인
    confirmation = input("위 설정으로 마이그레이션을 진행하시겠습니까? (y/n): ")
    if confirmation.lower() != 'y':
        print("마이그레이션이 취소되었습니다.")
        return
    
    # 마이그레이션 수행
    if config['migration_type'] in ['sqlite', 'both']:
        if os.path.exists(config['sqlite_path']):
            print("SQLite에서 MySQL로 마이그레이션 시작...")
            migrate_sqlite_to_mysql(config['sqlite_path'], mysql_config)
        else:
            print(f"SQLite 파일을 찾을 수 없습니다: {config['sqlite_path']}")
    
    if config['migration_type'] in ['json', 'both']:
        if os.path.exists(config['json_dir']):
            print("JSON에서 MySQL로 마이그레이션 시작...")
            migrate_json_to_mysql(config['json_dir'], mysql_config, config['table_name'])
        else:
            print(f"JSON 디렉토리를 찾을 수 없습니다: {config['json_dir']}")
    
    print("마이그레이션이 완료되었습니다.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        print(f"오류 발생: {e}")
    finally:
        input("Enter 키를 눌러 종료하세요...")