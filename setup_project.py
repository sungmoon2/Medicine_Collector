import os
import sys

def create_project_structure(base_path=None):
    """
    의약품 수집기 프로젝트 구조 생성
    
    Args:
        base_path (str, optional): 프로젝트 루트 경로. 
                                   None일 경우 현재 디렉토리에 생성.
    """
    # 기본 경로 설정
    if base_path is None:
        base_path = os.getcwd()
    
    # 프로젝트 디렉토리 구조
    directories = [
        'src',
        'scripts',
        'data/collected_data/json',
        'data/collected_data/html',
        'data/logs'
    ]
    
    # 파일 목록 (경로, 내용)
    files = [
        # src 디렉토리
        ('src/__init__.py', '# medicine collector package'),
        ('src/medicine_collector.py', '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
네이버 검색 API를 활용한 의약품 정보 수집기
"""

import os
import sys
import requests
import json
import time
import re
import csv
import argparse
import random
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
from dotenv import load_dotenv

# 여기에 기존 medicine_collector.py 코드 복사
'''),
        
        # scripts 디렉토리
        ('scripts/run_collector.py', '''import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.medicine_collector import MedicineCollector
import os
from dotenv import load_dotenv

def main():
    # 환경변수 로드
    load_dotenv()

    # 데이터 저장 디렉토리 설정
    data_dir = os.path.join(project_root, 'data', 'collected_data')

    # 네이버 API 인증 정보 로드
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')

    # 수집기 초기화
    collector = MedicineCollector(
        client_id, 
        client_secret, 
        output_dir=data_dir
    )

    # 의약품 수집 실행
    collector.collect_medicines()

if __name__ == "__main__":
    main()
'''),
        ('scripts/update_keywords.py', '''import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from src.medicine_collector import MedicineCollector
import os
from dotenv import load_dotenv

def main():
    # 환경변수 로드
    load_dotenv()

    # 데이터 저장 디렉토리 설정
    data_dir = os.path.join(project_root, 'data', 'collected_data')

    # 네이버 API 인증 정보 로드
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')

    # 수집기 초기화
    collector = MedicineCollector(
        client_id, 
        client_secret, 
        output_dir=data_dir
    )

    # 새로운 키워드 생성
    collector.generate_additional_keywords()

if __name__ == "__main__":
    main()
'''),
        
        # 루트 디렉토리
        ('requirements.txt', '''requests
beautifulsoup4
python-dotenv
tqdm
'''),
        ('.env', '''NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret
'''),
        ('.gitignore', '''# Python
__pycache__/
*.py[cod]
*$py.class

# 가상환경
venv/
env/

# 데이터 파일
data/collected_data/
*.log

# 환경변수
.env

# IDE
.vscode/
.idea/
''')
    ]
    
    # 디렉토리 생성
    for dir_path in directories:
        full_path = os.path.join(base_path, dir_path)
        os.makedirs(full_path, exist_ok=True)
        print(f"디렉토리 생성: {full_path}")
    
    # 파일 생성
    for file_path, content in files:
        full_path = os.path.join(base_path, file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"파일 생성: {full_path}")

def main():
    """
    프로젝트 구조 생성 메인 함수
    """
    # 프로젝트 이름 입력 받기
    print("의약품 정보 수집기 프로젝트 구조 생성")
    
    # 프로젝트 경로 선택
    while True:
        project_name = input("프로젝트 디렉토리 이름을 입력하세요 (기본값: medicine_collector): ").strip()
        
        if not project_name:
            project_name = "medicine_collector"
        
        # 절대 경로로 변환
        project_path = os.path.abspath(project_name)
        
        # 디렉토리 존재 확인
        if os.path.exists(project_path):
            overwrite = input(f"'{project_path}' 디렉토리가 이미 존재합니다. 계속 진행할까요? (y/n): ").lower()
            if overwrite != 'y':
                continue
        
        break
    
    try:
        # 프로젝트 구조 생성
        create_project_structure(project_path)
        
        print("\n프로젝트 구조 생성 완료!")
        print(f"프로젝트 경로: {project_path}")
        print("\n다음 단계:")
        print("1. cd " + project_path)
        print("2. python -m venv venv")
        print("3. source venv/bin/activate  # macOS/Linux")
        print("4. venv\\Scripts\\activate    # Windows")
        print("5. pip install -r requirements.txt")
        print("6. 네이버 개발자 센터(https://developers.naver.com/)에서 '검색' API 애플리케이션 등록")
        print("7. .env 파일에 발급받은 Client ID와 Client Secret 입력")
        print("\n상세 가이드:")
        print("- 네이버 개발자 센터 가입 및 애플리케이션 등록 절차:")
        print("  a) 네이버 계정으로 로그인")
        print("  b) '애플리케이션 등록' 메뉴 선택")
        print("  c) 애플리케이션 이름, 사용 API(검색 API) 선택")
        print("  d) Client ID와 Client Secret 확인")
        print("\n주의: API 사용에 대한 이용 약관과 할당량을 반드시 확인하세요.")
        
    except Exception as e:
        print(f"프로젝트 구조 생성 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()