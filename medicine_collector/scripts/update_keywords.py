import sys
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
