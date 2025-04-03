#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
환경 설정 및 상수
"""

# 사용자 에이전트 목록
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/111.0.1661.54 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0"
]

# API 관련 설정
API_CONFIG = {
    "MAX_RETRIES": 3,           # 최대 재시도 횟수
    "TIMEOUT": 10,              # API 요청 타임아웃 (초)
    "DAILY_LIMIT": 25000,       # 일일 API 요청 한도
    "MIN_REQUEST_INTERVAL": 0.5 # 최소 요청 간격 (초)
}

# 병렬 처리 관련 설정
PARALLEL_CONFIG = {
    "MAX_WORKERS": 4,           # 최대 병렬 작업자 수
    "MAX_PARALLEL_KEYWORDS": 10 # 최대 병렬 처리 키워드 수
}

# 파일 및 경로 관련 설정
FILE_CONFIG = {
    "HTML_ITEM_LIMIT": 100,     # HTML 파일당 최대 항목 수
    "CSV_BATCH_SIZE": 500       # CSV 내보내기 배치 크기
}

# 웹 요청 관련 설정
HTTP_CONFIG = {
    "MIN_PAGE_INTERVAL": 1.0,   # 페이지 요청 최소 간격 (초)
    "PAGE_TIMEOUT": 15          # 페이지 요청 타임아웃 (초)
}

# 키워드 생성 관련 설정
KEYWORD_CONFIG = {
    "MAX_NEW_KEYWORDS": 100,    # 최대 새 키워드 수
    "SIMILARITY_THRESHOLD": 0.8 # 유사도 임계값
}