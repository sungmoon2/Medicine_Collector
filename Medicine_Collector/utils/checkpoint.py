#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
체크포인트 관리
"""

import os
import json
import logging
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)

def save_checkpoint(keyword, processed_count=0, stats=None, output_dir="collected_data"):
    """
    현재 진행 상태 체크포인트 저장
    
    Args:
        keyword (str): 현재 처리 중인 키워드
        processed_count (int): 현재 키워드에서 처리한 항목 수
        stats (dict): 수집 통계
        output_dir (str): 출력 디렉토리
    """
    # 체크포인트 경로 명시적으로 정의
    checkpoint_path = os.path.join(output_dir, "checkpoint.json")
    
    # 통계 정보가 없으면 빈 딕셔너리 사용
    if stats is None:
        stats = {
            'total_searches': 0,
            'total_found': 0,
            'total_saved': 0,
            'failed_items': 0
        }
    
    checkpoint_data = {
        "current_keyword": keyword,
        "processed_count": processed_count,
        "total_searches": stats.get('total_searches', 0),
        "total_found": stats.get('total_found', 0),
        "total_saved": stats.get('total_saved', 0),
        "failed_items": stats.get('failed_items', 0),
        "timestamp": datetime.now().isoformat()
    }
    
    with open(checkpoint_path, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"체크포인트 저장 완료: {keyword} (처리 항목: {processed_count}개)")

def load_checkpoint(output_dir="collected_data"):
    """
    마지막 체크포인트 로드
    
    Args:
        output_dir (str): 출력 디렉토리
    
    Returns:
        dict or None: 체크포인트 데이터
    """
    checkpoint_path = os.path.join(output_dir, "checkpoint.json")
    
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                logger.info(f"체크포인트 로드됨: {checkpoint['current_keyword']} (최종 업데이트: {checkpoint['timestamp']})")
                return checkpoint
        except Exception as e:
            logger.error(f"체크포인트 로드 오류: {e}")
            # 손상된 체크포인트 파일 백업
            backup_path = f"{checkpoint_path}.backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
            try:
                os.rename(checkpoint_path, backup_path)
                logger.info(f"손상된 체크포인트 파일 백업: {backup_path}")
            except:
                pass
    
    return None