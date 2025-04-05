#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
루트 디렉토리에 있는 HTML 파일을 image_reextraction 디렉토리로 이동하는 스크립트
"""

import os
import shutil
import glob
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("move_html")

def move_html_files(base_dir=".", target_dir="image_reextraction"):
    """
    base_dir에 있는 HTML 파일을 target_dir 디렉토리로 이동
    
    Args:
        base_dir: 기본 디렉토리 (기본값: ".", 현재 디렉토리)
        target_dir: 대상 디렉토리 (기본값: "image_reextraction")
    
    Returns:
        int: 이동된 파일 수
    """
    # 대상 디렉토리 경로 구성 (상대 경로인 경우 절대 경로로 변환)
    if not os.path.isabs(target_dir):
        target_dir = os.path.abspath(os.path.join(base_dir, target_dir))
    
    # 대상 디렉토리가 없으면 생성
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        logger.info(f"대상 디렉토리 생성됨: {target_dir}")
    
    # base_dir에서 HTML 파일 찾기 (하위 디렉토리 제외)
    html_files = glob.glob(os.path.join(base_dir, "*.html")) + glob.glob(os.path.join(base_dir, "*.htm"))
    
    if not html_files:
        logger.info(f"{base_dir} 디렉토리에 HTML 파일이 없습니다.")
        return 0
    
    # 각 HTML 파일 이동
    moved_count = 0
    for html_file in html_files:
        # 대상 디렉토리 내부의 파일은 건너뛰기
        if os.path.dirname(os.path.abspath(html_file)) == target_dir:
            logger.debug(f"건너뜀 (이미 대상 디렉토리에 있음): {html_file}")
            continue
            
        filename = os.path.basename(html_file)
        destination = os.path.join(target_dir, filename)
        
        try:
            # 이미 동일한 파일이 있는지 확인
            if os.path.exists(destination):
                # 백업 파일명 생성 (타임스탬프 추가)
                name_parts = os.path.splitext(filename)
                backup_filename = f"{name_parts[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{name_parts[1]}"
                destination = os.path.join(target_dir, backup_filename)
                logger.info(f"동일한 파일명이 존재하여 새 이름으로 저장: {backup_filename}")
            
            # 파일 이동
            shutil.move(html_file, destination)
            logger.info(f"파일 이동 완료: {html_file} -> {destination}")
            moved_count += 1
            
        except Exception as e:
            logger.error(f"파일 이동 중 오류 발생: {html_file} -> {e}")
    
    logger.info(f"총 {moved_count}개 HTML 파일이 {target_dir} 디렉토리로 이동되었습니다.")
    return moved_count

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='HTML 파일 이동 스크립트')
    parser.add_argument('--src', default='.', help='소스 디렉토리 (기본값: 현재 디렉토리)')
    parser.add_argument('--dst', default='image_reextraction', help='대상 디렉토리 (기본값: image_reextraction)')
    
    args = parser.parse_args()
    
    print(f"{args.src} 디렉토리의 HTML 파일을 {args.dst} 디렉토리로 이동합니다...")
    moved = move_html_files(args.src, args.dst)
    
    if moved > 0:
        print(f"\n이동 완료! {moved}개 HTML 파일이 {args.dst} 디렉토리로 이동되었습니다.")
    else:
        print(f"\n이동할 HTML 파일이 없습니다.")