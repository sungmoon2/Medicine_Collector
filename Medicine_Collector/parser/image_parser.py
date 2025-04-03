#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
의약품 이미지 정보 파싱 모듈 - 정확한 태그에서만 추출
개선된 로깅 기능 포함
"""

import re
import urllib.parse
import logging

# 로거 설정
logger = logging.getLogger(__name__)

def extract_medicine_image(soup, medicine_id=None, medicine_name=None):
    """
    의약품 이미지 정보 추출 - 정확한 태그에서만 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        medicine_id (str, optional): 의약품 ID (로깅용)
        medicine_name (str, optional): 의약품 이름 (로깅용)
            
    Returns:
        dict: 이미지 데이터 또는 빈 딕셔너리(이미지 없는 경우)
    """
    image_data = {}
    med_info = ""
    
    # 로깅을 위한 의약품 정보 문자열 생성
    if medicine_id or medicine_name:
        if medicine_id and medicine_name:
            med_info = f"[ID: {medicine_id}] {medicine_name}"
        elif medicine_id:
            med_info = f"[ID: {medicine_id}]"
        elif medicine_name:
            med_info = f"{medicine_name}"
    
    # 알려진 더미/빈 이미지 URL 패턴 목록
    dummy_image_patterns = [
        "e.gif", "blank.gif", "spacer.gif", "transparent.gif",
        "empty.png", "pixel.gif", "noimage", "no_img", "no-img",
        "img_x", "_blank", "loading.gif", "spinner.gif"
    ]
    
    try:
        # <span class="img_box"> 태그 찾기
        img_box_spans = soup.find_all('span', class_='img_box')
        
        # 이미지 박스가 없으면 빈 딕셔너리 반환 (이미지 없음)
        if not img_box_spans:
            if med_info:
                logger.info(f"│  {med_info} - 이미지 태그(img_box)가 없습니다.")
            else:
                logger.info(f"│  이미지 태그(img_box)가 없습니다.")
            return {}
        
        # 이미지 박스 내에서 이미지 찾기
        for span in img_box_spans:
            # a 태그 찾기
            a_tag = span.find('a')
            if not a_tag:
                continue
                
            # img 태그 찾기
            img_tag = a_tag.find('img')
            if not img_tag:
                continue
            
            # 이미지 URL 추출 우선순위에 따라 처리
            img_url = None
            
            # 1. origin_src 속성 (고해상도 원본 이미지)
            if img_tag.has_attr('origin_src') and img_tag['origin_src']:
                img_url = img_tag['origin_src']
                image_data["image_quality"] = "high"
            
            # 2. src 속성 (중간 해상도 이미지)
            elif img_tag.has_attr('src') and img_tag['src']:
                img_url = img_tag['src']
                image_data["image_quality"] = "medium"
            
            # 3. data-src 속성 (대체 이미지)
            elif img_tag.has_attr('data-src') and img_tag['data-src']:
                img_url = img_tag['data-src']
                image_data["image_quality"] = "low"
            
            # 이미지 URL이 없으면 건너뛰기
            if not img_url:
                continue
                
            # 상대 경로를 절대 경로로 변환
            if not img_url.startswith(('http://', 'https://')):
                img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
            
            # 더미 이미지 필터링
            if any(dummy in img_url.lower() for dummy in dummy_image_patterns):
                continue
            
            # 이미지 정보 추출
            image_data["image_url"] = img_url
            
            # 이미지 크기 정보 추출
            if img_tag.has_attr('width') and img_tag.has_attr('height'):
                image_data["image_width"] = img_tag['width']
                image_data["image_height"] = img_tag['height']
            
            # 원본 크기 정보 추출
            if img_tag.has_attr('origin_width') and img_tag.has_attr('origin_height'):
                image_data["original_width"] = img_tag['origin_width']
                image_data["original_height"] = img_tag['origin_height']
            
            # alt 정보 추출
            if img_tag.has_attr('alt'):
                image_data["image_alt"] = img_tag['alt']
            
            # 유효한 이미지를 찾으면 즉시 반환 (더 이상 찾지 않음)
            if med_info:
                logger.info(f"│  {med_info} - 이미지 추출 성공 ({image_data.get('image_quality', '일반')} 품질)")
            else:
                logger.info(f"│  이미지 URL 추출 성공: {img_url}")
            return image_data
        
        # img_box는 있지만 유효한 이미지를 찾지 못한 경우
        if med_info:
            logger.info(f"│  {med_info} - 이미지 박스에서 유효한 이미지를 찾을 수 없습니다.")
        else:
            logger.info("│  img_box에서 유효한 이미지를 찾을 수 없습니다.")
        return {}
        
    except Exception as e:
        if med_info:
            logger.warning(f"│  {med_info} - 이미지 추출 중 오류: {str(e)}")
        else:
            logger.warning(f"│  이미지 추출 중 오류: {str(e)}")
        return {}  # 오류가 발생하면 빈 딕셔너리 반환