#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
이미지 정보 파싱 모듈
"""

import re
import urllib.parse
import logging

# 로거 설정
logger = logging.getLogger(__name__)

def extract_medicine_image(soup):
    """
    의약품 이미지 정보 추출 (성능 개선 버전)
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
            
    Returns:
        dict: 이미지 데이터
    """
    image_data = {}
    
    # 알려진 더미/빈 이미지 URL 패턴 목록
    dummy_image_patterns = [
        "e.gif", "blank.gif", "spacer.gif", "transparent.gif",
        "empty.png", "pixel.gif", "noimage", "no_img", "no-img",
        "img_x", "_blank", "loading.gif", "spinner.gif"
    ]
    
    try:
        # 이미지 추출 전략 (우선순위 순)
        extraction_strategies = [
            extract_image_from_imgbox,       # 1. 이미지 박스에서 추출
            extract_image_from_containers,   # 2. 이미지 컨테이너에서 추출
            extract_image_from_all_images    # 3. 모든 이미지에서 추출
        ]
        
        # 각 전략 순차적 시도
        for strategy in extraction_strategies:
            try:
                img_data = strategy(soup, dummy_image_patterns)
                if img_data and "image_url" in img_data:
                    image_data.update(img_data)
                    break
            except Exception as e:
                logger.warning(f"이미지 추출 전략 {strategy.__name__} 실행 중 오류: {str(e)}")
                continue
        
        # 로깅
        if "image_url" in image_data:
            logger.info(f"의약품 이미지 URL 추출 성공: {image_data['image_url']}")
        else:
            logger.info("의약품 이미지를 찾을 수 없습니다.")
    
    except Exception as e:
        logger.warning(f"이미지 추출 중 예외 발생: {str(e)}")
    
    return image_data

def extract_image_from_imgbox(soup, dummy_patterns):
    """
    이미지 박스(img_box)에서 이미지 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        dummy_patterns (list): 더미 이미지 패턴 목록
            
    Returns:
        dict: 이미지 데이터
    """
    image_data = {}
    
    # img_box 클래스 내의 이미지 링크 찾기 (가장 일반적인 케이스)
    img_box_spans = soup.find_all('span', class_='img_box')
    if not img_box_spans:
        img_box_spans = soup.find_all(['div', 'span'], class_=['thumb_img', 'thumb_area'])
    
    for span in img_box_spans:
        # a 태그 찾기
        a_tag = span.find('a')
        if a_tag and a_tag.has_attr('href') and 'phinf' in a_tag['href']:
            # 이미지 URL이 a 태그의 href에 있는 경우 (이미지 페이지로 연결)
            img_url = a_tag['href']
            
            # URL에서 실제 이미지 부분만 추출 시도
            url_match = re.search(r'imageUrl=([^&]+)', img_url)
            if url_match:
                img_url = urllib.parse.unquote(url_match.group(1))
            
            # img 태그가 있으면 추가 정보 확인
            img_tag = a_tag.find('img')
            if img_tag:
                # data-src 속성 확인 (지연 로딩 이미지)
                if img_tag.has_attr('data-src') and img_tag['data-src']:
                    img_url = img_tag['data-src']
                # src 속성 확인
                elif img_tag.has_attr('src') and img_tag['src']:
                    img_url = img_tag['src']
                
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
                
                # desc source 정보 추출 (이미지 설명)
                if img_tag.has_attr('desc_source'):
                    image_data["image_desc"] = img_tag['desc_source']
            
            # 상대 경로를 절대 경로로 변환
            if img_url and not img_url.startswith(('http://', 'https://')):
                img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
            
            # 더미 이미지 필터링
            if img_url and not any(dummy in img_url.lower() for dummy in dummy_patterns):
                image_data["image_url"] = img_url
                # 첫 번째 유효한 이미지 찾으면 종료
                break
    
    return image_data

def extract_image_from_containers(soup, dummy_patterns):
    """
    이미지 컨테이너에서 이미지 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        dummy_patterns (list): 더미 이미지 패턴 목록
            
    Returns:
        dict: 이미지 데이터
    """
    image_data = {}
    
    # 이미지 컨테이너 클래스 검색
    img_containers = soup.find_all(['div', 'span'], class_=[
        'thumb_area', 'thumb_wrap', 'photo_frame', 'img_area', 'thumb_img', 
        'medicine_img', 'drug_thumb', 'medicine_thumb'
    ])
    
    for container in img_containers:
        img_tag = container.find('img')
        if img_tag:
            # data-src 속성 확인 (지연 로딩 이미지)
            if img_tag.has_attr('data-src') and img_tag['data-src']:
                img_url = img_tag['data-src']
            # src 속성 확인
            elif img_tag.has_attr('src') and img_tag['src']:
                img_url = img_tag['src']
            else:
                continue
            
            # 상대 경로를 절대 경로로 변환
            if not img_url.startswith(('http://', 'https://')):
                img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
            
            # 더미 이미지 필터링
            if not any(dummy in img_url.lower() for dummy in dummy_patterns):
                # 이미지 크기가 너무 작은지 확인
                width = height = 0
                if img_tag.has_attr('width') and img_tag.has_attr('height'):
                    try:
                        width = int(img_tag['width'])
                        height = int(img_tag['height'])
                    except (ValueError, TypeError):
                        pass
                
                # 이미지가 충분히 큰 경우만 사용 (작은 아이콘 제외)
                if not (width > 0 and height > 0 and (width < 30 or height < 30)):
                    image_data["image_url"] = img_url
                    
                    # 추가 정보 수집
                    if img_tag.has_attr('width') and img_tag.has_attr('height'):
                        image_data["image_width"] = img_tag['width']
                        image_data["image_height"] = img_tag['height']
                    if img_tag.has_attr('alt'):
                        image_data["image_alt"] = img_tag['alt']
                    
                    # 첫 번째 유효한 이미지 찾으면 종료
                    break
    
    return image_data

def extract_image_from_all_images(soup, dummy_patterns):
    """
    모든 이미지 태그에서 의약품 관련 이미지 추출
    
    Args:
        soup (BeautifulSoup): 파싱된 HTML 객체
        dummy_patterns (list): 더미 이미지 패턴 목록
            
    Returns:
        dict: 이미지 데이터
    """
    image_data = {}
    
    # 의약품 관련 키워드
    medicine_keywords = ['약', '정', '알약', '의약품', '캡슐', 'drug', 'pill', 'med', 'pharm', 'medicine', 'tablet']
    
    # img 태그 중 크기가 큰 순서로 정렬하여 검사
    img_tags = soup.find_all('img')
    potential_images = []
    
    for img_tag in img_tags:
        # src 또는 data-src 속성 확인
        img_url = None
        if img_tag.has_attr('data-src') and img_tag['data-src']:
            img_url = img_tag['data-src']
        elif img_tag.has_attr('src') and img_tag['src']:
            img_url = img_tag['src']
        else:
            continue
        
        # 상대 경로를 절대 경로로 변환
        if not img_url.startswith(('http://', 'https://')):
            img_url = urllib.parse.urljoin('https://terms.naver.com', img_url)
        
        # 더미 이미지 필터링
        if any(dummy in img_url.lower() for dummy in dummy_patterns):
            continue
        
        # 이미지 점수 계산 (크기, 키워드 관련성 등)
        score = 0
        
        # 크기 점수
        width = height = 0
        if img_tag.has_attr('width') and img_tag.has_attr('height'):
            try:
                width = int(img_tag['width'])
                height = int(img_tag['height'])
                # 크기 점수: 면적에 비례
                score += (width * height) / 1000
            except (ValueError, TypeError):
                pass
        
        # 너무 작은 이미지는 제외 (아이콘)
        if (width > 0 and width < 30) or (height > 0 and height < 30):
            continue
        
        # 이미지 URL 및 대체 텍스트에서 의약품 관련 키워드 확인
        alt_text = img_tag.get('alt', '').lower()
        img_url_lower = img_url.lower()
        
        # 키워드 점수
        for keyword in medicine_keywords:
            if keyword in alt_text:
                score += 5  # alt 텍스트에 키워드가 있으면 높은 점수
            if keyword in img_url_lower:
                score += 3  # URL에 키워드가 있으면 중간 점수
        
        # 네이버 의약품 이미지 특징적인 URL 패턴
        if 'dbscthumb-phinf' in img_url_lower:
            score += 10  # 네이버 의약품 이미지 패턴 가산점
        
        # 잠재적 이미지 후보에 추가
        potential_images.append({
            'img_tag': img_tag,
            'img_url': img_url,
            'score': score
        })
    
    # 점수 기준 내림차순 정렬
    potential_images.sort(key=lambda x: x['score'], reverse=True)
    
    # 최고 점수 이미지 선택
    if potential_images:
        best_image = potential_images[0]
        img_tag = best_image['img_tag']
        img_url = best_image['img_url']
        
        image_data["image_url"] = img_url
        
        # 추가 정보 수집
        if img_tag.has_attr('width'):
            image_data["image_width"] = img_tag['width']
        if img_tag.has_attr('height'):
            image_data["image_height"] = img_tag['height']
        if img_tag.has_attr('alt'):
            image_data["image_alt"] = img_tag['alt']
    
    return image_data