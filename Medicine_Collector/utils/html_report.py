#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
HTML 보고서 생성
"""

import os
import logging
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)

def init_html_report(html_dir):
    """
    HTML 보고서 파일 초기화
    
    Args:
        html_dir (str): HTML 파일 저장 디렉토리
    
    Returns:
        str: 생성된 HTML 파일 경로
    """
    # 현재 HTML 파일 번호 계산
    html_files = [f for f in os.listdir(html_dir) if f.startswith('medicine_report_') and f.endswith('.html')]
    report_num = len(html_files) + 1
    
    # 파일명 생성
    html_filename = f"medicine_report_{report_num:03d}.html"
    html_path = os.path.join(html_dir, html_filename)
    
    # HTML 기본 구조 생성
    html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>의약품 데이터 수집 보고서</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        .medicine-item { 
            border: 1px solid #ddd; 
            margin-bottom: 20px; 
            padding: 15px; 
            border-radius: 5px;
            background-color: #f9f9f9;
        }
        .medicine-item h3 { 
            margin-top: 0; 
            color: #3498db; 
            border-bottom: 1px solid #ddd;
            padding-bottom: 5px;
        }
        .medicine-info { display: flex; flex-wrap: wrap; }
        .info-group { margin-right: 20px; margin-bottom: 10px; flex-basis: 30%; }
        .info-label { font-weight: bold; color: #7f8c8d; }
        .info-value { margin-left: 5px; }
        .missing { color: #e74c3c; }
        .exists { color: #27ae60; }
        .medicine-separator { 
            height: 2px; 
            background-color: #3498db; 
            margin: 30px 0; 
        }
        .timestamp { 
            color: #7f8c8d; 
            font-size: 0.8em; 
            text-align: right; 
            margin-top: 20px; 
        }
    </style>
</head>
<body>
    <h1>의약품 데이터 수집 보고서</h1>
    <div class="timestamp">생성 시간: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</div>
    <div id="medicine-list">
"""
    
    # 파일 저장
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"HTML 보고서 초기화 완료: {html_path}")
    
    return html_path

def add_to_html_report(medicine_data, html_file, item_count):
    """
    HTML 보고서에 의약품 데이터 추가
    
    Args:
        medicine_data (dict): 추가할 의약품 데이터
        html_file (str): HTML 파일 경로
        item_count (int): 현재 항목 카운터
        
    Returns:
        int: 업데이트된 항목 카운터
    """
    if html_file is None:
        return item_count
    
    # 의약품 데이터를 HTML로 변환
    html_item = """
        <div class="medicine-item">
            <h3>""" + medicine_data.get('korean_name', '이름 없음') + """</h3>
            <div class="medicine-info">
    """
    
    # 기본 정보 그룹
    html_item += """
                <div class="info-group">
                    <div><span class="info-label">ID:</span> <span class="info-value">""" + medicine_data.get('id', '') + """</span></div>
                    <div><span class="info-label">영문명:</span> <span class="info-value">""" + medicine_data.get('english_name', '정보 없음') + """</span></div>
                    <div><span class="info-label">제조사:</span> <span class="info-value">""" + medicine_data.get('company', '정보 없음') + """</span></div>
                    <div><span class="info-label">분류:</span> <span class="info-value">""" + medicine_data.get('category', '정보 없음') + """</span></div>
                    <div><span class="info-label">보험코드:</span> <span class="info-value">""" + medicine_data.get('insurance_code', '정보 없음') + """</span></div>
                </div>
    """
    
    # 외형 정보 그룹
    html_item += """
                <div class="info-group">
                    <div><span class="info-label">성상:</span> <span class="info-value">""" + medicine_data.get('appearance', '정보 없음') + """</span></div>
                    <div><span class="info-label">모양:</span> <span class="info-value">""" + medicine_data.get('shape', '정보 없음') + """</span></div>
                    <div><span class="info-label">색깔:</span> <span class="info-value">""" + medicine_data.get('color', '정보 없음') + """</span></div>
                    <div><span class="info-label">크기:</span> <span class="info-value">""" + medicine_data.get('size', '정보 없음') + """</span></div>
                    <div><span class="info-label">식별표기:</span> <span class="info-value">""" + medicine_data.get('identification', '정보 없음') + """</span></div>
    """

    # 분할선 정보 추가
    division_info_text = "정보 없음"
    if "division_info" in medicine_data and medicine_data["division_info"]:
        if isinstance(medicine_data["division_info"], dict) and "division_description" in medicine_data["division_info"]:
            division_info_text = medicine_data["division_info"]["division_description"] or "정보 없음"
    
    html_item += """
                    <div><span class="info-label">분할선:</span> <span class="info-value">""" + division_info_text + """</span></div>
                </div>
    """
    
    # 데이터 완성도 그룹
    html_item += """
                <div class="info-group">
                    <div><span class="info-label">성분정보:</span> <span class="info-value """ + ('exists' if 'components' in medicine_data and medicine_data['components'] else 'missing') + """">""" + ('있음' if 'components' in medicine_data and medicine_data['components'] else '없음') + """</span></div>
                    <div><span class="info-label">효능효과:</span> <span class="info-value """ + ('exists' if 'efficacy' in medicine_data and medicine_data['efficacy'] else 'missing') + """">""" + ('있음' if 'efficacy' in medicine_data and medicine_data['efficacy'] else '없음') + """</span></div>
                    <div><span class="info-label">용법용량:</span> <span class="info-value """ + ('exists' if 'dosage' in medicine_data and medicine_data['dosage'] else 'missing') + """">""" + ('있음' if 'dosage' in medicine_data and medicine_data['dosage'] else '없음') + """</span></div>
                    <div><span class="info-label">주의사항:</span> <span class="info-value """ + ('exists' if 'precautions' in medicine_data and medicine_data['precautions'] else 'missing') + """">""" + ('있음' if 'precautions' in medicine_data and medicine_data['precautions'] else '없음') + """</span></div>
                    <div><span class="info-label">이미지:</span> <span class="info-value """ + ('exists' if 'image_url' in medicine_data and medicine_data['image_url'] else 'missing') + """">""" + ('있음' if 'image_url' in medicine_data and medicine_data['image_url'] else '없음') + """</span></div>
                </div>
    """
    
    # 항목 마무리
    html_item += """
            </div>
        </div>
        <div class="medicine-separator"></div>
    """
    
    # HTML 파일에 항목 추가
    with open(html_file, 'a', encoding='utf-8') as f:
        f.write(html_item)
    
    # 항목 카운터 증가
    item_count += 1
    
    return item_count

def finalize_html_report(html_file):
    """
    HTML 보고서 마무리
    
    Args:
        html_file (str): HTML 파일 경로
    """
    if html_file is None:
        return
    
    # HTML 마무리 태그 추가
    with open(html_file, 'a', encoding='utf-8') as f:
        f.write("""
    </div>
    <div class="timestamp">완료 시간: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</div>
</body>
</html>
""")
    
    logger.info(f"HTML 보고서 마무리 완료: {html_file}")