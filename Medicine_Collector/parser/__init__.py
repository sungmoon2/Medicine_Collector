"""
파서 모듈 패키지.
HTML 파싱 및 데이터 추출 관련 모듈을 제공합니다.
"""


from .html_parser import is_medicine_page, fetch_medicine_data
from .profile_parser import extract_profile_data, extract_basic_info, standardize_profile_data, extract_supplementary_identification as extract_identification_info_safe
from .image_parser import extract_medicine_image
from .section_parser import extract_detailed_sections, normalize_field_names