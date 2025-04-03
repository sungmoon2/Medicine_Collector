#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
안전한 종료 및 정규식 유틸리티
"""

import os
import re
import time
import signal
import threading
import logging

# 로거 설정
logger = logging.getLogger(__name__)

# 전역 종료 플래그
shutdown_requested = False
shutdown_event = threading.Event()

# SIGINT(Ctrl+C) 핸들러
def sigint_handler(signum, frame):
    """SIGINT 신호 처리"""
    global shutdown_requested
    if not shutdown_requested:
        print("\n\n[!] Ctrl+C가 감지되었습니다. 안전하게 종료 중... (다시 누르면 강제 종료)")
        shutdown_requested = True
        shutdown_event.set()  # 이벤트 설정
        # 두 번째 Ctrl+C는 강제 종료로 설정
        signal.signal(signal.SIGINT, force_exit_handler)
    else:
        # 이미 종료 요청이 있었다면
        force_exit_handler(signum, frame)

# 강제 종료 핸들러
def force_exit_handler(signum, frame):
    """강제 종료 처리"""
    print("\n[!] 강제 종료 요청 감지. 즉시 종료합니다.")
    os._exit(1)  # 강제 종료 (sys.exit보다 더 강력)

# 주기적으로 종료 플래그 확인하는 감시 스레드 함수
def watchdog_thread():
    """종료 신호 감시 스레드"""
    while True:
        if shutdown_requested:
            print("[정보] 종료 감시 스레드: 종료 요청이 감지되었습니다.")
            # 모든 스레드에 KeyboardInterrupt 발생시키기 위한 시도
            if hasattr(threading, '_MainThread'):
                for thread in threading.enumerate():
                    if thread != threading.current_thread() and thread.is_alive():
                        try:
                            # 메인 스레드에 인터럽트 발생시키기
                            if hasattr(thread, "_tstate_lock") and thread._tstate_lock:
                                thread._tstate_lock.release()
                                thread._stop()
                        except:
                            pass
            # 1초 후 여전히 실행 중이면 더 강력한 종료 시도
            time.sleep(1)
            if threading.active_count() > 2:  # 메인 + 현재 스레드
                print("[경고] 일부 스레드가 여전히 실행 중입니다. 더 강력한 종료 시도...")
                try:
                    # 강제 종료 준비 (정리 작업)
                    os._exit(1)
                except:
                    pass
            break
        time.sleep(0.5)  # 0.5초마다 체크

# 실행 시 신호 핸들러 등록 함수
def setup_signal_handlers():
    """신호 핸들러 설정"""
    # Ctrl+C 핸들러 등록
    signal.signal(signal.SIGINT, sigint_handler)
    
    # 감시 스레드 시작
    watchdog = threading.Thread(target=watchdog_thread, daemon=True)
    watchdog.start()
    
    print("[정보] 안전한 종료 메커니즘이 설정되었습니다. Ctrl+C로 언제든지 중단할 수 있습니다.")

# 모든 정규식 패턴을 안전하게 사용하는 헬퍼 함수
def safe_regex_search(pattern, text, default=None):
    """
    안전하게 정규식 검색을 수행하는 유틸리티 함수
    
    Args:
        pattern: 정규식 패턴
        text: 검색할 텍스트
        default: 매치가 없거나 오류 발생 시 반환할 기본값
        
    Returns:
    re.Match 객체 또는 default
    """
    if not text:
        return default
        
    try:
        return re.search(pattern, text)
    except Exception as e:
        logger.warning(f"정규식 검색 중 오류: {pattern} - {str(e)}")
        return default

def safe_regex_group(match, group_idx, default=""):
    """
    안전하게 정규식 그룹을 추출하는 유틸리티 함수
    
    Args:
        match: re.Match 객체
        group_idx: 그룹 인덱스 또는 이름
        default: 그룹이 없거나 오류 발생 시 반환할 기본값
        
    Returns:
        추출된 그룹 문자열 또는 default
    """
    if not match:
        return default
        
    try:
        return match.group(group_idx)
    except (IndexError, AttributeError) as e:
        logger.warning(f"정규식 그룹 접근 중 오류: 그룹 {group_idx} - {str(e)}")
        return default