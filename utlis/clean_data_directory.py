import os
import shutil
import glob

def clean_data_directory(base_dir='collected_data'):
    # 디렉토리 존재 확인
    if not os.path.exists(base_dir):
        print(f"{base_dir} 디렉토리가 존재하지 않습니다.")
        return

    # 삭제할 하위 디렉토리 및 파일 목록
    subdirs_to_remove = ['json', 'html', 'data']
    
    # 모든 관련 파일 찾아 삭제
    files_to_remove = [
        os.path.join(base_dir, 'medicine_collector.log'),
        os.path.join(base_dir, 'daily_request_count.json'), 
        os.path.join(base_dir, 'checkpoint.json'), 
        os.path.join(base_dir, 'current_keyword.txt'), 
        os.path.join(base_dir, 'keywords_todo.txt'), 
        os.path.join(base_dir, 'keywords_done.txt'),
        os.path.join(base_dir, 'processed_medicine_ids.txt')
    ]

    # 하위 디렉토리 삭제 및 재생성
    for subdir in subdirs_to_remove:
        full_path = os.path.join(base_dir, subdir)
        if os.path.exists(full_path):
            shutil.rmtree(full_path)
            print(f"{full_path} 디렉토리 삭제 완료")
        
        # 디렉토리 다시 생성
        os.makedirs(full_path)
        print(f"{full_path} 디렉토리 재생성 완료")

    # 파일 삭제
    for file_path in files_to_remove:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"{file_path} 파일 삭제 완료")
        except Exception as e:
            print(f"{file_path} 파일 삭제 중 오류: {e}")

    # 숨겨진 파일이나 다른 확장자 파일 찾아 삭제
    hidden_files = glob.glob(os.path.join(base_dir, '.*'))
    for hidden_file in hidden_files:
        try:
            if os.path.isfile(hidden_file):
                os.remove(hidden_file)
                print(f"{hidden_file} 숨김 파일 삭제 완료")
        except Exception as e:
            print(f"{hidden_file} 숨김 파일 삭제 중 오류: {e}")

    print("데이터 초기화 완료")

# 스크립트 실행
if __name__ == "__main__":
    clean_data_directory()