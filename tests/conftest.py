"""
Pytest 설정 파일 - 모든 테스트에서 사용할 공통 설정
"""
import sys
from pathlib import Path

# 프로젝트 루트 경로를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
