"""
Pytest 설정 파일 - 모든 테스트에서 사용할 공통 설정
"""
import os
import sys
from pathlib import Path

# 프로젝트 루트 경로를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def mock_airflow_variable():
    """Airflow Variable 모킹 fixture"""
    with patch('airflow.models.Variable.get') as mock_get:
        def side_effect(key, default=None):
            test_variables = {
                "ELASTICSEARCH_HOSTS": "localhost:9200",
                "ELASTICSEARCH_USER": "elastic",
                "ELASTICSEARCH_PASSWORD": "password",
                "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
                "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            }
            return test_variables.get(key, default)
        
        mock_get.side_effect = side_effect
        yield mock_get


@pytest.fixture
def mock_dag_run():
    """Mock DAG Run fixture"""
    mock_run = Mock()
    mock_run.conf = {
        "project_name": "test_project",
        "st_seq": 1,
        "es_source_index": "test_index",
        "query": {"match_all": {}},
        "fields": ["field1", "field2"],
        "es_target_hosts": ["localhost:9200"],
        "es_target_index": "target_index",
        "mysql_host": "localhost",
        "mysql_database": "test_db",
        "mysql_table": "test_table",
        "user": "test_user",
        "password": "test_password",
    }
    return mock_run


@pytest.fixture
def mock_logger():
    """Logger 모킹 fixture"""
    with patch('app.config.logger.get_logger') as mock_get_logger:
        mock_logger_instance = Mock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance
