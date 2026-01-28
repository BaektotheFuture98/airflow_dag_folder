"""
MySQL Pipeline DAG 테스트 코드
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from airflow.sdk.definitions import Dag
from dags.mysql_pipe import mysql_pipeline_dag


class TestMySQLPipelineDAG:
    """MySQL Pipeline DAG 테스트"""

    def test_dag_exists(self):
        """DAG가 성공적으로 생성되는지 확인"""
        dag = mysql_pipeline_dag()
        assert dag is not None
        assert isinstance(dag, Dag)

    def test_dag_id(self):
        """DAG ID가 올바른지 확인"""
        dag = mysql_pipeline_dag()
        assert dag.dag_id == "mysql_pipeline_dag"

    def test_dag_schedule(self):
        """DAG schedule이 None인지 확인"""
        dag = mysql_pipeline_dag()
        assert dag.schedule is None

    def test_dag_start_date(self):
        """DAG start_date가 올바른지 확인"""
        dag = mysql_pipeline_dag()
        expected_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert dag.start_date == expected_date

    def test_dag_catchup_false(self):
        """DAG catchup이 False인지 확인"""
        dag = mysql_pipeline_dag()
        assert dag.catchup is False

    def test_dag_tasks_count(self):
        """DAG가 4개의 task를 가지고 있는지 확인"""
        dag = mysql_pipeline_dag()
        tasks = dag.tasks
        assert len(tasks) == 4

    def test_dag_task_names(self):
        """DAG의 task 이름들이 올바른지 확인"""
        dag = mysql_pipeline_dag()
        task_ids = [task.task_id for task in dag.tasks]
        
        expected_task_ids = [
            "mySQLTrigger",
            "register_avro_schema",
            "create_jdbc_sink_connector",
            "search_and_publish_elasticsearch"
        ]
        
        for expected_id in expected_task_ids:
            assert any(expected_id in task_id for task_id in task_ids), \
                f"Task {expected_id} not found in DAG tasks"

    def test_dag_task_dependencies(self):
        """DAG의 task 의존성이 올바른지 확인"""
        dag = mysql_pipeline_dag()
        
        # 모든 task가 순차적으로 연결되어 있는지 확인
        tasks = dag.tasks
        assert len(tasks) == 4
        
        # 첫 번째 task를 제외한 모든 task는 upstream이 있어야 함
        for i in range(1, len(tasks)):
            assert len(tasks[i].upstream_list) > 0

    def test_dag_documentation(self):
        """DAG의 documentation이 있는지 확인"""
        dag = mysql_pipeline_dag()
        assert dag.doc_md is not None
        assert "MySQL to Kafka to MySQL Pipeline DAG" in dag.doc_md

    @patch('app.tasks.mysql_tasks.mySQLTrigger')
    def test_dag_with_mocked_tasks(self, mock_mysql_trigger):
        """Mock된 task와 함께 DAG 실행 테스트"""
        mock_mysql_trigger.return_value = {
            "project_name": "test_project",
            "st_seq": 1,
            "es_source_config": {},
            "mysql_config": {}
        }
        
        dag = mysql_pipeline_dag()
        assert dag is not None
        assert dag.dag_id == "mysql_pipeline_dag"

    def test_dag_is_valid(self):
        """DAG이 유효한지 확인 (기본 DAG 검증)"""
        dag = mysql_pipeline_dag()
        
        # DAG의 task들이 모두 유효한지 확인
        assert len(dag.tasks) > 0
        
        for task in dag.tasks:
            assert task.task_id
            assert task.dag is not None


class TestMySQLPipelineDAGTasks:
    """MySQL Pipeline DAG의 개별 Task 테스트"""

    @patch('app.tasks.mysql_tasks.get_logger')
    def test_mysql_trigger_task_with_config(self, mock_logger):
        """mySQLTrigger task가 config를 올바르게 처리하는지 확인"""
        from plugins.tasks.mysql_tasks import mySQLTrigger
        
        # Mock kwargs 설정
        mock_dag_run = Mock()
        mock_dag_run.conf = {
            "project_name": "test_project",
            "es_source_index": "test_index",
            "query": {"match_all": {}},
            "fields": ["field1", "field2"],
            "mysql_host": "localhost",
            "mysql_database": "test_db",
            "mysql_table": "test_table",
            "user": "test_user",
            "password": "test_password",
            "st_seq": 1
        }
        
        kwargs = {"dag_run": mock_dag_run}
        
        with patch('app.tasks.mysql_tasks.Variable.get') as mock_variable_get, \
             patch('app.tasks.mysql_tasks.build_es_source_model') as mock_build_source, \
             patch('app.tasks.mysql_tasks.build_mysql_config') as mock_build_mysql, \
             patch('app.tasks.mysql_tasks.ElasticsearchService') as mock_es_service:
            
            mock_variable_get.side_effect = lambda key: f"test_{key}"
            mock_build_source.return_value = Mock()
            mock_build_mysql.return_value = Mock()
            mock_es_service.return_value.get_chunk_count.return_value = 10
            
            result = mySQLTrigger(**kwargs)
            
            assert result is not None
            assert "project_name" in result
            assert "st_seq" in result

    @patch('app.tasks.mysql_tasks.get_logger')
    def test_mysql_trigger_task_without_config(self, mock_logger):
        """mySQLTrigger task가 config 없이 실패하는지 확인"""
        from plugins.tasks.mysql_tasks import mySQLTrigger
        from airflow.exceptions import AirflowFailException
        
        mock_dag_run = Mock()
        mock_dag_run.conf = None
        
        kwargs = {"dag_run": mock_dag_run}
        
        with pytest.raises(AirflowFailException):
            mySQLTrigger(**kwargs)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
