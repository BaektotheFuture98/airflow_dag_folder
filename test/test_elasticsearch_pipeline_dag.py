"""
Elasticsearch Pipeline DAG 테스트 코드
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from airflow.sdk.definitions import Dag
from dags.es_pipe import elasticsearch_pipeline_dag


class TestElasticsearchPipelineDAG:
    """Elasticsearch Pipeline DAG 테스트"""

    def test_dag_exists(self):
        """DAG가 성공적으로 생성되는지 확인"""
        dag = elasticsearch_pipeline_dag()
        assert dag is not None
        assert isinstance(dag, Dag)

    def test_dag_id(self):
        """DAG ID가 올바른지 확인"""
        dag = elasticsearch_pipeline_dag()
        assert dag.dag_id == "elasticsearch_pipeline_dag"

    def test_dag_schedule(self):
        """DAG schedule이 None인지 확인"""
        dag = elasticsearch_pipeline_dag()
        assert dag.schedule is None

    def test_dag_start_date(self):
        """DAG start_date가 올바른지 확인"""
        dag = elasticsearch_pipeline_dag()
        expected_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert dag.start_date == expected_date

    def test_dag_catchup_false(self):
        """DAG catchup이 False인지 확인"""
        dag = elasticsearch_pipeline_dag()
        assert dag.catchup is False

    def test_dag_tasks_count(self):
        """DAG가 5개의 task를 가지고 있는지 확인"""
        dag = elasticsearch_pipeline_dag()
        tasks = dag.tasks
        assert len(tasks) == 5

    def test_dag_task_names(self):
        """DAG의 task 이름들이 올바른지 확인"""
        dag = elasticsearch_pipeline_dag()
        task_ids = [task.task_id for task in dag.tasks]
        
        expected_task_ids = [
            "esTrigger",
            "register_avro_schema",
            "create_es_index",
            "create_es_sink_connector",
            "search_and_publish_elasticsearch"
        ]
        
        for expected_id in expected_task_ids:
            assert any(expected_id in task_id for task_id in task_ids), \
                f"Task {expected_id} not found in DAG tasks"

    def test_dag_task_dependencies(self):
        """DAG의 task 의존성이 올바른지 확인"""
        dag = elasticsearch_pipeline_dag()
        
        # 모든 task가 순차적으로 연결되어 있는지 확인
        tasks = dag.tasks
        assert len(tasks) == 5
        
        # 첫 번째 task를 제외한 모든 task는 upstream이 있어야 함
        for i in range(1, len(tasks)):
            assert len(tasks[i].upstream_list) > 0

    def test_dag_documentation(self):
        """DAG의 documentation이 있는지 확인"""
        dag = elasticsearch_pipeline_dag()
        assert dag.doc_md is not None
        assert "MySQL to Kafka to MySQL Pipeline DAG" in dag.doc_md

    @patch('app.tasks.elasticsearch_tasks.esTrigger')
    def test_dag_with_mocked_tasks(self, mock_es_trigger):
        """Mock된 task와 함께 DAG 실행 테스트"""
        mock_es_trigger.return_value = {
            "project_name": "test_project",
            "st_seq": 1,
            "es_source_config": {},
            "es_target_config": {}
        }
        
        dag = elasticsearch_pipeline_dag()
        assert dag is not None
        assert dag.dag_id == "elasticsearch_pipeline_dag"

    def test_dag_is_valid(self):
        """DAG이 유효한지 확인 (기본 DAG 검증)"""
        dag = elasticsearch_pipeline_dag()
        
        # DAG의 task들이 모두 유효한지 확인
        assert len(dag.tasks) > 0
        
        for task in dag.tasks:
            assert task.task_id
            assert task.dag is not None


class TestElasticsearchPipelineDAGTasks:
    """Elasticsearch Pipeline DAG의 개별 Task 테스트"""

    @patch('app.tasks.elasticsearch_tasks.get_logger')
    def test_es_trigger_task_with_config(self, mock_logger):
        """esTrigger task가 config를 올바르게 처리하는지 확인"""
        from plugins.tasks.elasticsearch_tasks import esTrigger
        
        # Mock kwargs 설정
        mock_dag_run = Mock()
        mock_dag_run.conf = {
            "project_name": "test_project",
            "es_source_index": "test_index",
            "query": {"match_all": {}},
            "fields": ["field1", "field2"],
            "es_target_hosts": ["localhost:9200"],
            "es_target_index": "target_index",
            "user": "test_user",
            "password": "test_password",
            "st_seq": 1
        }
        
        kwargs = {"dag_run": mock_dag_run}
        
        with patch('app.tasks.elasticsearch_tasks.build_es_source_model') as mock_build_source, \
             patch('app.tasks.elasticsearch_tasks.build_es_target_model') as mock_build_target:
            
            mock_build_source.return_value = Mock()
            mock_build_target.return_value = Mock()
            
            result = esTrigger(**kwargs)
            
            assert result is not None
            assert "project_name" in result
            assert "st_seq" in result
            assert "es_source_config" in result
            assert "es_target_config" in result

    @patch('app.tasks.elasticsearch_tasks.get_logger')
    def test_es_trigger_task_without_config(self, mock_logger):
        """esTrigger task가 config 없이 실패하는지 확인"""
        from plugins.tasks.elasticsearch_tasks import esTrigger
        from airflow.exceptions import AirflowFailException
        
        mock_dag_run = Mock()
        mock_dag_run.conf = None
        
        kwargs = {"dag_run": mock_dag_run}
        
        with pytest.raises(AirflowFailException):
            esTrigger(**kwargs)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
