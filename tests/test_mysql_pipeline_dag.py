from datetime import datetime, timezone

from airflow.sdk.definitions import Dag

from dags.mysql_pipe import mysql_pipeline_dag
from pipeline.tasks.common import build_chunked_topic_names, normalize_record


class TestMySQLPipelineDAG:
    def test_dag_exists(self):
        dag = mysql_pipeline_dag()
        assert isinstance(dag, Dag)

    def test_dag_metadata(self):
        dag = mysql_pipeline_dag()
        assert dag.dag_id == "mysql_pipeline_dag"
        assert dag.schedule is None
        assert dag.start_date == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert dag.catchup is False
        assert dag.doc_md == "Elasticsearch to Kafka to MySQL pipeline DAG."

    def test_dag_task_names(self):
        dag = mysql_pipeline_dag()
        task_ids = [task.task_id for task in dag.tasks]
        assert task_ids == [
            "mySQLTrigger",
            "register_avro_schema",
            "create_jdbc_sink_connector",
            "search_and_publish_elasticsearch",
        ]

    def test_dag_dependencies(self):
        dag = mysql_pipeline_dag()
        tasks = {task.task_id: task for task in dag.tasks}

        assert tasks["register_avro_schema"].upstream_task_ids == {"mySQLTrigger"}
        assert tasks["create_jdbc_sink_connector"].upstream_task_ids == {"register_avro_schema"}
        assert tasks["search_and_publish_elasticsearch"].upstream_task_ids == {"create_jdbc_sink_connector"}


class TestMySQLTaskHelpers:
    def test_build_chunked_topic_names(self):
        assert build_chunked_topic_names("project", "table", 3) == [
            ("project", "table"),
            ("project-002", "table_002"),
            ("project-003", "table_003"),
        ]

    def test_normalize_record(self):
        assert normalize_record({"an_content": "", "kw_docid": "1"}) == {
            "an_content": " ",
            "kw_docid": "1",
        }
