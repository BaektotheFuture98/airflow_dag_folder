from datetime import datetime, timezone

from airflow.sdk.definitions import Dag

from dags.es_pipe import elasticsearch_pipeline_dag
from pipeline.tasks.common import get_search_after_token


class TestElasticsearchPipelineDAG:
    def test_dag_exists(self):
        dag = elasticsearch_pipeline_dag()
        assert isinstance(dag, Dag)

    def test_dag_metadata(self):
        dag = elasticsearch_pipeline_dag()
        assert dag.dag_id == "elasticsearch_pipeline_dag"
        assert dag.schedule is None
        assert dag.start_date == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert dag.catchup is False
        assert dag.doc_md == "Elasticsearch to Kafka to Elasticsearch pipeline DAG."

    def test_dag_task_names(self):
        dag = elasticsearch_pipeline_dag()
        task_ids = [task.task_id for task in dag.tasks]
        assert task_ids == [
            "es_trigger",
            "register_avro_schema",
            "create_elasticsearch_index",
            "create_elasticsearch_sink_connector",
            "publish_elasticsearch_documents_to_kafka",
        ]

    def test_dag_dependencies(self):
        dag = elasticsearch_pipeline_dag()
        tasks = {task.task_id: task for task in dag.tasks}

        assert tasks["register_avro_schema"].upstream_task_ids == {"es_trigger"}
        assert tasks["create_elasticsearch_index"].upstream_task_ids == {"register_avro_schema"}
        assert tasks["create_elasticsearch_sink_connector"].upstream_task_ids == {"create_elasticsearch_index"}
        assert tasks["publish_elasticsearch_documents_to_kafka"].upstream_task_ids == {"create_elasticsearch_sink_connector"}


class TestElasticsearchTaskHelpers:
    def test_get_search_after_token(self):
        hits = [
            {"sort": ["1"]},
            {"sort": ["2"]},
        ]
        assert get_search_after_token(hits) == "2"

    def test_get_search_after_token_without_sort(self):
        assert get_search_after_token([{"_source": {"kw_docid": "1"}}]) is None
