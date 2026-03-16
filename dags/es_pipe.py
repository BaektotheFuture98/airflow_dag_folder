from datetime import datetime, timezone

from airflow.sdk import dag

from pipeline.tasks.elasticsearch_tasks import (
    create_es_index,
    create_es_sink_connector,
    esTrigger,
    register_avro_schema,
    search_and_publish_elasticsearch,
)

@dag(
    dag_id="elasticsearch_pipeline_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md="Elasticsearch to Kafka to Elasticsearch pipeline DAG.",
)
def elasticsearch_pipeline_dag():
    es_trigger = esTrigger()
    schema_info = register_avro_schema(es_trigger)
    es_index_info = create_es_index(schema_info)
    es_sink_info = create_es_sink_connector(es_index_info)
    publish_result = search_and_publish_elasticsearch(es_sink_info)

    es_trigger >> schema_info >> es_index_info >> es_sink_info >> publish_result

elasticsearch_pipeline_dag()
