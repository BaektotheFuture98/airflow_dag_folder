from datetime import datetime, timezone

from airflow.sdk import dag

from pipeline.tasks.elasticsearch_tasks import (
    create_elasticsearch_index,
    create_elasticsearch_sink_connector,
    es_trigger,
    publish_elasticsearch_documents_to_kafka,
    register_avro_schema,
)

@dag(
    dag_id="elasticsearch_pipeline_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md="Elasticsearch to Kafka to Elasticsearch pipeline DAG.",
)
def elasticsearch_pipeline_dag():
    trigger_result = es_trigger()
    schema_info = register_avro_schema(trigger_result)
    index_info = create_elasticsearch_index(schema_info)
    sink_info = create_elasticsearch_sink_connector(index_info)
    publish_result = publish_elasticsearch_documents_to_kafka(sink_info)

    trigger_result >> schema_info >> index_info >> sink_info >> publish_result

elasticsearch_pipeline_dag()
