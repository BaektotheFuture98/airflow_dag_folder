from datetime import datetime, timezone

from airflow.sdk import dag

from pipeline.tasks.mysql_tasks import (
    create_jdbc_sink_connectors,
    mysql_trigger,
    publish_elasticsearch_documents_to_kafka,
    register_avro_schema,
)

@dag(
    dag_id="mysql_pipeline_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md="Elasticsearch to Kafka to MySQL pipeline DAG.",
)
def mysql_pipeline_dag():
    trigger_result = mysql_trigger()
    schema_info = register_avro_schema(trigger_result)
    connector_info = create_jdbc_sink_connectors(schema_info)
    publish_result = publish_elasticsearch_documents_to_kafka(connector_info)

    trigger_result >> schema_info >> connector_info >> publish_result

mysql_pipeline_dag()
