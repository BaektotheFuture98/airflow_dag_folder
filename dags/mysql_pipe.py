from datetime import datetime, timezone

from airflow.sdk import dag

from pipeline.tasks.mysql_tasks import (
    create_jdbc_sink_connector,
    mySQLTrigger,
    register_avro_schema,
    search_and_publish_elasticsearch,
)

@dag(
    dag_id="mysql_pipeline_dag",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md="Elasticsearch to Kafka to MySQL pipeline DAG.",
)
def mysql_pipeline_dag():
    mysql_trigger = mySQLTrigger()
    schema_info = register_avro_schema(mysql_trigger)
    jdbc_info = create_jdbc_sink_connector(schema_info)
    publish_result = search_and_publish_elasticsearch(jdbc_info)

    mysql_trigger >> schema_info >> jdbc_info >> publish_result

mysql_pipeline_dag()
