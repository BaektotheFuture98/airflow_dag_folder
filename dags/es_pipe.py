from datetime import datetime, timezone
from airflow.sdk import dag

# TaskFlow API tasks from core_tasks
from plugins.tasks.elasticsearch_tasks import (
	esTrigger,
	register_avro_schema,
    create_es_index,
	create_es_sink_connector,
	search_and_publish_elasticsearch,
)

@dag(
    dag_id = "elasticsearch_pipeline_dag",
    schedule = None, 
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md = """
    MySQL to Kafka to MySQL Pipeline DAG
    """
)
def elasticsearch_pipeline_dag():
    esTrigger_task = esTrigger()
    schema_info = register_avro_schema(esTrigger_task)
    es_index_info = create_es_index(schema_info)
    es_sink_info = create_es_sink_connector(es_index_info)
    es_result = search_and_publish_elasticsearch(es_sink_info)
    
    esTrigger_task >> schema_info >> es_index_info >> es_sink_info >> es_result

elasticsearch_pipeline_dag()