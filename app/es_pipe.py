from datetime import datetime, timezone
from airflow.sdk import dag

# TaskFlow API tasks from core_tasks
from app.tasks.core_tasks import (
	mySQLTrigger,
	register_avro_schema,
	create_jdbc_sink_connector,
	search_and_publish_elasticsearch,
)

@dag(
    dag_id = "mysql_pipeline_dag",
    schedule = None, 
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    doc_md = """
    MySQL to Kafka to MySQL Pipeline DAG
    """
)
def mysql_pipeline_dag():
    mysql_trigger = mySQLTrigger()
    schema_info = register_avro_schema(mysql_trigger)
    jdbc_info = create_jdbc_sink_connector(schema_info)
    es_result = search_and_publish_elasticsearch(jdbc_info)
    
    mysql_trigger >> schema_info >> jdbc_info >> es_result

mysql_pipeline_dag()