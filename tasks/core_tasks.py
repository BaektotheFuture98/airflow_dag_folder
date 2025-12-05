from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import List, Dict, List, Any
from datetime import datetime, timezone
from models.connect_plugins import JdbcSinkConfig


@task(doc_md="")
def MySQLTrigger(**kwargs) -> Dict[str, Any]:
    from models.elasticsearch_model import ElasticsearchSourceConfig
    from utils.build_models import build_es_model
    from services.elasticsearch_service import data_chunks_check

    dag_run = kwargs.get("dag_run")
    info = dag_run.conf if dag_run else {}

    if isinstance(info, dict) and 'conf' in info:
        info = info.get("conf", {})
    
    if not info : 
        raise AirflowFailException("No configuration received for MySQLTrigger task")
    
    es_config = build_es_model(
        name = info.get("project_name"),
        index = info.get("elasticsearch_index"),
        query = info.get("query")
    )

    chunks = data_chunks_check(
        index = info.get("index"),
        query = info.get("query"),
        es_config = es_config
    )


    return {
        "base_info" : info, 
        "chunk" : chunks
    }

@task(doc_md="MySQL Sink Flow 시작 작업 (로깅 전용)")
def mysql_start_flow(info: Dict[str, Any]) -> Dict[str, Any]:
    print("🚀 Starting MySQL Sink Flow")
    return info

@task(doc_md="Avro 스키마 등록")
def register_avro_schema(info: Dict[str, Any]) -> Dict[str, Any]:
    pass