from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import List, Dict, List, Any

@task(doc_md="API 수신 완료, 설정 파일 구성")
def MySQLTrigger(**kwargs) -> Dict[str, Any]:
    from repositories.elasticsearch_repo import ElasticsearchRepo
    from services.elasticsearch_service import ElasticsearchService
    from models.build_models import build_es_source_model, build_mysql_model

    dag_run = kwargs.get("dag_run")
    info = dag_run.conf if dag_run else {}

    if isinstance(info, dict) and 'conf' in info:
        info = info.get("conf", {})
    
    if not info : 
        raise AirflowFailException("No configuration received for MySQLTrigger task")
    
    es_repo = ElasticsearchRepo(info.get(Variable.get("elasticsearch_hosts")))
    es_service = ElasticsearchService(es_repo)
    
    es_source_config = build_es_source_model(
        name = info.get("project_name"),
        index = info.get("elasticsearch_index"),
        fields =info.get("fields"),
        query = info.get("query")
    )
    
    mysql_config = build_mysql_model(
        name = info.get("project_name"),
        db = info.get("mysql_db"),
        table = info.get("mysql_table"),
        user = info.get("user"),
        password = info.get("password")
    )

    chunks = es_service.get_chunk_count(
        index = info.get("index"),
        query = info.get("query")
    )

    return {
        "project_name" : info.get("project_name"),
        "es_source_config" : es_source_config,
        "mysql_config" : mysql_config,
        "chunk" : chunks
    }

@task(doc_md="Avro 스키마 등록")
def register_avro_schema(info: Dict[str, Any]) -> Dict[str, Any]:
    project_name = info.get("project_name")
    es_source_config = info.get("es_source_config")
    
    from repositories.schema_registry_repo import SchemaRegistryRepo
    from services.schema_registry_service import SchemaRegistryService
    from models.build_models import build_avro_schema
    
    repo = SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY"))
    services = SchemaRegistryService(repo)
    schema = build_avro_schema(project_name=project_name, fields=es_source_config.get("fields"))
    latest_version = services.register_schema(project_name, schema)
    info["schema_gersion"] = latest_version
    return info
    
