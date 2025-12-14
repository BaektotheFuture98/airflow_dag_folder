from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import Dict, Any
from app.config.logger import get_logger
log = get_logger(__name__)


@task(doc_md="API 수신 완료, 설정 파일 구성")
def mySQLTrigger(**kwargs) -> Dict[str, Any]:
    from app.repositories.elasticsearch_repo import ElasticsearchRepo
    from app.services.elasticsearch_service import ElasticsearchService
    from app.models.build_models import build_es_source_model, build_mysql_config

    dag_run = kwargs.get("dag_run")
    info = dag_run.conf if dag_run else {}

    if isinstance(info, dict) and 'conf' in info:
        info = info.get("conf", {})
    
    if not info : 
        log.error("MySQLTrigger: No configuration received in dag_run.conf")
        raise AirflowFailException("No configuration received for MySQLTrigger task")
    
    es_repo = ElasticsearchRepo(Variable.get("ELASTICSEARCH_HOSTS"), (Variable.get("ELASTICSEARCH_USER"), Variable.get("ELASTICSEARCH_PASSWORD")))
    es_service = ElasticsearchService(es_repo)
    log.info("MySQLTrigger: Building ES source and MySQL configs")
    
    service = info.get("service")
    
    es_source_config = build_es_source_model(
        project_name = info.get("project_name"),
        index = info.get("elasticsearch_index") if info.get("elasticsearch_index") else "",
        query = info.get("query"),
        fields =info.get("fields")
    )
    
    chunks = es_service.get_chunk_count(
        index = info.get("elasticsearch_index"),
        query = info.get("query")
    )
    
    log.info(f"MySQLTrigger: Calculated chunks={chunks}")
    
    return {
        "project_name" : info.get("project_name"),
        "es_source_config" : es_source_config,
        service : "", 
        "chunks" : chunks,
    }

def _get_servcie_config(info : Dict) : 
    from app.models.build_models import build_es_source_model, build_mysql_config
    if info.get("service") == "mysql" : 
        return build_mysql_config(
            
        )
    pass    

@task(doc_md="Avro 스키마 등록")
def register_avro_schema(info: Dict[str, Any]) -> Dict[str, Any]:
    project_name = info.get("project_name")
    es_source_config = info.get("es_source_config")
    
    from app.repositories.schema_registry_repo import SchemaRegistryRepo
    from app.services.schema_registry_service import SchemaRegistryService
    from app.models.build_models import build_avro_schema
    
    repo = SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY"))
    services = SchemaRegistryService(repo)
    
    schema = build_avro_schema(project_name=project_name, fields=es_source_config.get("fields"))
    log.info(f"Schema: Registering Avro schema for project={project_name}")
    
    latest_version = services.register_schema(project_name, schema)
    info["schema_version"] = latest_version
    info["schema_str"] = schema
    log.info(f"Schema: Registered version={latest_version}")
    return info
