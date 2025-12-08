from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import List, Dict, List, Any

@task(doc_md="API 수신 완료, 설정 파일 구성")
def MySQLTrigger(**kwargs) -> Dict[str, Any]:
    from repositories.elasticsearch_repo import ElasticsearchRepo
    from services.elasticsearch_service import ElasticsearchService
    from models.build_models import build_es_source_model, build_jdbc_sink_config

    dag_run = kwargs.get("dag_run")
    info = dag_run.conf if dag_run else {}

    if isinstance(info, dict) and 'conf' in info:
        info = info.get("conf", {})
    
    if not info : 
        raise AirflowFailException("No configuration received for MySQLTrigger task")
    
    es_repo = ElasticsearchRepo(Variable.get("elasticsearch_hosts"))
    es_service = ElasticsearchService(es_repo)
    
    es_source_config = build_es_source_model(
        name = info.get("project_name"),
        index = info.get("elasticsearch_index"),
        fields =info.get("fields"),
        query = info.get("query")
    )

    mysql_config = mysql_config(
        database = info.get("mysql_db"),
        user = info.get("user"),
        password = info.get("password"),
        table = info.get("mysql_table")
    )
    
    chunks = es_service.get_chunk_count(
        index = info.get("index"),
        query = info.get("query")
    )

    return {
        "project_name" : info.get("project_name"),
        "es_source_config" : es_source_config,
        "mysql_config" : mysql_config,
        "chunks" : chunks
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
    info["schema_version"] = latest_version
    return info

@task(doc_md = "JdbcSinkConnector 생성")
def create_jdbc_sink_connector(info: Dict[str, Any]) -> Dict[str, Any] : 
    from repositories.kafka_connect_repo import KafkaConnectRepo
    from services.kafka_connect_service import KafkaConnectService

    kafka_connect_repo = KafkaConnectRepo(Variable.get("KAFKA_CONNECT"))
    kafka_connect_service = KafkaConnectService(kafka_connect_repo)

    conn_topic_list = kafka_connect_service.create_connector(
        chunks = info.get("chunks"),
        service_name=info.get("project_name"),
        mysql_config = info.get("mysql_config")
    )
    
    info["conn_topic_list"] = conn_topic_list
    return info

@task(doc_md = "Elasticsearch 데이터 조회 및 전송")
def search_and_publish_elasticsearch(info: Dict[str, Any]) -> List[str, Any] : 
    from repositories.schema_registry_repo import SchemaRegistryRepo
    from services.schema_registry_service import SchemaRegistryService
    
    repo = SchemaRegistryRepo()
    schema_service = SchemaRegistryService(repo)
    
    es_source_config = info.get("es_source_config")
    schema_version = info.get("schema_version")
    
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.schema_registry import record_subject_name_strategy
    
    avro_serializer = AvroSerializer(
        schema_registry_client = schema_service.get_client(),
        schema_str = None, 
        config = {
            "auto.register.schemas" : False, 
            "use.schema.id" : schema_version,
            "subject.name.strategy" : record_subject_name_strategy
        }
    )

    kafka_producer = SerializingProducer(
        {
            "bootstrap.servers" : "192.168.125.24:9092",
            "security.protocol" : "plaintext", 
            "value.serializer" : avro_serializer
        }
    )
    
    from repositories.elasticsearch_repo import ElasticsearchRepo
    from services.elasticsearch_service import ElasticsearchService
    es_repo = ElasticsearchRepo(
            hosts=es_source_config.get("hosts"), 
            basic_auth=(es_source_config.get("user"), es_source_config.get("password"))
        )
    es_service = ElasticsearchService(es_repo)
    
    
    
    search_after = ""
    while(True) : 
        es_search = es_service.search(
                index = es_source_config.get("index"), 
                query = es_source_config.get("query"), 
                search_after = search_after
            )
    
        if len(es_search) == 0 : 
            break
        
        