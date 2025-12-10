from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import Dict, Any
from mysql_pipeline.config.logger import get_logger

log = get_logger(__name__)

@task(doc_md="API 수신 완료, 설정 파일 구성")
def mySQLTrigger(**kwargs) -> Dict[str, Any]:
    from mysql_pipeline.repositories.elasticsearch_repo import ElasticsearchRepo
    from mysql_pipeline.services.elasticsearch_service import ElasticsearchService
    from mysql_pipeline.models.build_models import build_es_source_model, build_mysql_config

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
    
    es_source_config = build_es_source_model(
        project_name = info.get("project_name"),
        index = info.get("elasticsearch_index") if info.get("elasticsearch_index") else "",
        query = info.get("query"),
        fields =info.get("fields")
    )

    mysql_config = build_mysql_config(
        host = info.get("host"),
        database = info.get("database"),
        user = info.get("user"),
        password = info.get("password"),
        table = info.get("table")
    )
    
    chunks = es_service.get_chunk_count(
        index = info.get("elasticsearch_index"),
        query = info.get("query")
    )
    log.info(f"MySQLTrigger: Calculated chunks={chunks}")

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
    
    from mysql_pipeline.repositories.schema_registry_repo import SchemaRegistryRepo
    from mysql_pipeline.services.schema_registry_service import SchemaRegistryService
    from mysql_pipeline.models.build_models import build_avro_schema
    
    repo = SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY"))
    services = SchemaRegistryService(repo)
    
    schema = build_avro_schema(project_name=project_name, fields=es_source_config.get("fields"))
    log.info(f"Schema: Registering Avro schema for project={project_name}")
    
    latest_version = services.register_schema(project_name, schema)
    info["schema_version"] = latest_version
    info["schema_str"] = schema
    log.info(f"Schema: Registered version={latest_version}")
    
    return info

@task(doc_md = "JdbcSinkConnector 생성")
def create_jdbc_sink_connector(info: Dict[str, Any]) -> Dict[str, Any] : 
    from mysql_pipeline.repositories.kafka_connect_repo import KafkaConnectRepo
    from mysql_pipeline.services.kafka_connect_service import KafkaConnectService

    kafka_connect_repo = KafkaConnectRepo(Variable.get("KAFKA_CONNECT"))
    kafka_connect_service = KafkaConnectService(kafka_connect_repo)

    log.info("KafkaConnect: Creating JDBC Sink connectors and topics")
    conn_topic_list = kafka_connect_service.create_connector(
        chunks = info.get("chunks"),
        service_name=info.get("project_name"),
        mysql_config = info.get("mysql_config")
    )
    
    info["conn_topic_list"] = conn_topic_list
    log.info(f"KafkaConnect: Created topics count={len(conn_topic_list)}")
    return info

@task(doc_md = "Elasticsearch 데이터 조회 및 전송")
def search_and_publish_elasticsearch(info: Dict[str, Any]) -> Dict[str, Any] : 
    
    # Schema Registry setup
    from mysql_pipeline.repositories.schema_registry_repo import SchemaRegistryRepo
    from mysql_pipeline.services.schema_registry_service import SchemaRegistryService

    log.info("SearchPublish: Initializing Schema Registry client")
    schema_repo = SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY"))
    schema_service = SchemaRegistryService(schema_repo)

    es_source_config = info.get("es_source_config")
    schema_version = info.get("schema_version")
    schema_name = info.get("project_name")
    latest_version = schema_service.get_schema_from_registry(schema_name)
    # Kafka producer with Avro serializer
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import record_subject_name_strategy
    from confluent_kafka.schema_registry.avro import AvroSerializer


    avro_serializer = AvroSerializer(
        schema_registry_client=schema_service.get_client(), 
        schema_str=latest_version.schema.schema_str,
        conf={
            "auto.register.schemas": False,
            "use.schema.id": schema_version,
            "subject.name.strategy": record_subject_name_strategy
        }
    )

    log.info("SearchPublish: Initializing Kafka producer")
    producer = SerializingProducer(
        {
            "bootstrap.servers": Variable.get("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "plaintext",
            "value.serializer": avro_serializer
        }
    )

    # Elasticsearch repo/service
    from mysql_pipeline.repositories.elasticsearch_repo import ElasticsearchRepo
    from mysql_pipeline.services.elasticsearch_service import ElasticsearchService

    log.info("SearchPublish: Initializing Elasticsearch repo/service")
    es_repo = ElasticsearchRepo(Variable.get("ELASTICSEARCH_HOSTS"), (Variable.get("ELASTICSEARCH_USER"), Variable.get("ELASTICSEARCH_PASSWORD")))
    es_service = ElasticsearchService(es_repo)
    

    # Publish loop with pagination and chunk topic boundaries
    index = es_source_config.get("index")
    topic_list = info.get("conn_topic_list")
    fields = es_source_config.get("fields")
    chunk_size = 100000
    query = es_source_config.get("query")
    search_after = ""

    try:
        log.info(f"SearchPublish: Start publishing. index={index}, topics={len(topic_list)}, chunk_size={chunk_size}")
        for topic in topic_list:
            sent_in_topic = 0
            log.info(f"SearchPublish: Processing topic={topic}")

            while True:
                hits = es_service.search(index=index, fields=fields, query=query, search_after=search_after)
                log.info(f"SearchPublish: Retrieved hits={hits} search_after={search_after}")
                if not hits:
                    log.info("SearchPublish: No more hits, break")
                    break

                for hit in hits:
                    record = hit.get("_source")
                    if record.get("an_content") == '' or record.get("an_content") is None:
                        record["an_content"] = " "

                    producer.produce(topic=topic, value=record)

                # Update counters and pagination token
                sent_in_topic += len(hits)
                log.info(f"SearchPublish: Batch size={len(hits)} total_in_topic={sent_in_topic}")

                try:
                    search_after = hits[-1].get("sort")[0]
                except Exception:
                    search_after = None

                if sent_in_topic >= chunk_size:
                    log.info("SearchPublish: Chunk size reached, flushing and moving to next topic")
                    producer.flush()
                    break

        log.info("SearchPublish: Flushing producer at end")
        producer.flush()
    finally:
        try:
            producer.flush()
        except Exception:
            pass

    # Return info dict for downstream tasks; typing of original stub was invalid
    return info