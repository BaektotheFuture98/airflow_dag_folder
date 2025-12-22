from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import Dict, Any
from app.config.logger import get_logger

log = get_logger(__name__)

@task(doc_md="API 수신 완료, 설정 파일 구성")
def esTrigger(**kwargs) -> Dict[str, Any]:
    from app.models.build_models import build_es_source_model, build_es_target_model

    dag_run = kwargs.get("dag_run")
    info = dag_run.conf if dag_run else {}

    if isinstance(info, dict) and 'conf' in info:
        info = info.get("conf", {})
    
    if not info : 
        log.error("Elasticsearch Trigger: No configuration received in dag_run.conf")
        raise AirflowFailException("No configuration received for ElasticsearchTrigger task")

    log.info("ElasticsearchTrigger: Building ES source and MySQL configs")
    
    es_source_config = build_es_source_model(
        project_name= info.get("project_name"),
        es_source_index = info.get("es_source_index"),
        query = info.get("query"),
        fields =info.get("fields")
    )

    es_target_config = build_es_target_model(
        project_name= info.get("project_name"),
        es_target_hosts = info.get("es_target_hosts"),
        es_target_index = info.get("es_target_index"),
        user= info.get("user"),
        password= info.get("password")
    )
    info = {
        "project_name" : info.get("project_name"),
        "es_source_config" : es_source_config,
        "es_target_config" : es_target_config
    }
    _insert_spark_task(info, status="S")
    return info

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


@task(doc_md = "Elasticsearch Index 생성")
def create_es_index(info: Dict[str, Any]) -> Dict[str, Any] : 
    from app.repositories.elasticsearch_repo import ElasticsearchRepo
    from app.services.elasticsearch_service import ElasticsearchService

    es_target_config = info.get("es_target_config")
    es_source_config = info.get("es_source_config")
    log.info(f"user, passwd : {es_target_config.get('user')}, {es_target_config.get('password')}")

    es_repo = ElasticsearchRepo(
        es_target_config.get("es_target_hosts"),(es_target_config.get("user"), es_target_config.get("password"))
    )

    es_service = ElasticsearchService(es_repo)

    source_index = es_source_config.get("es_source_index")
    target_index = es_target_config.get("es_target_index") 
    es_service.create_index_before_migration(source_index=source_index, target_index=target_index)
    
    return info


@task(doc_md = "elasticsearch_sink_connector 생성")
def create_es_sink_connector(info: Dict[str, Any]) -> Dict[str, Any] : 
    from app.repositories.kafka_connect_repo import KafkaConnectRepo
    from app.services.kafka_connect_service import KafkaConnectService

    kafka_connect_repo = KafkaConnectRepo(Variable.get("KAFKA_CONNECT"))
    kafka_connect_service = KafkaConnectService(kafka_connect_repo)

    log.info("KafkaConnect: Creating Elasticsearch Sink connectors and topics")
    es_target_config = info.get("es_target_config")
    
    kafka_connect_service.create_es_sink_connector(
            es_config = es_target_config
    )
    return info

    
@task(doc_md = "Elasticsearch 데이터 조회 및 전송")
def search_and_publish_elasticsearch(info: Dict[str, Any]) -> Dict[str, Any] : 
    
    # Schema Registry setup
    from app.repositories.schema_registry_repo import SchemaRegistryRepo
    from app.services.schema_registry_service import SchemaRegistryService

    log.info("SearchPublish: Initializing Schema Registry client")
    schema_repo = SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY"))
    schema_service = SchemaRegistryService(schema_repo)

    es_source_config = info.get("es_source_config")
    schema_version = info.get("schema_version")
    schema_name = info.get("project_name")
    es_target_config = info.get("es_target_config")
    latest_version = schema_service.get_schema_from_registry(schema_name)

    # Kafka producer with Avro serializer
    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import record_subject_name_strategy
    from confluent_kafka.schema_registry.avro import AvroSerializer

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_service.get_client(), 
        schema_str=latest_version.schema.schema_str,
        conf={
            'auto.register.schemas': False,
            'normalize.schemas': False,
            'use.schema.id': schema_version,
            'use.latest.version': False,
            'use.latest.with.metadata': None,
            'subject.name.strategy': record_subject_name_strategy
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
    from app.repositories.elasticsearch_repo import ElasticsearchRepo
    from app.services.elasticsearch_service import ElasticsearchService

    log.info("SearchPublish: Initializing Elasticsearch repo/service")
    es_repo = ElasticsearchRepo(Variable.get("ELASTICSEARCH_HOSTS"), (Variable.get("ELASTICSEARCH_USER"), Variable.get("ELASTICSEARCH_PASSWORD")))
    es_service = ElasticsearchService(es_repo)

    # Publish loop with pagination and chunk topic boundaries
    index = es_source_config.get("es_source_index")
    topic = es_target_config.get("es_target_index")
    fields = es_source_config.get("fields")
    chunk_size = 100000
    query = es_source_config.get("query")
    search_after = ""

    try:
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

                producer.produce(topic=topic, key=record.get("kw_docid"), value=record)

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
            es_service.close_client()
            producer.flush()
            _update_spark_task(info, status="C", set_end_date=True)
        except Exception:
            pass

    # Return info dict for downstream tasks; typing of original stub was invalid
    return info



def _insert_spark_task(info: Dict[str, Any], status: str = "W") -> Dict[str, Any]:
    """Insert a row into spark_task using mysql_service/mysql_repo.

    - status: 진행 상태( W:대기 / S:진행 / C:완료 / E:에러 )
    """
    import json
    from datetime import datetime

    # Build MySQL client for status DB
    from app.repositories.mysql_repo import MySQLRepo
    from app.services.mysql_service import MySQLService
    log.info("SparkTask: Inserting task with status=%s", status)
    
    mysql_repo = MySQLRepo(
        host=Variable.get("MYSQL_STATUS_HOST"),
        database=Variable.get("MYSQL_STATUS_DATABASE"),
        user=Variable.get("MYSQL_STATUS_USER"),
        password=Variable.get("MYSQL_STATUS_PASSWORD")
    )
    mysql_service = MySQLService(mysql_repo)

    es_source_config = info.get("es_source_config", {})
    es_target_config = info.get("es_target_config", {})

    def _short_host(raw):
        val = raw
        if isinstance(val, (list, tuple)):
            val = val[0] if val else ""
        if isinstance(val, str):
            val = val.split(",")[0].strip()
            val = val.replace("http://", "").replace("https://", "")
            val = val.split("/")[0]
            val = val.split(":")[0]
        else:
            val = str(val or "")
        return val[:20]

    host_short = _short_host(es_target_config.get("es_target_hosts"))

    # Normalize query and fields
    q = es_source_config.get("query")
    if isinstance(q, (dict, list)):
        try:
            q_str = json.dumps(q, ensure_ascii=False)
        except Exception:
            q_str = str(q)
    else:
        q_str = q or ""

    f = es_source_config.get("fields")
    if isinstance(f, (list, tuple)):
        field_str = ",".join(map(str, f))
    else:
        field_str = str(f or "")

    # Validate status code
    valid_status = {"W", "S", "C", "E"}
    if status not in valid_status:
        log.warning(f"SparkTask: Invalid status '{status}', defaulting to 'W'")
        status = "W"

    # Parameterized named placeholders to match dict keys
    insert_sql = (
        "INSERT INTO `spark_task` ("
        "`st_name`, `st_progress`, `st_user`, `st_status`, "
        "`st_query`, `st_field`, "
        "`st_host`, `st_db`, `st_table`, "
        "`st_db_id`, `st_db_pw`, "
        "`st_str_date`, `st_end_date`"
        ") VALUES ("
        "%(st_name)s, %(st_progress)s, %(st_user)s, %(st_status)s, "
        "%(st_query)s, %(st_field)s, "
        "%(st_host)s, %(st_db)s, %(st_table)s, "
        "%(st_db_id)s, %(st_db_pw)s, "
        "%(st_str_date)s, %(st_end_date)s"
        ")"
    )

    es_target_config = info.get("es_target_config", {})
    payload = {
        "st_name": info.get("project_name"),
        "st_progress": 0,
        "st_user": info.get("user_id"),
        "st_status": status,
        "st_query": (q_str[:512] if q_str else None),
        "st_field": (field_str[:512] if field_str else None),
        "st_host": host_short,
        "st_db": None,
        "st_table": es_target_config.get("es_target_index"),
        "st_db_id": es_target_config.get("user"),
        "st_db_pw": es_target_config.get("password"),
        "st_str_date": datetime.now(),
        "st_end_date": None,
    }

    log.info(
        "SparkTask: Inserting task — name=%s status=%s host=%s db=%s table=%s",
        payload.get("st_name"), payload.get("st_status"), payload.get("st_host"), payload.get("st_db"), payload.get("st_table"),
    )
    try:
        last_id = mysql_service.insert_query(insert_sql, payload)
        log.info("SparkTask: Inserted row id=%s status=%s", last_id, payload.get("st_status"))
        info["spark_task_id"] = last_id
    except Exception as e:
        log.exception("SparkTask: Insert failed for status=%s — error=%s", payload.get("st_status"), e)
    return info


def _update_spark_task(info: Dict[str, Any], status: str, set_end_date: bool = False) -> Dict[str, Any]:
    """Update `spark_task` row status and optionally set end date.

    - status: 진행 상태( W:대기 / S:진행 / C:완료 / E:에러 )
    - set_end_date: True일 때 `st_end_date`를 현재 시간으로 설정
    우선 `info['spark_task_id']`로 업데이트하고, 없으면 `project_name`의 진행중인 최신 행을 업데이트합니다.
    """
    from datetime import datetime
    from app.repositories.mysql_repo import MySQLRepo
    from app.services.mysql_service import MySQLService

    valid_status = {"W", "S", "C", "E"}
    if status not in valid_status:
        log.warning("SparkTask: Invalid status '%s', defaulting to 'W'", status)
        status = "W"

    mysql_repo = MySQLRepo(
        host=Variable.get("MYSQL_STATUS_HOST"),
        database=Variable.get("MYSQL_STATUS_DATABASE"),
        user=Variable.get("MYSQL_STATUS_USER"),
        password=Variable.get("MYSQL_STATUS_PASSWORD")
    )
    mysql_service = MySQLService(mysql_repo)

    st_id = info.get("spark_task_id")
    end_dt = datetime.now() if set_end_date else None

    if st_id:
        if set_end_date:
            update_sql = (
                "UPDATE `spark_task` SET "
                "`st_status`=%(st_status)s, `st_end_date`=%(st_end_date)s "
                "WHERE `st_seq`=%(st_seq)s"
            )
            params = {"st_status": status, "st_end_date": end_dt, "st_seq": st_id}
            log.info("SparkTask: Updating id=%s to status=%s, end_date=%s", st_id, status, end_dt)
        else:
            update_sql = (
                "UPDATE `spark_task` SET "
                "`st_status`=%(st_status)s "
                "WHERE `st_seq`=%(st_seq)s"
            )
            params = {"st_status": status, "st_seq": st_id}
            log.info("SparkTask: Updating id=%s to status=%s", st_id, status)

        affected = mysql_service.update_query(update_sql, params)
        log.info("SparkTask: Updated rows=%s", affected)
    else:
        if set_end_date:
            update_sql = (
                "UPDATE `spark_task` SET "
                "`st_status`=%(st_status)s, `st_end_date`=%(st_end_date)s "
                "WHERE `st_name`=%(st_name)s AND `st_end_date` IS NULL "
                "ORDER BY `st_seq` DESC LIMIT 1"
            )
            params = {"st_status": status, "st_end_date": end_dt, "st_name": info.get("project_name")}
            log.info("SparkTask: Updating latest active row for project=%s to status=%s, end_date=%s", info.get("project_name"), status, end_dt)
        else:
            update_sql = (
                "UPDATE `spark_task` SET "
                "`st_status`=%(st_status)s "
                "WHERE `st_name`=%(st_name)s AND `st_end_date` IS NULL "
                "ORDER BY `st_seq` DESC LIMIT 1"
            )
            params = {"st_status": status, "st_name": info.get("project_name")}
            log.info("SparkTask: Updating latest active row for project=%s to status=%s", info.get("project_name"), status)

        affected = mysql_service.update_query(update_sql, params)
        log.info("SparkTask: Updated rows=%s", affected)

    return info