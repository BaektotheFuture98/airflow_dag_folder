from __future__ import annotations

from typing import Any

from airflow.sdk import Variable, task

from pipeline.config.logger import get_logger
from pipeline.domain.build_models import build_avro_schema, build_es_source_model, build_mysql_config
from pipeline.repositories.kafka_connect_repo import KafkaConnectRepo
from pipeline.services.kafka_connect_service import KafkaConnectService
from pipeline.tasks.common import (
    build_chunked_topic_names,
    get_dag_run_conf,
    get_default_elasticsearch_service,
    get_schema_registry_service,
    publish_elasticsearch_documents,
)

log = get_logger(__name__)


@task(doc_md="API 수신 완료, 설정 파일 구성")
def mySQLTrigger(**kwargs) -> dict[str, Any]:
    info = get_dag_run_conf(kwargs, "mySQLTrigger")
    es_service = get_default_elasticsearch_service()

    es_source_config = build_es_source_model(
        project_name=info.get("project_name"),
        es_source_index=info.get("es_source_index"),
        query=info.get("query"),
        fields=info.get("fields"),
    )
    mysql_config = build_mysql_config(
        mysql_host=info.get("mysql_host"),
        mysql_database=info.get("mysql_database"),
        mysql_table=info.get("mysql_table"),
        user=info.get("user"),
        password=info.get("password"),
    )

    try:
        chunks = es_service.get_chunk_count(
            index=info.get("es_source_index"),
            query=info.get("query"),
        )
    finally:
        es_service.close_client()
    log.info("MySQL trigger built source config for project=%s with chunks=%s", info.get("project_name"), chunks)

    return {
        "project_name": info.get("project_name"),
        "st_seq": info.get("st_seq"),
        "es_source_config": es_source_config,
        "mysql_config": mysql_config,
        "chunks": chunks,
    }


@task(doc_md="Avro 스키마 등록")
def register_avro_schema(info: dict[str, Any]) -> dict[str, Any]:
    schema_service = get_schema_registry_service()
    schema = build_avro_schema(
        project_name=info["project_name"],
        fields=info["es_source_config"]["fields"],
    )
    schema_version = schema_service.register_schema(info["project_name"], schema)
    log.info("Registered Avro schema for project=%s version=%s", info["project_name"], schema_version)

    return {
        **info,
        "schema_version": schema_version,
        "schema_str": schema,
    }


@task(doc_md="JdbcSinkConnector 생성")
def create_jdbc_sink_connector(info: dict[str, Any]) -> dict[str, Any]:
    kafka_connect_service = KafkaConnectService(KafkaConnectRepo(Variable.get("KAFKA_CONNECT")))
    mysql_config = info["mysql_config"]

    topic_names = build_chunked_topic_names(
        project_name=info["project_name"],
        table_name=mysql_config["mysql_table"],
        chunks=info["chunks"],
    )

    for service_name, table_name in topic_names:
        kafka_connect_service.create_jdbc_connector(
            service_name=service_name,
            mysql_config={**mysql_config, "mysql_table": table_name},
        )

    topic_list = kafka_connect_service.get_sink_topic_list()
    log.info("Created JDBC sink connectors for topics=%s", topic_list)
    return {**info, "topic_list": topic_list}


@task(doc_md="Elasticsearch 데이터 조회 및 전송")
def search_and_publish_elasticsearch(info: dict[str, Any]) -> dict[str, Any]:
    return publish_elasticsearch_documents(info=info, topics=info["topic_list"])
