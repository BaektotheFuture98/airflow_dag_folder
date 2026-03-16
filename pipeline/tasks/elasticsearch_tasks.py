from __future__ import annotations

from typing import Any

from airflow.sdk import Variable, task

from pipeline.config.logger import get_logger
from pipeline.domain.build_models import build_avro_schema, build_es_source_model, build_es_target_model
from pipeline.repositories.kafka_connect_repo import KafkaConnectRepo
from pipeline.services.kafka_connect_service import KafkaConnectService
from pipeline.tasks.common import (
    get_dag_run_conf,
    get_schema_registry_service,
    get_target_elasticsearch_service,
    publish_elasticsearch_documents,
)

log = get_logger(__name__)


@task(doc_md="API 수신 완료, 설정 파일 구성")
def esTrigger(**kwargs) -> dict[str, Any]:
    info = get_dag_run_conf(kwargs, "esTrigger")

    es_source_config = build_es_source_model(
        project_name=info.get("project_name"),
        es_source_index=info.get("es_source_index"),
        query=info.get("query"),
        fields=info.get("fields"),
    )
    es_target_config = build_es_target_model(
        project_name=info.get("project_name"),
        es_target_hosts=info.get("es_target_hosts"),
        es_target_index=info.get("es_target_index"),
        user=info.get("user"),
        password=info.get("password"),
    )

    return {
        "project_name": info.get("project_name"),
        "st_seq": info.get("st_seq"),
        "es_source_config": es_source_config,
        "es_target_config": es_target_config,
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


@task(doc_md="Elasticsearch Index 생성")
def create_es_index(info: dict[str, Any]) -> dict[str, Any]:
    es_service = get_target_elasticsearch_service(info["es_target_config"])
    source_index = info["es_source_config"]["es_source_index"]
    target_index = info["es_target_config"]["es_target_index"]
    try:
        es_service.create_index_before_migration(source_index=source_index, target_index=target_index)
    finally:
        es_service.close_client()
    log.info("Prepared Elasticsearch target index=%s from source=%s", target_index, source_index)
    return info


@task(doc_md="elasticsearch_sink_connector 생성")
def create_es_sink_connector(info: dict[str, Any]) -> dict[str, Any]:
    kafka_connect_service = KafkaConnectService(KafkaConnectRepo(Variable.get("KAFKA_CONNECT")))
    kafka_connect_service.create_es_sink_connector(es_config=info["es_target_config"])
    log.info("Created Elasticsearch sink connector for index=%s", info["es_target_config"]["es_target_index"])
    return info


@task(doc_md="Elasticsearch 데이터 조회 및 전송")
def search_and_publish_elasticsearch(info: dict[str, Any]) -> dict[str, Any]:
    topic = info["es_target_config"]["es_target_index"]
    return publish_elasticsearch_documents(info=info, topics=[topic], key_field="kw_docid")
