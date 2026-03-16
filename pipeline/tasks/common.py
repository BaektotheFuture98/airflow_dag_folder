from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer

from pipeline.config.logger import get_logger
from pipeline.repositories.elasticsearch_repo import ElasticsearchRepo
from pipeline.repositories.mysql_repo import MySQLRepo
from pipeline.repositories.schema_registry_repo import SchemaRegistryRepo
from pipeline.services.elasticsearch_service import ElasticsearchService
from pipeline.services.mysql_service import MySQLService
from pipeline.services.schema_registry_service import SchemaRegistryService

log = get_logger(__name__)

DEFAULT_CHUNK_SIZE = 100000
VALID_SPARK_TASK_STATUSES = {"W", "S", "C", "E"}


def get_dag_run_conf(kwargs: dict[str, Any], task_name: str) -> dict[str, Any]:
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    if isinstance(conf, dict) and "conf" in conf:
        conf = conf.get("conf", {})

    if not conf:
        message = f"No configuration received for {task_name}"
        log.error("%s task received an empty dag_run.conf", task_name)
        raise AirflowFailException(message)

    if not isinstance(conf, dict):
        raise AirflowFailException(f"Invalid dag_run.conf type for {task_name}: {type(conf)!r}")

    return conf


def get_schema_registry_service() -> SchemaRegistryService:
    return SchemaRegistryService(SchemaRegistryRepo(Variable.get("SCHEMA_REGISTRY")))


def get_status_mysql_service() -> MySQLService:
    mysql_repo = MySQLRepo(
        host=Variable.get("MYSQL_STATUS_HOST"),
        database=Variable.get("MYSQL_STATUS_DATABASE"),
        user=Variable.get("MYSQL_STATUS_USER"),
        password=Variable.get("MYSQL_STATUS_PASSWORD"),
    )
    return MySQLService(mysql_repo)


def get_default_elasticsearch_service() -> ElasticsearchService:
    repo = ElasticsearchRepo(
        Variable.get("ELASTICSEARCH_HOSTS"),
        (Variable.get("ELASTICSEARCH_USER"), Variable.get("ELASTICSEARCH_PASSWORD")),
    )
    return ElasticsearchService(repo)


def get_target_elasticsearch_service(es_target_config: dict[str, Any]) -> ElasticsearchService:
    repo = ElasticsearchRepo(
        es_target_config["es_target_hosts"],
        (es_target_config["user"], es_target_config["password"]),
    )
    return ElasticsearchService(repo)


def build_serializing_producer(schema_subject: str, schema_version: int) -> SerializingProducer:
    schema_service = get_schema_registry_service()
    latest_version = schema_service.get_schema_from_registry(schema_subject)
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_service.get_client(),
        schema_str=latest_version.schema.schema_str,
        conf={
            "auto.register.schemas": False,
            "normalize.schemas": False,
            "use.schema.id": schema_version,
            "use.latest.version": False,
            "use.latest.with.metadata": None,
            "subject.name.strategy": record_subject_name_strategy,
        },
    )
    return SerializingProducer(
        {
            "bootstrap.servers": Variable.get("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "plaintext",
            "value.serializer": avro_serializer,
        }
    )


def publish_elasticsearch_documents(
    *,
    info: dict[str, Any],
    topics: list[str],
    key_field: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    producer = build_serializing_producer(info["project_name"], info["schema_version"])
    es_service = get_default_elasticsearch_service()
    es_source_config = info["es_source_config"]
    search_after = None
    completed = False
    exhausted = False

    try:
        for topic in sorted(topics):
            sent_in_topic = 0
            log.info("Publishing Elasticsearch documents to topic=%s", topic)

            while sent_in_topic < chunk_size:
                hits = es_service.search(
                    index=es_source_config["es_source_index"],
                    fields=es_source_config["fields"],
                    query=es_source_config["query"],
                    search_after=search_after,
                )
                if not hits:
                    exhausted = True
                    break

                for hit in hits:
                    record = normalize_record(hit.get("_source", {}))
                    producer.produce(
                        topic=topic,
                        key=record.get(key_field) if key_field else None,
                        value=record,
                    )

                sent_in_topic += len(hits)
                search_after = get_search_after_token(hits)

                if search_after is None:
                    break

            producer.flush()
            if exhausted:
                break

        completed = True
        return info
    except Exception:
        update_spark_task(info, status="E", set_end_date=True)
        raise
    finally:
        if completed:
            update_spark_task(info, status="C", set_end_date=True)
        producer.flush()
        es_service.close_client()


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(record)
    if normalized.get("an_content") in ("", None):
        normalized["an_content"] = " "
    return normalized


def get_search_after_token(hits: list[dict[str, Any]]) -> Any | None:
    if not hits:
        return None

    last_hit = hits[-1]
    sort_values = last_hit.get("sort") or []
    if not sort_values:
        return None

    return sort_values[0]


def build_chunked_topic_names(project_name: str, table_name: str, chunks: int) -> list[tuple[str, str]]:
    topic_names: list[tuple[str, str]] = []

    for chunk in range(1, chunks + 1):
        if chunk == 1:
            topic_names.append((project_name, table_name))
            continue

        suffix = str(chunk).zfill(3)
        topic_names.append((f"{project_name}-{suffix}", f"{table_name}_{suffix}"))

    return topic_names


def update_spark_task(
    info: dict[str, Any],
    *,
    status: str,
    set_end_date: bool = False,
    st_seq: int | None = None,
) -> int:
    if status not in VALID_SPARK_TASK_STATUSES:
        log.warning("SparkTask received invalid status=%s, defaulting to W", status)
        status = "W"

    task_id = st_seq if st_seq is not None else info.get("st_seq")
    if not task_id:
        log.info("SparkTask update skipped because st_seq is missing")
        return 0

    mysql_service = get_status_mysql_service()

    if set_end_date:
        query = (
            "UPDATE `spark_task` SET "
            "`st_status`=%(st_status)s, `st_end_date`=%(st_end_date)s, `st_progress`=%(st_progress)s "
            "WHERE `st_seq`=%(st_seq)s"
        )
        params = {
            "st_status": status,
            "st_end_date": datetime.now(),
            "st_progress": 100,
            "st_seq": task_id,
        }
    else:
        query = "UPDATE `spark_task` SET `st_status`=%(st_status)s WHERE `st_seq`=%(st_seq)s"
        params = {"st_status": status, "st_seq": task_id}

    affected = mysql_service.update_query(query, params)
    log.info("SparkTask updated rows=%s for st_seq=%s", affected, task_id)
    return affected
