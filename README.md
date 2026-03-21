# Auto Pipeline (Airflow DAGs)

## 이 코드가 뭘 하는 건가요?

이 저장소는 **Elasticsearch 데이터를 다른 저장소로 이관/적재**하기 위한 Airflow DAG 프로젝트입니다.

- 목적: Elasticsearch 검색 결과를 Kafka로 발행하고, 최종적으로 MySQL 또는 Elasticsearch에 적재
- 사용 시점: 대량 데이터 이관, 재색인, 배치 적재 작업이 필요할 때
- 실행 방식: 기본 `schedule=None`이라서 주기 실행보다 **필요 시 수동 트리거** 중심
- 핵심 가치: 스키마 등록, 커넥터 생성, 전송/상태 업데이트를 DAG 한 번으로 자동화

## 빠른 이해 (30초)

1. `dag_run.conf`로 소스/타겟/필드를 전달한다.
2. DAG가 Avro 스키마를 등록하고 Kafka Connect Sink를 만든다.
3. Elasticsearch 데이터를 페이지네이션으로 읽어 Kafka로 보낸다.
4. Sink Connector가 최종 목적지(MySQL 또는 Elasticsearch)에 적재한다.

즉, 이 코드는 **"Elasticsearch -> Kafka -> 목적지 저장소" 이관 파이프라인 자동화 코드**입니다.

## 한 줄 다이어그램

`Input (dag_run.conf)` -> `TaskFlow DAG` -> `Schema Registry + Kafka Connect 준비` -> `Elasticsearch 검색/발행` -> `Output (MySQL 또는 Elasticsearch)`

## What This Project Does

- `mysql_pipeline_dag`: Elasticsearch -> Kafka -> MySQL
- `elasticsearch_pipeline_dag`: Elasticsearch -> Kafka -> Elasticsearch
- Avro 스키마 자동 생성 및 Schema Registry 등록
- Kafka Connect Sink Connector 자동 생성
- 상태 테이블(`spark_task`) 업데이트

## Project Layout

- [dags/mysql_pipe.py](dags/mysql_pipe.py): MySQL 대상 DAG
- [dags/es_pipe.py](dags/es_pipe.py): Elasticsearch 대상 DAG
- [pipeline/tasks/mysql_tasks.py](pipeline/tasks/mysql_tasks.py): MySQL DAG task wrappers
- [pipeline/tasks/elasticsearch_tasks.py](pipeline/tasks/elasticsearch_tasks.py): Elasticsearch DAG task wrappers
- [pipeline/tasks/common.py](pipeline/tasks/common.py): 공통 Task helper (producer, status update 등)
- [pipeline/services/](pipeline/services): 비즈니스 로직 계층
- [pipeline/repositories/](pipeline/repositories): 외부 시스템 접근 계층
- [pipeline/domain/](pipeline/domain): config/schema builder 및 domain 객체
- [pipeline/config/](pipeline/config): 로깅/설정 유틸
- [pipeline/sql/queries/status.sql](pipeline/sql/queries/status.sql): SQL 리소스
- [tests/](tests): DAG/helper 테스트

## Execution Flow

### `mysql_pipeline_dag`
`mySQLTrigger` -> `register_avro_schema` -> `create_jdbc_sink_connector` -> `search_and_publish_elasticsearch`

### `elasticsearch_pipeline_dag`
`esTrigger` -> `register_avro_schema` -> `create_es_index` -> `create_es_sink_connector` -> `search_and_publish_elasticsearch`

## Prerequisites

- Python 3.12+
- Apache Airflow 3.x
- Elasticsearch, Kafka, Schema Registry, Kafka Connect, MySQL 접근 가능 환경
- Dependencies: [pyproject.toml](pyproject.toml) 또는 [requirements.txt](requirements.txt)

## Required Airflow Variables

| Variable | Required | Example |
| --- | --- | --- |
| `ELASTICSEARCH_HOSTS` | Yes | `http://host1:9200,http://host2:9200` |
| `ELASTICSEARCH_USER` | Yes | `elastic` |
| `ELASTICSEARCH_PASSWORD` | Yes | `secret` |
| `ELASTICSEARCH_INDEX` | Optional | `source_index` |
| `SCHEMA_REGISTRY` | Yes | `http://schema-registry:8081` |
| `KAFKA_CONNECT` | Yes | `http://kafka-connect:8083` |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` |
| `MYSQL_STATUS_HOST` | Conditional | `10.0.0.10` |
| `MYSQL_STATUS_DATABASE` | Conditional | `status_db` |
| `MYSQL_STATUS_USER` | Conditional | `status_user` |
| `MYSQL_STATUS_PASSWORD` | Conditional | `status_password` |

`MYSQL_STATUS_*`는 `spark_task` 상태 업데이트를 사용할 때 필요합니다.

## `dag_run.conf` Schema

### Common Fields

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `project_name` | string | Yes | 파이프라인/토픽/커넥터 이름 prefix |
| `st_seq` | int | No | 상태 업데이트용 시퀀스 |
| `es_source_index` | string | Yes | 소스 Elasticsearch 인덱스 |
| `query` | string | Yes | Elasticsearch `query_string.query` 값 |
| `fields` | array[string] | Yes | `_source` 필드 목록 |

### MySQL Pipeline Additional Fields

| Key | Type | Required |
| --- | --- | --- |
| `mysql_host` | string | Yes |
| `mysql_database` | string | Yes |
| `mysql_table` | string | Yes |
| `user` | string | Yes |
| `password` | string | Yes |

### Elasticsearch Pipeline Additional Fields

| Key | Type | Required |
| --- | --- | --- |
| `es_target_hosts` | string | Yes |
| `es_target_index` | string | Yes |
| `user` | string | Yes |
| `password` | string | Yes |

## Trigger Examples

### MySQL Pipeline

```bash
airflow dags trigger mysql_pipeline_dag \
  --conf '{
    "project_name": "my_project",
    "st_seq": 123,
    "es_source_index": "source_index",
    "query": "kw_docid:*",
    "fields": ["kw_docid", "an_content"],
    "mysql_host": "10.0.0.10",
    "mysql_database": "dw",
    "mysql_table": "my_table",
    "user": "dbuser",
    "password": "secret"
  }'
```

### Elasticsearch Pipeline

```bash
airflow dags trigger elasticsearch_pipeline_dag \
  --conf '{
    "project_name": "my_project",
    "st_seq": 456,
    "es_source_index": "source_index",
    "query": "kw_docid:*",
    "fields": ["kw_docid", "an_content"],
    "es_target_hosts": "http://es-target:9200",
    "es_target_index": "target_index",
    "user": "elastic",
    "password": "elastic"
  }'
```

## Operational Notes

- 검색은 `kw_docid` 오름차순 + `search_after` 기반 페이지네이션으로 수행됩니다.
- `an_content`가 빈 값일 때는 공백으로 정규화해서 전송합니다.
- MySQL 파이프라인은 전체 건수를 기준으로 connector/topic을 청크 분할 생성합니다.
- ES 인덱스 생성 시 custom analyzer를 정제하는 로직이 있으므로 운영 전 매핑 영향 검토가 필요합니다.
- 대용량 처리 튜닝은 `chunk_size`, pagination `size`를 코드에서 조정해야 합니다.

## Development

```bash
# syntax check
uv run python -m compileall dags pipeline tests
```

테스트 환경이 준비되어 있다면 `pytest`를 사용해 DAG 및 helper 테스트를 실행하세요.
