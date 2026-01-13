# Auto Pipeline (Airflow DAGs)

이 리포지토리는 Elasticsearch ↔ Kafka ↔ MySQL 간 데이터 파이프라인을 Airflow TaskFlow API로 구현한 DAG들의 모음입니다. 두 개의 주요 DAG이 있으며, 스키마 레지스트리/카프카 커넥트/Elasticsearch 인덱스 생성까지 자동화합니다.

**핵심 기능**
- Elasticsearch → Kafka → MySQL 파이프라인(`mysql_pipeline_dag`)
- Elasticsearch → Kafka → Elasticsearch 파이프라인(`elasticsearch_pipeline_dag`)
- Avro 스키마 자동 생성/등록(Confluent Schema Registry)
- Kafka Connect JDBC/Elasticsearch Sink 커넥터 자동 생성
- Elasticsearch 인덱스 복제 및 매핑 정제 후 대상 인덱스 생성
- 진행 상태(`spark_task` 테이블) 업데이트

**폴더 구조**
- [app/es_pipe.py](app/es_pipe.py): `elasticsearch_pipeline_dag` 정의
- [app/mysql_pipe.py](app/mysql_pipe.py): `mysql_pipeline_dag` 정의
- [app/tasks/elasticsearch_tasks.py](app/tasks/elasticsearch_tasks.py): ES→Kafka→ES TaskFlow 작업들
- [app/tasks/mysql_tasks.py](app/tasks/mysql_tasks.py): ES→Kafka→MySQL TaskFlow 작업들
- [app/models/build_models.py](app/models/build_models.py): 스키마/커넥터 설정 빌더
- [app/services/*](app/services): 비즈니스 로직 서비스 레이어
- [app/repositories/*](app/repositories): 외부 시스템 접근 레포지토리 레이어
- [app/config/elasticsearch_index.py](app/config/elasticsearch_index.py): 인덱스 복제/생성 유틸리티
- [app/config/logger.py](app/config/logger.py): 통일된 로깅 설정
- [pyproject.toml](pyproject.toml), [requirements.txt](requirements.txt): 의존성 관리

**아키텍처 개요**
- Source: Elasticsearch 인덱스(검색/페이지네이션)
- Transport: Kafka(Avro 직렬화, `SerializingProducer`)
- Sink: MySQL(JDBC Sink Connector) 또는 Elasticsearch(ES Sink Connector)
- Schema: Confluent Schema Registry(RecordNameStrategy)
- Orchestrator: Airflow 3.x Task SDK(`@dag`, `@task`)

**주요 컴포넌트**
- `ElasticsearchService`: 검색/청크 계산/인덱스 생성 래퍼. [app/services/elasticsearch_service.py](app/services/elasticsearch_service.py)
- `KafkaConnectService`: JDBC/ES Sink 커넥터 생성 및 토픽 관리. [app/services/kafka_connect_service.py](app/services/kafka_connect_service.py)
- `SchemaRegistryService`: 스키마 등록/조회. [app/services/schema_registry_service.py](app/services/schema_registry_service.py)
- `MySQLService`: 상태 테이블(`spark_task`) 업데이트. [app/services/mysql_service.py](app/services/mysql_service.py)

**Airflow DAGs**
- `mysql_pipeline_dag`([app/mysql_pipe.py](app/mysql_pipe.py))
	- `mySQLTrigger` → `register_avro_schema` → `create_jdbc_sink_connector` → `search_and_publish_elasticsearch`
	- ES에서 조회한 레코드를 Avro로 Kafka에 Publish → JDBC Sink가 MySQL 테이블로 적재
- `elasticsearch_pipeline_dag`([app/es_pipe.py](app/es_pipe.py))
	- `esTrigger` → `register_avro_schema` → `create_es_index` → `create_es_sink_connector` → `search_and_publish_elasticsearch`
	- 대상 ES 인덱스 자동 생성 후 Kafka ES Sink로 적재

**필수 Airflow Variables**
- `ELASTICSEARCH_HOSTS`: 예 `http://host1:9200,http://host2:9200`
- `ELASTICSEARCH_USER`, `ELASTICSEARCH_PASSWORD`
- `ELASTICSEARCH_INDEX`: 소스 인덱스 기본값(옵션)
- `SCHEMA_REGISTRY`: 예 `http://<schema-registry-host>:8081`
- `KAFKA_CONNECT`: 예 `http://<kafka-connect-host>:8083`
- `KAFKA_BOOTSTRAP_SERVERS`: 예 `host:9092`
- 상태 DB용: `MYSQL_STATUS_HOST`, `MYSQL_STATUS_DATABASE`, `MYSQL_STATUS_USER`, `MYSQL_STATUS_PASSWORD`

**DAG 트리거 입력(JSON `dag_run.conf`)**
- 공통 필드
	- `project_name`: 파이프라인/토픽/커넥터 명의 베이스
	- `st_seq`: 상태 업데이트용 시퀀스(선택)
	- `es_source_index`: 소스 ES 인덱스명
	- `query`: ES `query_string` 쿼리 문자열
	- `fields`: `_source` 필드 리스트
- MySQL 파이프라인 추가 필드
	- `mysql_host`, `mysql_database`, `mysql_table`, `user`, `password`
- ES 파이프라인 추가 필드
	- `es_target_hosts`, `es_target_index`, `user`, `password`

예시: MySQL 파이프라인 트리거

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

예시: Elasticsearch 파이프라인 트리거

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

**동작 상세**
- 스키마 등록: `fields`를 기준으로 단순 타입 추론(`*in*` 포함시 `int`, 그 외 `string`)하여 Avro 스키마 생성/등록
- Elasticsearch 검색: `kw_docid` 기준 오름차순 정렬 + `search_after`로 페이지네이션
- Publish: 레코드별 `an_content`가 비거나 `None`이면 공백으로 보정 후 Kafka 전송
- 청크 분할(MySQL): `ElasticsearchService.get_chunk_count()`로 전체 건수/청크 수 계산 후 커넥터/토픽 분할 생성
- 인덱스 생성(ES): 원본 인덱스 설정/매핑을 가져와 `normalizer`만 유지, custom analyzer는 `standard`로 정제, 샤드/레플리카를 1:1로 생성
- 진행 상태: `_update_spark_task()`로 `spark_task` 테이블 상태/완료 시간 갱신

**요구사항**
- Airflow 3.x 및 Task SDK, Elasticsearch, Kafka, Schema Registry, Kafka Connect, MySQL 접근 가능
- 의존성은 [pyproject.toml](pyproject.toml) 또는 [requirements.txt](requirements.txt) 참고

**개발/운영 팁**
- Airflow Variables를 먼저 정확히 설정하세요.
- 대용량 처리 시 `chunk_size`(기본 100,000)와 페이지네이션 `size`(기본 100)를 환경에 맞게 조정하려면 서비스 코드 수정이 필요합니다.
- ES 인덱스 생성 로직은 대상으로 custom analyzer 제거/치환을 수행합니다. 검색/인덱싱 영향 검토 후 사용하세요.

문의나 개선 요청은 코드 내 TODO 주석과 서비스 레이어를 기준으로 확장하면 됩니다.
