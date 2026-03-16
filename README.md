# Auto Pipeline (Airflow DAGs)

이 리포지토리는 Elasticsearch, Kafka, MySQL 간 데이터 파이프라인을 Airflow TaskFlow API로 구현합니다. 현재 구조는 Airflow 관례에 맞춰 `dags/`는 orchestration만 두고, 일반 애플리케이션 코드는 `pipeline/` 패키지로 분리해 두었습니다.

**핵심 기능**
- Elasticsearch → Kafka → MySQL 파이프라인(`mysql_pipeline_dag`)
- Elasticsearch → Kafka → Elasticsearch 파이프라인(`elasticsearch_pipeline_dag`)
- Avro 스키마 자동 생성 및 Schema Registry 등록
- Kafka Connect JDBC/Elasticsearch Sink 커넥터 자동 생성
- Elasticsearch 인덱스 복제 및 매핑 정제
- 진행 상태(`spark_task`) 업데이트

**권장 폴더 구조**
- [dags/mysql_pipe.py](dags/mysql_pipe.py): MySQL 대상 DAG 정의
- [dags/es_pipe.py](dags/es_pipe.py): Elasticsearch 대상 DAG 정의
- [pipeline/tasks/mysql_tasks.py](pipeline/tasks/mysql_tasks.py): Airflow TaskFlow task wrappers
- [pipeline/tasks/elasticsearch_tasks.py](pipeline/tasks/elasticsearch_tasks.py): Airflow TaskFlow task wrappers
- [pipeline/tasks/common.py](pipeline/tasks/common.py): task 공통 helper
- [pipeline/services/elasticsearch_service.py](pipeline/services/elasticsearch_service.py): 비즈니스 로직
- [pipeline/services/kafka_connect_service.py](pipeline/services/kafka_connect_service.py): 비즈니스 로직
- [pipeline/repositories/elasticsearch_repo.py](pipeline/repositories/elasticsearch_repo.py): 외부 시스템 접근
- [pipeline/repositories/mysql_repo.py](pipeline/repositories/mysql_repo.py): 외부 시스템 접근
- [pipeline/domain/build_models.py](pipeline/domain/build_models.py): 설정/스키마 builder
- [pipeline/config/logger.py](pipeline/config/logger.py): 로깅 설정
- [pipeline/config/elasticsearch_index.py](pipeline/config/elasticsearch_index.py): 인덱스 복제 유틸리티
- [pipeline/sql/queries/status.sql](pipeline/sql/queries/status.sql): SQL 리소스
- [tests/test_mysql_pipeline_dag.py](tests/test_mysql_pipeline_dag.py): DAG/helper 테스트
- [tests/test_elasticsearch_pipeline_dag.py](tests/test_elasticsearch_pipeline_dag.py): DAG/helper 테스트

**아키텍처 개요**
- `dags/`: DAG 선언과 task wiring만 담당
- `pipeline/tasks/`: Airflow task wrapper와 orchestration helper
- `pipeline/services/`: 순수 비즈니스 로직
- `pipeline/repositories/`: Elasticsearch, MySQL, Schema Registry, Kafka Connect 접근
- `pipeline/domain/`: 설정 객체와 스키마 builder
- `pipeline/config/`: 공통 설정과 로깅
- `tests/`: DAG 구조와 helper 검증

**주요 컴포넌트**
- `ElasticsearchService`: 검색, 청크 계산, 인덱스 생성 래퍼. [pipeline/services/elasticsearch_service.py](pipeline/services/elasticsearch_service.py)
- `KafkaConnectService`: JDBC/ES Sink 커넥터 생성과 토픽 관리. [pipeline/services/kafka_connect_service.py](pipeline/services/kafka_connect_service.py)
- `SchemaRegistryService`: 스키마 등록과 조회. [pipeline/services/schema_registry_service.py](pipeline/services/schema_registry_service.py)
- `MySQLService`: 상태 테이블 업데이트. [pipeline/services/mysql_service.py](pipeline/services/mysql_service.py)

**Airflow DAGs**
- `mysql_pipeline_dag`([dags/mysql_pipe.py](dags/mysql_pipe.py))
  `mySQLTrigger` → `register_avro_schema` → `create_jdbc_sink_connector` → `search_and_publish_elasticsearch`
- `elasticsearch_pipeline_dag`([dags/es_pipe.py](dags/es_pipe.py))
  `esTrigger` → `register_avro_schema` → `create_es_index` → `create_es_sink_connector` → `search_and_publish_elasticsearch`

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
