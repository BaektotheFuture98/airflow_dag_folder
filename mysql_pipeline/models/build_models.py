"""
    API 서버를 통해 유효성 검사를 끝냈다고 가정
"""

from typing import List, Dict
from airflow.sdk import Variable
import json

"""
    MySQLTrigger Task 
"""
def build_es_source_model(project_name: str, index: str, query: Dict, fields: List[str]) -> Dict:
    hosts_str = Variable.get("ELASTICSEARCH_HOSTS", default_var="")
    hosts = [h.strip() for h in hosts_str.split(",") if h.strip()]
    return {
        "project_name": project_name,
        "query": query,
        "fields": fields,
        "index": index if index else Variable.get("ELASTICSEARCH_INDEX"),
        "hosts": hosts,
        "user": Variable.get("ELASTICSEARCH_USER"),
        "password": Variable.get("ELASTICSEARCH_PASSWORD"),
    }
    
def build_mysql_config(database: str, user: str, password: str, table: str | None = None) -> Dict[str, str]:
    return {
        "database": database,
        "user": user,
        "password": password,
        "table": table
    }


"""
    Register_Avro_Schema Task 
"""
def build_avro_schema(project_name: str, fields: List[str]) -> str:
    avro_fields = []
    for field_name in fields:
        field_type = "int" if ("in" in field_name) else ["string", "null"]
        avro_fields.append({"name": field_name, "type": field_type})

    data_schema = {
        "type": "record",
        "name": project_name,
        "namespace": "auto.pipeline",
        "fields": avro_fields,
    }
    return json.dumps(data_schema)



"""
    Create_jdbc_sink_connector Task
"""
def build_jdbc_sink_config(name: str, mysql_conf: Dict[str, str]) -> Dict:
    mysql_host = Variable.get("MYSQL_HOST", default_var="localhost:3306")
    return _jdbc_sink_connector_config(
        name=name,
        mysql_host=mysql_host,
        schema_registry_url=Variable.get("SCHEMA_REGISTRY"),
        database=mysql_conf.get("database"),
        user=mysql_conf.get("user"),
        password=mysql_conf.get("password"),
        table=mysql_conf.get("table"),
    )

def _jdbc_sink_connector_config(
    name: str,
    mysql_host: str,
    schema_registry_url: str,
    database: str,
    user: str,
    password: str,
    table: str | None = None,
) -> Dict:
    return {
        "name": name + "-SinkConnector",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": table if table else name,
            "connection.url": f"jdbc:mysql://{mysql_host}/{database}",
            "connection.user": user,
            "connection.password": password,
            "table.name.format": table if table else name,
            "auto.create": "true",
            "auto.evolve": "true",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": schema_registry_url,
            "value.converter.subject.name.strategy": "io.confluent.kafka.serializers.subject.RecordNameStrategy",
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "none",
        },
    }