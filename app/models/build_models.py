"""
    API 서버를 통해 유효성 검사를 끝냈다고 가정
"""
from typing import List, Dict
from airflow.sdk import Variable
from pydantic import BaseModel, field_validator
import json


"""
    Trigger Task 
"""
def build_es_source_model(project_name: str, index: str, query: Dict, fields: List[str]) -> Dict:
    hosts_str = Variable.get("ELASTICSEARCH_HOSTS")
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
    
def build_mysql_config(host:str, database: str, user: str, password: str, table: str | None = None) -> Dict[str, str]:
    return {
        "host": host,
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
        field_type = "int" if ("in" in field_name) else "string"
        avro_fields.append({"name": field_name, "type": field_type})

    data_schema = {
        "type": "record",
        "name": project_name,
        "fields": avro_fields,
    }
    return json.dumps(data_schema)



"""
    Create_jdbc_sink_connector Task
"""
def build_jdbc_sink_config(name: str, mysql_conf: Dict[str, str]) -> Dict:
    return _jdbc_sink_config(
        name=name,
        mysql_host=mysql_conf.get("host"),
        schema_registry_url=Variable.get("SCHEMA_REGISTRY"),
        database=mysql_conf.get("database"),
        user=mysql_conf.get("user"),
        password=mysql_conf.get("password"),
        table=mysql_conf.get("table"),
    )

def _jdbc_sink_config(
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
            "topics": name,
            "connection.url": f"jdbc:mysql://{mysql_host}/{database}",
            "connection.user": user,
            "connection.password": password,
            "table.name.format": table if table else name,
            "auto.create": "true",
            "auto.evolve": "true",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": schema_registry_url,
            "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.RecordNameStrategy",
            "value.converter.schemas.enable": "true",
            "errors.tolerance": "none",
        },
    }
    
"""
    Create_elasticsearch_sink_connector Task
"""
def build_es_target_model(project_name: str, es_hosts:str, index: str, user:str, passwd:str) -> Dict : 
    return {
        "project_name" : project_name,
        "es_hosts" : es_hosts,
        "index" : index, 
        "user" : user, 
        "passwd" : passwd
    }



def build_es_sink_connector_config(es_config : dict) -> Dict: 
    return _es_sink_connector_config(
        project_name = es_config.get("project_name"),
        index = es_config.get("index"),
        hosts = es_config.get("hosts"),
        user = es_config.get("user"),
        password = es_config.get("password"),
        schema_registry_url = Variable.get("SCHEMA_REGISTRY")
    )

def _es_sink_connector_config(
        project_name: str,
        index : str, 
        hosts: List[str],
        user: str,
        password: str,
        schema_registry_url: str
    ) -> Dict:
        return {
            "name": project_name + "-EsSinkConnector",
            "config": {
                "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                "type.name": "_doc",
                "connection.url": f"{hosts}",
                "connection.username": user,
                "connection.password": password,
                "topics": project_name,
                "write.method": "UPSERT",
                "tasks.max": "1",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": schema_registry_url,
                "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                "key.ignore": "false",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter"
            }
        }