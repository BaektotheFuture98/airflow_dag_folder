from airflow.sdk import Variable
from typing import List
import json


def build_avro_schema(project_name: str, fields : List[str]) -> str : 
        avro_fields = []
        for field_name in fields : 
            field_type = "int" if ("in" in field_name) else ["string", "null"]
            avro_fields.append({"name" : field_name, "type" : field_type})
        
        data_schema = {
            "type" : "record", 
            "name" : project_name,
            "namespace" : "auto.pipeline", 
            "fields" : avro_fields
        }
        return json.dumps(data_schema)

def _jdbc_sink_connector_config(name:str, kafka_connect_url:str, schema_registry_url:str, database: str, user:str, password:str, table:str = None) -> dict: 
        return {
                "name": name + "-SinkConnector",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "tasks.max": "1",
                    "topics": table if table else name,
                    "connection.url": f"jdbc:mysql://{kafka_connect_url}/{database}",
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
                }
            }

def build_jdbc_sink_config(name:str, mysql_config:dict) -> dict :     
    return _jdbc_sink_connector_config(
            name = name,
            kafka_connect_url = Variable.get("KAFKA_CONNECT"),
            schema_registry_url = Variable.get("SCHEMA_REGISTRY"),
            database = mysql_config.get("database"),
            user = mysql_config.get("user"),
            password = mysql_config.get("password"), 
            table = mysql_config.get("table")
        )

def mysql_config(database :str, user :str, password :str, table :str= None) -> dict :
    return {
        "database" : database, 
        "user" : user, 
        "password" : password, 
        "table" : table
    }


def build_es_source_model(project_name : str, query : str, fields:str,  index:str = Variable.get("elasticsearch_index")) -> dict:
    from airflow.sdk import Variable
    return {
        "project_name" : project_name,
        "query" : query, 
        "fields" : fields, 
        "index" : index,
        "hosts" : Variable.get("elasticsearch_hosts"),
        "user" : Variable.get("elasticsearch_user"),
        "password" : Variable.get("elasticsearch_password")
    }
    
def build_es_target_model(project_name : str, hosts : str, index : str, user : str, passsword : str) -> dict : 
    return {
        "project_name" : project_name, 
        "hosts" : hosts, 
        "index" : index,
        "user" : user, 
        "password" : passsword
    }