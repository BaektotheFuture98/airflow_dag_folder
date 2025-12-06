from airflow.sdk import Variable
from typing import List
import json


def build_jdbc_sink_config(name:str, kafka_connect_url:str, schema_registry_url:str, database: str, user:str, password:str) -> dict: 
        '''
        프로젝트 이름으로 토픽, 커넥터(<프로젝트이름>-SinkConnector) 생성

        :param name: 프로젝트 이름
        :type name: str 
        :param schmea_registry_url: 스키마 레지스트리 URL
        :type schmea_registry_url: str
        :param user: MySQL 사용자 이름
        :type user: str
        :param password: MySQL 사용자 비밀번호
        :type password: str
        :return: 커넥터 생성 결과
        :rtype: dict
        '''

        return {
                "name": name + "-SinkConnector",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "tasks.max": "1",
                    "topics": name,
                    "connection.url": f"jdbc:mysql://{kafka_connect_url}/{database}",
                    "connection.user": user,
                    "connection.password": password,
                    "table.name.format": name,
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

def build_jdbc_sink_config(name:str, kafka_connect_url:str, schema_registry_url:str, database: str, user:str, password:str) -> dict: 
        '''
        프로젝트 이름으로 토픽, 커넥터(<프로젝트이름>-SinkConnector) 생성

        :param name: 프로젝트 이름
        :type name: str 
        :param schmea_registry_url: 스키마 레지스트리 URL
        :type schmea_registry_url: str
        :param user: MySQL 사용자 이름
        :type user: str
        :param password: MySQL 사용자 비밀번호
        :type password: str
        :return: 커넥터 생성 결과
        :rtype: dict
        '''

        return {
                "name": name + "-SinkConnector",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "tasks.max": "1",
                    "topics": name,
                    "connection.url": f"jdbc:mysql://{kafka_connect_url}/{database}",
                    "connection.user": user,
                    "connection.password": password,
                    "table.name.format": name,
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

def build_mysql_model(name:str, db:str, table:str, user:str, password:str) -> dict :     
    return {
        "project_name" : name,
        "mysql_db" : db, 
        "mysql_table" : table,
        "user" : user, 
        "password" : password
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