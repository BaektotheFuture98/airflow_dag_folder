def build_es_model(name : str, index : str, query : str) -> dict:
    from models.elasticsearch_model import ElasticsearchSourceConfig
    from airflow.sdk import Variable
    es_config = ElasticsearchSourceConfig(
        name = name,
        hosts = Variable.get("elasticsearch_hosts"),
        user = Variable.get("elasticsearch_user"),
        password = Variable.get("elasticsearch_password"),
        index = index,
        query = query
    )
    return es_config


def build_jdbc_sink_config(self, name:str, kafka_connect_url:str, schema_registry_url:str, database: str, user:str, password:str) -> dict: 
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