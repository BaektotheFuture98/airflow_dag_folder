from app.repositories.kafka_connect_repo import KafkaConnectRepo
from app.models.build_models import build_jdbc_sink_config, build_es_sink_connector_config
from typing import List

class KafkaConnectService() : 
    def __init__(self, kafka_connect_repo : KafkaConnectRepo) : 
        self.connect_client = kafka_connect_repo
        self.topic_list = []

    def create_jdbc_connector(self, service_name : str, mysql_config : dict) -> List[str]:
        self.connect_client.create_connector(build_jdbc_sink_config(service_name, mysql_config))
        self.topic_list.append(service_name)
        
    def create_es_sink_connector(self, es_config:dict) :
        self.connect_client.create_connector(build_es_sink_connector_config(es_config))

    def get_sink_topic_list(self) -> List[str] :
        return self.topic_list

    def delete_connector(self, service_name: str) :
        pass