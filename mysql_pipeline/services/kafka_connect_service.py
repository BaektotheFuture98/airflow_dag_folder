from mysql_pipeline.repositories.kafka_connect_repo import KafkaConnectRepo
from mysql_pipeline.models.build_models import build_jdbc_sink_config
from typing import List

class KafkaConnectService() : 
    def __init__(self, kafka_connect_repo : KafkaConnectRepo) : 
        self.connect_client = kafka_connect_repo
    
    def create_connector(self, chunks : int, service_name : str, mysql_config : dict) -> List[str]:
        topic_list = []
        for chunk in range(1,chunks+1) : 
            topic_name = self._make_topic_name(service_name, chunk)
            self.connect_client.create_connector(build_jdbc_sink_config(topic_name, mysql_config))
            topic_list.append(topic_name)
        return topic_list

    def _make_topic_name(self, project_name: str, chunk_num: int) -> str:
        if chunk_num == 1:
            return f"{project_name}"
        else:
            return f"{project_name}-{str(chunk_num).zfill(3)}"

