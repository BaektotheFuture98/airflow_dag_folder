from repositories.kafka_connect_repo import KafkaConnectRepo
from models.build_models import build_jdbc_sink_config

class KafkaConnectService() : 
    def __init__(self, kafka_connect_repo : KafkaConnectRepo) : 
        self.connect_client = kafka_connect_repo
    
    def create_connector(self, chunks : int, mysql_config : dict) -> None :
        for i in range(chunks) : 
            
            self.connect_client.create_connector(build_jdbc_sink_config(topic_name, mysql_config))

    def _make_topic_name(self, project_name: str, chunk_num: int) -> str:
        if chunk_num == 1:
            return f"{project_name}-topic"
        else:
            return f"{project_name}-topic-{str(chunk_num).zfill(3)}"

