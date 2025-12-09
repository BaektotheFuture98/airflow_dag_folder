class KafkaConnectRepo:
    def __init__(self, kafka_connect_url: str):
        from kafka_connect import KafkaConnect
        self.client = KafkaConnect(kafka_connect_url)
    
    def create_connector(self, config: dict) -> None:
        self.client.create_connector(config)
        