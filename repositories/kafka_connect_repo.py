class KafkaConnectRepo:
    def __init__(self, kafka_connect_url: str):
        from kafka_connect import KafkaConnect
        self.client = KafkaConnect(kafka_connect_url)
    
    def create_connector(self, config: dict) -> None:
        '''
        커넥터 생성
        :param config: 커넥터 설정파일
        '''
        