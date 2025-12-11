class KafkaConnectRepo:
    def __init__(self, kafka_connect_url: str):
        self.url = kafka_connect_url + "/connectors"
        
    
    def create_connector(self, config: dict) :
        import requests
        res = requests.post(self.url, json=config)  
        return res.json()
    
    def delete_connector(self, service_name: str) : 
        pass