from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

class SchemaRegistryRepo:
    def __init__(self, schema_registry_url: str) : 
        self.client = SchemaRegistryClient({"url": schema_registry_url})

    def register_schema(self, subject_name: str, avro_schema_str: str) -> int :
        sr = self.client
        schema = Schema(subject_name, avro_schema_str, schema_type="AVRO")
        schema_id = sr.register_schema(schema)
        return schema_id

    def get_schema_from_registry(self, url: str, subject: str) : 
        sr = self.client
        latest_version = sr.get_latest_version(subject)
        return sr, latest_version
    
