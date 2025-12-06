from repositories.schema_registry_repo import SchemaRegistryRepo

class SchemaRegistryService : 
    def __init__(self, schema_registry : SchemaRegistryRepo) : 
        self.sr = schema_registry

    def get_schema_from_registry(self, shchema_subject: str) : 
        return self.sr.get_schema_from_registry(shchema_subject)

    def register_schema(self, schema_subject : str, schema_str : str) : 
        return self.sr.register_schema(schema_subject, schema_str)
    
    def get_client(self) : 
        return self.sr.schema_registry_client