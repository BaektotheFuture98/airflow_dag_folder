from dataclasses import dataclass

@dataclass
class JdbcSinkConfig:
    """
    Docstring for JdbcSinkConfig
    """
    name: str
    host: str
    database: str
    user: str
    password: str
    schema_registry_url: str

