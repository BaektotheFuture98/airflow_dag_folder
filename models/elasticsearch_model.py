from dataclasses import dataclass

@dataclass
class ElasticsearchSourceConfig:
    """
    Docstring for ElasticsearchSourceConfig
    """
    name: str
    hosts: str
    index: str
    user: str
    password: str
    query : dict

