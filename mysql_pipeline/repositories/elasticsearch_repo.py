from typing import Tuple, List
from mysql_pipeline.config.logger import get_logger
log = get_logger(__name__)

class ElasticsearchRepo: 
    def __init__(self, hosts: str, basic_auth: Tuple[str, str]): 
        from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
        log.info(f"ElasticsearchRepo: Initializing with hosts={hosts}, type={type(hosts)}")
        host_list = [host.strip() for host in hosts.split(",")]
        log.info(f"ElasticsearchRepo: Connecting to hosts={host_list}")
        self.hook = ElasticsearchPythonHook(hosts = host_list, es_conn_args={"basic_auth": basic_auth})

    def count(self, index:str, query:dict) -> int: 
        conn = self.hook.get_conn
        res = conn.count(index=index, body=query)
        return res.get("count")
    
    def search(self, index:str, query:dict, size:int=200) -> List[dict]:
        res = self.hook.search(query=query, index=index)
        result = res.get("hits", {})
        return result