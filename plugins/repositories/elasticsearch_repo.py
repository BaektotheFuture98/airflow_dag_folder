from typing import Tuple, List
from plugins.config.logger import get_logger
log = get_logger(__name__)

class ElasticsearchRepo: 
    def __init__(self, hosts: str, basic_auth: Tuple[str, str]): 
        from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
        log.info(f"ElasticsearchRepo: Initializing with hosts={hosts}, type={type(hosts)}")
        log.info(f"ElasticsearchRepo: Basic auth user={basic_auth}")
        host_list = [host.strip() for host in hosts.split(",")]
        log.info(f"ElasticsearchRepo: Connecting to hosts={host_list}")
        self.hook = ElasticsearchPythonHook(hosts = host_list, es_conn_args={"basic_auth": basic_auth})

    def get_client(self) : 
        return self.hook.get_conn

    def close_client(self) -> None :
        self.hook.get_conn.close()

    def count(self, index:str, query:dict) -> int: 
        conn = self.hook.get_conn
        res = conn.count(index=index, body=query)
        return res.get("count")
    
    def search(self, index:str, query:dict, size:int=200) -> List[dict]:
        res = self.hook.search(query=query, index=index)
        result = res.get("hits", {})
        return result
    
    def create_index(self, index:str, body:dict) -> None: 
        conn = self.hook.get_conn
        conn.indices.create(index = index, body = body)

    def get_index_mapping(self, index:str) -> dict : 
        conn = self.hook.get_conn
        log.info(f"ElasticsearchRepo: Fetching mapping for index={index}")
        try : 
            mapping_setting = conn.indices.get_mapping(index=index)
        except Exception as e :
            log.error(f"ElasticsearchRepo: Error fetching mapping for index={index}, error={e}")
            raise e
        return mapping_setting
    
    def exists(self, index:str) -> bool :
        conn = self.hook.get_conn
        return conn.indices.exists(index=index)