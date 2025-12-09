from typing import List, Tuple

class ElasticsearchRepo: 
    def __init__(self, hosts: List[str], basic_auth: Tuple[str, str]): 
        from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
        self.hook = ElasticsearchPythonHook(hosts = hosts, es_conn_args={"basic_auth": basic_auth})

    def count(self, index:str, query:dict) -> int: 
        conn = self.hook.get_conn()
        res = conn.count(index=index, query=query)
        return res.get("count")
    
    def search(self, index:str, query:dict, size:int=200) -> List[dict]:
        res = self.hook.search(query=query, index=index)
        result = res.get("hits", {}).get("hits", [])
        return result