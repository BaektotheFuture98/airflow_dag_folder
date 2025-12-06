from typing import List

class ElasticsearchRepo: 
    def __init__(self, hosts:List[str], basic_auth:tuple): 
        from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook
        self.hook = ElasticsearchPythonHook(hosts = hosts, es_conn_args={"basic_auth": basic_auth})

    def count(self, index:str, query:dict) -> int: 
        res = self.hook.get_conn.count(index = index, body = query)
        return res.get("count")
    
    def search(self, index:str, query:dict, size:int=200) -> List[dict]:
        res = self.hook.search(index = index, body = query, size = size)   
        sesarch_after = "", 
        result = res.get("hits", {}).get("hits", [])
        return sesarch_after, result