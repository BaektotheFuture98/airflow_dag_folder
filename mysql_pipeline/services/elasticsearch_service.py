from mysql_pipeline.repositories.elasticsearch_repo import ElasticsearchRepo
from datetime import datetime, timezone
from mysql_pipeline.config.logger import get_logger

log = get_logger(__name__)

class ElasticsearchService() : 
    def __init__(self, elasticsearchRepo : ElasticsearchRepo) : 
        self.client = elasticsearchRepo
        self.chunk_size = 100000
        self.pagination_size = 200
        self.today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    def get_chunk_count(self, index: str, query: dict) -> int : 
        count_query = self._query_with_pagination(query)
        log.info("query : %s",count_query)
        total_count = self.client.count(index = index, query = count_query)
        num_chunks = (total_count // self.chunk_size) + (1 if total_count % self.chunk_size > 0 else 0)
        if num_chunks == 0 : num_chunks = 1
        return num_chunks
    
    def search(self, index : str, query : str, search_after : str) : 
        query = self._query_with_pagination(query, self.pagination_size, search_after)
        return self.client.search(index=index, query=query)
    
    def _query_with_pagination(self, query: str, page_size: int = None, search_after:str = None) -> dict: 
        build_query = {
            "query": {
                "query_string": {
                    "query": query
                }
            }
        }
        
        if search_after :
            build_query["search_after"] = [search_after]
            build_query["size"] = page_size
            build_query["sort"] = [
                {"kw_docid": {"order": "asc"}}
            ]
            
        return build_query