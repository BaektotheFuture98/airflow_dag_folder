from repositories.elasticsearch_repo import ElasticsearchRepo
from datetime import datetime, timezone


class ElasticsearchService() : 
    def __init__(self, elasticsearchRepo : ElasticsearchRepo) : 
        self.client = elasticsearchRepo
        self.chunk_size = 100000
        self.pagination_size = 200
        self.today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    def get_chunk_count(self, index: str, query: dict) -> int : 
        total_count = self.client.count(index = index, query = query)
        num_chunks = (total_count // self.chunk_size) + (1 if total_count % self.chunk_size > 0 else 0)
        if num_chunks == 0 : num_chunks = 1
        return num_chunks
    
    def search(self, index : str, query : str, search_after : str) : 
        query = self._query_with_pagination(query, self.pagination_size, search_after)
        return self.client.search(index=index, query=query)
    
    def _query_with_pagination(self, query: str, page_size: int, search_after) : 
        paginated_query = {
            "size": page_size,
            "query": {
                "query_string": {
                    "query": query
                }
            },
            "sort": [
                {"kw_docid": {"order" : "asc"}}
            ], 

        }
        if search_after:
            paginated_query["search_after"] = [search_after]
        
        return paginated_query