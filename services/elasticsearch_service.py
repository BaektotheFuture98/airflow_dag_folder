from repositories.elasticsearch_repo import ElasticsearchRepo
from datetime import datetime, timezone


class ElasticsearchService() : 
    
    def __init__(self, elasticsearchRepo : ElasticsearchRepo) : 
        self.client = elasticsearchRepo
        self.chunk_size = 100000
        self.today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    def get_chunk_count(self, index: str, query: dict) -> int : 
        total_count = self.client.count(index = index, query = query)
        num_chunks = (total_count // self.chunk_size) + (1 if total_count % self.chunk_size > 0 else 0)
        if num_chunks == 0 : num_chunks = 1
        return num_chunks
    
    def search(self, index : str, query : str) : 
        return self.client.search(index=index, query=query)