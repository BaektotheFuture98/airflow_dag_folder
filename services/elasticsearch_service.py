from datetime import datetime, timezone
from models.elasticsearch_model import ElasticsearchSourceConfig

chunk_size = 100000
today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

def data_chunks_check(index: str, query: dict, es_config: ElasticsearchSourceConfig) -> int : 
    from repositories.elasticsearch_repo import ElasticsearchRepo
    es_repo = ElasticsearchRepo(
        hosts = list(es_config.hosts),
        basic_auth = (es_config.user, es_config.password)
    )

    total_count = es_repo._count(index = index, query = query)
    num_chunks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)

    if num_chunks == 0 : num_chunks = 1

    return num_chunks
