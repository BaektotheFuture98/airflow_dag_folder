from pipeline.repositories.elasticsearch_repo import ElasticsearchRepo
from datetime import datetime, timezone
from pipeline.config.logger import get_logger
from pipeline.config.elasticsearch_index import create_index_with_copied_mapping_FINAL

log = get_logger(__name__)

class ElasticsearchService() : 
    def __init__(self, elasticsearchRepo : ElasticsearchRepo) : 
        self.client = elasticsearchRepo
        self.chunk_size = 100000
        self.pagination_size = 100
        self.today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    def get_chunk_count(self, index: str, query: dict) -> int : 
        count_query = self._query_with_pagination(query)
        total_count = self.client.count(index = index, query = count_query)
        log.info(f"ElasticsearchService: Total count for index={index} is {total_count}")

        num_chunks = (total_count // self.chunk_size) + (1 if total_count % self.chunk_size > 0 else 0)
        log.info(f"ElasticsearchService: Calculated num_chunks={num_chunks} with chunk_size={self.chunk_size}")
        
        if num_chunks == 0 : num_chunks = 1
        return num_chunks
    
    def search(self, index : str, fields:list, query : str, search_after : str) : 
        query = self._query_pagination(query, fields, self.pagination_size, search_after)
        return self.client.search(index=index, query=query)
    
    def close_client(self) -> None :
        self.client.close_client()

    def _query_pagination(self, query: str, fields : list, page_size: int, search_after:str) -> dict: 
        build_query = {
            "query": {
                "query_string": {
                    "query": query
                }
            },
            "_source": fields,
            "size": page_size,
            "sort": [
                {"kw_docid": {"order": "asc"}}
            ],
            "search_after": [search_after]
        }   
        log.info(f"ElasticsearchService: Built paginated query: {build_query}")
        return build_query
    
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
    

    def create_index_before_migration(self, source_index:str, target_index:str) -> bool : 
        source_index = source_index.lower()
        target_index = target_index.lower()
        return create_index_with_copied_mapping_FINAL(self.client.get_client(), source_index, target_index)
        # index_metadata_response = self.client.get_index_mapping(source_index)
        
        # metadata = index_metadata_response[source_index]
        # settings_data = metadata.get('settings', {}).get('index', {})
        # settings_to_copy = {
        #     k: v for k, v in settings_data.items() 
        #     if k not in ['creation_date', 'uuid', 'version', 'provided_name']
        # }

        # mappings_data = metadata.get('mappings', {})

        # if 'analysis' in settings_to_copy : 
        #     analysis_block = settings_to_copy['analysis']
            
        #     normalizer_block = analysis_block.pop('normalizer', None) 
            
        #     if normalizer_block : 
        #         settings_to_copy['analysis'] = {'normalizer':normalizer_block}
        #         log.info("설정: 'analysis' 블록 재구성 완료. normalizer 정의 유지.")
        #     else : 
        #         del settings_to_copy['analysis']
        #         log.info("설정: 'analysis' 블록 제거 완료 (normalizer 정의 없음).")

        # modified_mappings = json.loads(json.dumps(mappings_data))

        # # 전체 매핑 트리를 대상으로 안전하게 제거/치환 수행
        # self._remove_analyzer_from_mapping(modified_mappings)
        
        # new_index_body = {
        #     "settings": settings_to_copy,
        #     "mappings": modified_mappings 
        # }

        # try:
        #     if self.client.exists(index=target_index):
        #         log.info(f"⚠️ 경고: 대상 인덱스 '{target_index}'가 이미 존재합니다. 생성을 건너뜁니다.")
        #         return True

        #     creation_response = self.client.create_index(index=target_index, body=new_index_body)
            
        #     if creation_response.get('acknowledged'):
        #         log.info(f"🎉 성공: 새 인덱스 '{target_index}'가 성공적으로 생성되었고 설정/매핑이 적용되었습니다.")
        #         return True
        #     else:
        #         return False

        # except Exception as e:
        #     print(f"❌ 오류: 인덱스 생성 중 알 수 없는 예외 발생: {e}")
        #     return False        

    def _remove_analyzer_from_mapping(self, node):
        """
        매핑 트리에서 특정 analyzer(komoran, cjk, url, whitespace)를 제거하거나
        text 타입일 경우 'standard'로 교체.
        dict / list / 기타 타입을 모두 안전하게 처리.
        """
        targets = {'komoran', 'cjk', 'url', 'whitespace'}

        # dict 처리
        if isinstance(node, dict):
            # 현재 노드에 analyzer 키가 있으면 처리
            if 'analyzer' in node and node.get('analyzer') in targets:
                if node.get('type') == 'text':
                    # text 타입이면 표준 분석기로 교체
                    node['analyzer'] = 'standard'
                else:
                    # 그 외 타입이면 analyzer 제거
                    node.pop('analyzer', None)

            # properties 하위 필드 순회
            props = node.get('properties')
            if isinstance(props, dict):
                for sub in props.values():
                    self._remove_analyzer_from_mapping(sub)

            # multi-fields (fields) 순회
            fields = node.get('fields')
            if isinstance(fields, dict):
                for sub in fields.values():
                    self._remove_analyzer_from_mapping(sub)

            # dynamic_templates 순회 (list[ {name: {mapping: {...}}}, ... ])
            dyn = node.get('dynamic_templates')
            if isinstance(dyn, list):
                for tmpl in dyn:
                    if isinstance(tmpl, dict):
                        for tmpl_body in tmpl.values():
                            if isinstance(tmpl_body, dict):
                                mapping = tmpl_body.get('mapping')
                                if mapping:
                                    self._remove_analyzer_from_mapping(mapping)

            # 기타 하위 dict/list도 방어적 순회
            for v in node.values():
                if isinstance(v, (dict, list)):
                    self._remove_analyzer_from_mapping(v)
            return

        # list 처리
        if isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    self._remove_analyzer_from_mapping(item)
            return

        # 그 외 타입(str, int 등)은 처리 없음
        return
