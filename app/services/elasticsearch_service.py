from app.repositories.elasticsearch_repo import ElasticsearchRepo
from datetime import datetime, timezone
from app.config.logger import get_logger
import json

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
        index_metadata_response = self.client.get_index_mapping(source_index)
        
        metadata = index_metadata_response[source_index]
        settings_data = metadata.get('settings', {}).get('index', {})
        settings_to_copy = {
            k: v for k, v in settings_data.items() 
            if k not in ['creation_date', 'uuid', 'version', 'provided_name']
        }

        mappings_data = metadata.get('mappings', {})

        if 'analysis' in settings_to_copy : 
            analysis_block = settings_to_copy['analysis']
            
            normalizer_block = analysis_block.pop('normalizer', None) 
            
            if normalizer_block : 
                settings_to_copy['analysis'] = {'normalizer':normalizer_block}
                log.info("설정: 'analysis' 블록 재구성 완료. normalizer 정의 유지.")
            else : 
                del settings_to_copy['analysis']
                log.info("설정: 'analysis' 블록 제거 완료 (normalizer 정의 없음).")

        modified_mappings = json.loads(json.dumps(mappings_data))

        if 'properties' in modified_mappings : 
            self._remove_analyzer_from_mapping(modified_mappings['properties'])
        
        if 'dynamic_templates' in modified_mappings : 
            self._remove_analyzer_from_mapping({"dynamic_templates": modified_mappings['dynamic_templates']})
        
        new_index_body = {
            "settings": settings_to_copy,
            "mappings": modified_mappings 
        }

        try:
            if self.client.exists(index=target_index):
                log.info(f"⚠️ 경고: 대상 인덱스 '{target_index}'가 이미 존재합니다. 생성을 건너뜁니다.")
                return True

            creation_response = self.client.create_index(index=target_index, body=new_index_body)
            
            if creation_response.get('acknowledged'):
                log.info(f"🎉 성공: 새 인덱스 '{target_index}'가 성공적으로 생성되었고 설정/매핑이 적용되었습니다.")
                return True
            else:
                return False

        except Exception as e:
            print(f"❌ 오류: 인덱스 생성 중 알 수 없는 예외 발생: {e}")
            return False        

    def _remove_analyzer_from_mapping(self, mapping_dict : dict) -> dict : 
        if not isinstance(mapping_dict, dict):
            return mapping_dict
        
        if 'analyzer' in mapping_dict and mapping_dict['analyzer'] in ['komoran', 'cjk', 'url', 'whitespace'] : 
            if mapping_dict.get('type') == 'text' : 
                mapping_dict.pop('analyzer') == 'standard'
            else : 
                del mapping_dict['analyzer']

        for key, value in mapping_dict.items():
            if key == 'dynamic_templates' and isinstance(value, list):
                # Dynamic Templates 처리
                for template in value:
                    for temp_key, temp_value in template.items():
                        if isinstance(temp_value, dict) and 'mapping' in temp_value:
                            self._remove_analyzer_from_mapping(temp_value['mapping'])
            
            elif isinstance(value, dict):
                # properties, fields, mapping 등 내부 딕셔너리 처리
                self._remove_analyzer_from_mapping(value)
            
            elif isinstance(value, list):
                # 리스트 내 딕셔너리 처리 (예: dynamic_templates)
                for item in value:
                    self._remove_analyzer_from_mapping(item)
                
        return mapping_dict
