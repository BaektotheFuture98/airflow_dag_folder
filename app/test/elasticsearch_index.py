from elasticsearch import Elasticsearch, exceptions
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ... (remove_analyzer_from_mapping 함수는 그대로 유지) ...

# 이전 답변에서 제공된 remove_analyzer_from_mapping 함수를 여기에 포함했다고 가정합니다.
def remove_analyzer_from_mapping(mapping_dict: dict) -> dict:
    # ... (이전 답변의 remove_analyzer_from_mapping 함수 내용) ...
    if not isinstance(mapping_dict, dict):
        return mapping_dict

    # 1. 'analyzer' 키를 확인하고 변경/제거합니다.
    if 'analyzer' in mapping_dict and mapping_dict['analyzer'] in ['komoran', 'cjk', 'url', 'whitespace']:
        logging.info(f"    -> Analyzer '{mapping_dict['analyzer']}'를 'standard'로 변경하거나 제거합니다.")
        if mapping_dict.get('type') == 'text':
             mapping_dict['analyzer'] = 'standard'
        else:
             del mapping_dict['analyzer']
        
    
    # 2. 딕셔너리의 모든 키와 값에 대해 재귀적으로 처리합니다.
    for key, value in mapping_dict.items():
        if key == 'dynamic_templates' and isinstance(value, list):
            # Dynamic Templates 처리
            for template in value:
                for temp_key, temp_value in template.items():
                    if isinstance(temp_value, dict) and 'mapping' in temp_value:
                        remove_analyzer_from_mapping(temp_value['mapping'])
        
        elif isinstance(value, dict):
            # properties, fields, mapping 등 내부 딕셔너리 처리
            remove_analyzer_from_mapping(value)
        
        elif isinstance(value, list):
            # 리스트 내 딕셔너리 처리 (예: dynamic_templates)
            for item in value:
                remove_analyzer_from_mapping(item)
                
    return mapping_dict

def create_index_with_copied_mapping_FINAL(client: Elasticsearch, source_index: str, target_index: str) -> bool:
    print(f"\n✅ 작업 시작: '{source_index}' 설정 및 매핑 복사 -> '{target_index}' 생성")
    
    # 1. 원본 인덱스의 메타데이터 가져오기
    try:
        index_metadata_response = client.indices.get(index=source_index)
        logging.info(f"원본 인덱스 '{source_index}' 메타데이터 조회 성공.")
    except exceptions.NotFoundError:
        print(f"❌ 오류: 원본 인덱스 '{source_index}'를 찾을 수 없습니다.")
        return False
    except Exception as e:
        print(f"❌ 오류: 메타데이터를 가져오는 중 예외 발생: {e}")
        return False
        
    # 2. 필요한 설정 및 매핑 구조 추출
    try:
        metadata = index_metadata_response[source_index]
        
        # settings 추출 및 불필요한 필터링 (UUID 등)
        settings_data = metadata.get('settings', {}).get('index', {})
        settings_to_copy = {
            k: v for k, v in settings_data.items() 
            if k not in ['creation_date', 'uuid', 'version', 'provided_name']
        }
        
        mappings_data = metadata.get('mappings', {})
        
    except (KeyError, TypeError):
        print("❌ 오류: 인덱스 메타데이터 응답 구조가 예상과 다릅니다. 추출 실패.")
        return False

    # --- 3. 핵심 수정 로직 적용 ---
    
    # 3-1. settings: analysis 블록에서 normalizer만 보존하고 나머지는 제거
    if 'analysis' in settings_to_copy:
        analysis_block = settings_to_copy['analysis']
        
        # normalizer 블록만 따로 추출합니다.
        normalizer_block = analysis_block.pop('normalizer', None) 
        
        # analysis_block에서 analyzer, tokenizer, filter, char_filter 등 
        # 불필요한 사용자 정의 분석기 관련 블록을 모두 제거합니다. (pop으로 이미 대부분 제거됨)
        # 이제 analysis_block은 비어있거나 normalizer만 제거된 상태입니다.
        
        # analysis_block을 완전히 재구성하여 normalizer만 포함하도록 합니다.
        if normalizer_block:
            settings_to_copy['analysis'] = {'normalizer': normalizer_block}
            logging.info("⭐ 설정: 'analysis' 블록 재구성 완료. normalizer 정의 유지.")
        else:
            # normalizer가 없으면 analysis 블록 전체를 제거합니다.
            del settings_to_copy['analysis']
            logging.info("⭐ 설정: 'analysis' 블록 제거 완료 (normalizer 정의 없음).")

    # 3-2. mappings: properties 및 dynamic_templates 내 analyzer 수정/제거
    modified_mappings = json.loads(json.dumps(mappings_data))
    
    logging.info("⭐ 매핑: custom analyzer 필드 수정 시작.")
    
    if 'properties' in modified_mappings:
        remove_analyzer_from_mapping(modified_mappings['properties'])
        
    if 'dynamic_templates' in modified_mappings:
        remove_analyzer_from_mapping({"dynamic_templates": modified_mappings['dynamic_templates']})

    # 4. 새 인덱스 생성 요청 본문 구성
    new_index_body = {
        "settings": settings_to_copy,
        "mappings": modified_mappings 
    }
    
    # 5. 새 인덱스 생성 및 매핑 적용 (PUT /new_index_name)
    try:
        if client.indices.exists(index=target_index):
            print(f"⚠️ 경고: 대상 인덱스 '{target_index}'가 이미 존재합니다. 생성을 건너뜁니다.")
            return True

        creation_response = client.indices.create(index=target_index, body=new_index_body)
        
        if creation_response.get('acknowledged'):
            print(f"🎉 성공: 새 인덱스 '{target_index}'가 성공적으로 생성되었고 설정/매핑이 적용되었습니다.")
            return True
        else:
            return False

    except exceptions.RequestError as e:
        print(f"❌ 오류: 인덱스 생성 중 요청 오류 발생 (status code {e.status_code}): {e.info}")
        return False
    except Exception as e:
        print(f"❌ 오류: 인덱스 생성 중 알 수 없는 예외 발생: {e}")
        return False

# --- 사용 예시 ---

# # 실제 클라이언트 연결 (예시)
# client = Elasticsearch(['http://192.168.125.63:9200'], 
#                        basic_auth=('elastic', 'elastic')) 
# SOURCE_INDEX = "lucy_main_v1_20241115"
# TARGET_INDEX = "migration_es_index_final_test"

# # 함수 호출 (주석 해제 후 실행)
# create_index_with_copied_mapping_FINAL(client, SOURCE_INDEX, TARGET_INDEX)

