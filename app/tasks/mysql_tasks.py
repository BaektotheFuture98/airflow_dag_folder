from airflow.sdk import task, Variable
from airflow.exceptions import AirflowFailException
from typing import Dict, Any
from app.config.logger import get_logger

log = get_logger(__name__)
@task(doc_md = "JdbcSinkConnector 생성")
def create_jdbc_sink_connector(info: Dict[str, Any]) -> Dict[str, Any] : 
    from app.repositories.kafka_connect_repo import KafkaConnectRepo
    from app.services.kafka_connect_service import KafkaConnectService

    kafka_connect_repo = KafkaConnectRepo(Variable.get("KAFKA_CONNECT"))
    kafka_connect_service = KafkaConnectService(kafka_connect_repo)

    log.info("KafkaConnect: Creating JDBC Sink connectors and topics")
    chunks = info.get("chunks")
    mysql_config = info.get("mysql_config")
    for chunk in range(1,chunks+1) : 
        project_name, table_name = _make_topic_name(info.get("project_name"), info.get("mysql_config").get("table"), chunk)
        mysql_config.update({"table": f"{table_name}"})
        kafka_connect_service.create_jdbc_connector(
            service_name = project_name,
            mysql_config = mysql_config
        )

    topic_list = kafka_connect_service.get_sink_topic_list()
    info["topic_list"] = topic_list
    log.info(f"KafkaConnect: Created topics={topic_list}")
    return info

def _make_topic_name(project_name: str, table_name:str, chunk_num: int) -> str:
    if chunk_num == 1:
        return f"{project_name}", f"{table_name}"
    else:
        return f"{project_name}-{str(chunk_num).zfill(3)}", f"{table_name}_{str(chunk_num).zfill(3)}"