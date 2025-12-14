from pydantic import BaseModel,field_validator
import socket


class InfoModel(BaseModel) : 
    project_name : str
    es_source_config : dict
    mysql_sink_config : dict | None
    es_sink_config : dict | None
    schema_config : dict | None
    chunks : str 
    
    @field_validator("mysql_sink_config", mode="before")
    def check_mysql_sink_config(cls, mysql_config : dict) : 
        if mysql_config is None : 
            return mysql_config
        
        mysql_host = mysql_config.get("host")
        
        if not isinstance(mysql_host, str) : 
            return TypeError("mysql_host must be str")
        
        if not valid_ip(mysql_host) : 
            raise ValueError("ip pattern unavailable")
        
        return mysql_config
    
    @field_validator("es_sink_config", mode="before")
    def check_es_sink_config(cls, es_config : dict | None) : 
        if es_config is None : 
            return es_config
        
        es_hosts = es_config.get("es_hosts")
        
        if not isinstance(es_hosts, str) : 
            raise TypeError("es_hosts must be str")
        
        for host in es_hosts.split(",") : 
            if not valid_ip(host) :
                raise ValueError("ip pattern unavailable")
        
        return es_config
    
def valid_ip(address : str) -> bool : 
    try : 
        socket.inet_aton(address)
        return address.count(".") == 3
    except : 
        return False