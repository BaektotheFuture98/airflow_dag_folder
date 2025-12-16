
from repositories.mysql_repo import MySQLRepo   
class MySQLService:
    
    def __init__(self, mysqlRepo: MySQLRepo):
        self.client = mysqlRepo

    def execute_query(self, query: str):
        return self.client.execute_query(query)
    

# repo = MySQLRepo(host="192.168.125.62:8080", database="AUTO_PIPE", user="root", password="dkfdptmdps404" )
# service = MySQLService(repo)

# result = service.execute_query("SELECT * FROM tabletable;")