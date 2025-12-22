
from app.repositories.mysql_repo import MySQLRepo


class MySQLService:
    def __init__(self, mysqlRepo: MySQLRepo):
        self.client = mysqlRepo

    def execute_query(self, query: str):
        return self.client.execute_query(query)

    def insert_query(self, query: str, data: dict):
        return self.client.insert_query(query, data)

    def update_query(self, query: str, data: dict):
        return self.client.update_query(query, data)