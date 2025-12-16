import mysql.connector

class MySQLRepo:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

    def execute_query(self, query: str):
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
