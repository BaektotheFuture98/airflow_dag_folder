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
    
    def insert_query(self, query: str, data: dict) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, data)
            self.connection.commit()
            return cursor.lastrowid or 0
        finally:
            cursor.close()

    def update_query(self, query: str, data: dict) -> int:
        """Execute an UPDATE statement and return affected row count."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, data)
            self.connection.commit()
            return cursor.rowcount or 0
        finally:
            cursor.close()