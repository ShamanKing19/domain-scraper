import os
import pymysql


class DbConnector:
    def __init__(self):
        self.db_host = os.environ.get("DB_HOST")
        self.db_name = os.environ.get("DB_DATABASE")
        self.db_user = os.environ.get("DB_USER")
        self.db_password = os.environ.get("DB_PASSWORD")
        self.connection = self.create_connection()

    def create_connection(self):
        connection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password,
                                     database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        return connection


    def make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result


    def make_single_db_request(self, sql):
        with self.connection.cursor() as cursor:
            # Количество url'ов
            cursor.execute(sql)
            result = cursor.fetchone()
        return result