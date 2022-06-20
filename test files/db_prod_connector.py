import os
import pymysql


class DbProdConnector:
    def __init__(self):
        self.db_host = "109.248.133.9"
        self.db_name = "admin_domains"
        self.db_user = "admin_domains"
        self.db_password = "vHVLHeoSrk"
        self.connection = self.__create_connection()

    def __create_connection(self):
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