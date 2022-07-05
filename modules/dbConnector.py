import os
import pymysql


class DbConnector:
    def __init__(self):
        self.dbHost = os.environ.get("DB_HOST")
        self.dbName = os.environ.get("DB_DATABASE")
        self.dbUser = os.environ.get("DB_USER")
        self.dbPassword = os.environ.get("DB_PASSWORD")
        self.connection = self.CreateConnection()

    def CreateConnection(self):
        connection = pymysql.connect(host=self.dbHost, user=self.dbUser, password=self.dbPassword,
                                     database=self.dbName, cursorclass=pymysql.cursors.DictCursor)
        return connection


    def MakeDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result


    def MakeSingleDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            # Количество url'ов
            cursor.execute(sql)
            result = cursor.fetchone()
        return result