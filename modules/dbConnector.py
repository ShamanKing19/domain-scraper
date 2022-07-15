from datetime import datetime
import os
import time
import pymysql


class DbConnector:
    def __init__(self):
        # Для локалки
        self.dbHost = os.environ.get("DB_HOST", "localhost")
        self.dbName = os.environ.get("DB_DATABASE", "test_domains")
        self.dbUser = os.environ.get("DB_USER", "root")
        self.dbPassword = os.environ.get("DB_PASSWORD", "password")

        # Для сервера
        # self.dbHost = os.environ.get("DB_HOST", "localhost")
        # self.dbName = os.environ.get("DB_DATABASE", "admin_domains")
        # self.dbUser = os.environ.get("DB_USER", "admin_domains")
        # self.dbPassword = os.environ.get("DB_PASSWORD", "vHVLHeoSrk")
        self.pauseAfterError = 5
        self.maxRetries = 12
        self.connection = self.createConnection()

    def createConnection(self):
        connection = pymysql.connect(
            host=self.dbHost,
            user=self.dbUser,
            password=self.dbPassword,
            database=self.dbName,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection


    def makeDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result


    def makeSingleDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
        return result


    def log(self, filename, content):
        file = open(filename, "a", encoding="utf-8")
        file.write(content + "\n")
        file.close()
