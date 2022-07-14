from datetime import datetime
import os
import time
import pymysql


class DbConnector:
    def __init__(self):
        self.dbHost = os.environ.get("DB_HOST")
        self.dbName = os.environ.get("DB_DATABASE")
        self.dbUser = os.environ.get("DB_USER")
        self.dbPassword = os.environ.get("DB_PASSWORD")
        self.connection = self.createConnection()
        


    def createConnection(self):
        # Для устойчивости от падения бд
        isStable = False
        while not isStable:
            try:
                connection = pymysql.connect(
                    host=self.dbHost,
                    user=self.dbUser,
                    password=self.dbPassword,
                    database=self.dbName,
                    cursorclass=pymysql.cursors.DictCursor
                )
                isStable = True
                return connection
            # TODO: Обрабатывать только ошибки подключения
            except pymysql.Error as e:
                print(f"Connection error: {e} - {datetime.now()}")
                time.sleep(5)


    def makeDbRequest(self, sql):
        isStable = False
        while not isStable:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                self.connection.commit()
                isStable = True
                return result
            except (pymysql.err.ProgrammingError, pymysql.err.OperationalError) as e:
                print(f"Request error: {e} - {datetime.now()}")
                time.sleep(5)


    def makeSingleDbRequest(self, sql):
        isStable = False
        while not isStable:
            try:
                with self.connection.cursor() as cursor:
                    # Количество url'ов
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    isStable = True
                return result
            except (pymysql.err.ProgrammingError, pymysql.err.OperationalError) as e:
                print(f"Single request error: {e} - {datetime.now()}")
                time.sleep(5)