from genericpath import exists
import os, time
from urllib.request import urlretrieve
import zipfile
try:
    import asyncio
except:
    os.system("pip install asyncio")
try:
    import aiohttp
except:
    os.system("pip install aiohttp")
try:
    import pymysql
except:
    os.system("pip install PyMySQL")
try:
    from dotenv import load_dotenv
except:
    os.system("pip install python-dotenv")
from zipfile import ZipFile
import urllib

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


class StatusParser:
    def __init__(self, db_host, db_name, db_user, db_password, db_table_name):
        self.download_link = "https://statonline.ru/domainlist/file?tld="
        self.extracted_files_path = "archives/extracted"
        self.archive_path = "archives/ru.zip"
        
        self.file_path = "archives/extracted/ru_domains.txt"
        self.zone = 'ru'
        
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.connection = self.__create_connection()
        self.table_name = db_table_name

        # Можно разбить на connection и readtimeout
        self.timeout = 5
        self.step = 10000
        # Будет выводиться каждая запись кратная этому числу
        self.every_printable = 10

        self.domains = []


    def parse_statuses(self):
        self.__create_table_if_not_exists()
        self.__download_ru_domains_file_if_not_exists()
        self.domains = self.__get_rows_from_txt()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__request_all())



    def __download_ru_domains_file_if_not_exists(self):
        if (exists(self.file_path)): return
        start_time = time.time()
        print("Началась загрузка файла с доменами...")
        urlretrieve(self.download_link, self.archive_path)
        print(f"Файл загружен за {time.time() - start_time}")
        
        print("Начата распаковка файла")
        with zipfile.ZipFile(self.archive_path, 'r') as zip_file:
            zip_file.extractall(self.extracted_files_path)
        print("Файл распакован")
        os.remove(self.archive_path)
        print("Архив удалён")


    def __create_table_if_not_exists(self):
        sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    domain VARCHAR(255),
                    zone VARCHAR(10),
                    real_domain VARCHAR(255),
                    status INT
                );
            """
        self.__make_db_request(sql)
        print(f"Таблица {self.table_name} создана в базе {self.db_name}")


    def __get_headers(self):
        user_agents = {
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
            'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'User-agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0',
            'User-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0',
            'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
            'User-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0'
        }
        return user_agents


    def __create_connection(self):
        connection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password, database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        return connection


    def __make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        self.connection.commit()


    def __get_rows_from_txt(self):
        domains_array = []
        start_time = time.time()
        with open(self.file_path, "r") as file:
            for row in file:
                row = row.split(';')
                domains_array.append('http://' + row[0])
        print(f'Файл прочитан за {time.time() - start_time}')
        return domains_array


    async def __process_domain(self, domain, zone, counter, start_time):   
        session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = self.timeout, sock_read = self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)

        try:
            async with session.get(domain, headers=self.__get_headers()) as response: # Тут есть allow_redirects=true/false
                if counter % self.every_printable == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - {response.status}')
                sql_request = f"INSERT IGNORE INTO {self.table_name} (domain, zone, real_domain, status) VALUES ('{domain}', '{zone}', '{response.url}', '{response.status}')"
                self.__make_db_request(sql_request)
        except Exception as e:
            if counter % self.every_printable == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - {e}')
            sql = f"INSERT IGNORE INTO {self.table_name} (domain, zone, real_domain, status) VALUES ('{domain}', '{zone}', '', '404')"
            self.__make_db_request(sql)
        finally:
            await session.close()


    async def __request_all(self):
        start_index = 0
        step = 10000
        requests = []
        start_time = time.time()

        print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')

        for portion in range(0, len(self.domains), step):
            for domain_index in range(start_index, portion):
                requests.append(self.__process_domain(self.domains[domain_index], self.zone, domain_index, start_time))
                if domain_index % 10000 == 0: print(f'Добавлена задача №{domain_index}')
            await asyncio.gather(*requests)
            print(f'---------------------------------- Обработано ссылок с {start_index} до {portion} за {time.time() - start_time} ---------------------------------- ')
            start_index = portion
            requests.clear()
            
        print(f'---------------------------------- Обработка заняла {time.time() - start_time} секунд ---------------------------------- ')


def main():
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_table_name = 'domains'


    status_parser = StatusParser(db_host, db_name, db_user, db_password, db_table_name)
    status_parser.parse_statuses()


if __name__ == "__main__":
    main()
