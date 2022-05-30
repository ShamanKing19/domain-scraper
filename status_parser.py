from genericpath import exists
import os, time
from urllib.request import urlretrieve
import zipfile
import asyncio
import aiohttp
import pymysql
from dotenv import load_dotenv
import argparse

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


class StatusParser:
    def __init__(self, db_host, db_name, db_user, db_password, db_table_name):
        self.download_link = "https://statonline.ru/domainlist/file?tld="
        self.archives_path = "archives"
        self.ru_archive_path = "archives/ru.zip"
        self.extracted_files_path = "archives/extracted"
        self.file_path = "archives/extracted/ru_domains.txt"
        self.zone = 'ru'

        # Настройка аргумента --offset при запуске через консоль
        self.offset = 0
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument("--offset")
        arg_parser.add_argument("--table") # 1 - table_exists = True, 0 - table_exists = False
        args = arg_parser.parse_args()
        if args.offset: self.offset = args.offset
        
        # Настройка аргумента --table при запуске через консоль
        self.is_table_exists = True
        if args.table:
            self.is_table_exists = bool(int(args.table))

        
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
        self.every_printable = 100

        # self.is_table_exists = is_table_exists

        if self.is_table_exists:
            request_time = time.time()
            # print("Started 5000000 query")
            self.domains_count = len(self.__make_db_request(F"SELECT * FROM {self.table_name}"))
            self.domains = self.__make_db_request(f"SELECT * FROM {self.table_name} LIMIT {self.domains_count} OFFSET {self.offset}")
            self.domains_count = len(self.domains)
            # print(f"Ended 5000000 query for {time.time() - request_time} secs")
        else:
            self.__create_table_if_not_exists()
            self.__download_ru_domains_file_if_not_exists()
            self.domains = self.__get_rows_from_txt()
            self.domains_count = len(self.domains)
        

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__request_all())


    def __download_ru_domains_file_if_not_exists(self):
        if (exists(self.file_path)): return
        if not (exists(self.archives_path)): os.mkdir(self.archives_path)
        if not (exists(self.extracted_files_path)): os.mkdir(self.extracted_files_path)
        start_time = time.time()
        print("Началась загрузка архива с доменами...")
        urlretrieve(self.download_link, self.ru_archive_path)
        print(f"Файл загружен за {time.time() - start_time}")
        
        print("Начата распаковка файла")
        with zipfile.ZipFile(self.ru_archive_path, 'r') as zip_file:
            zip_file.extractall(self.extracted_files_path)
        print("Файл распакован")
        os.remove(self.ru_archive_path)
        print("Архив удалён")


    def __get_rows_from_txt(self):
        domains = []
        start_time = time.time()
        counter = 0
        with open(self.file_path, "r") as file:
            for row in file:
                row = row.split(';')
                domains.append({
                    "id": counter,
                    "domain": row[0],
                    "zone": row[1].split("-")[-1],
                })
                counter += 1
        print(f'Файл прочитан за {time.time() - start_time}')
        return domains


    def __create_table_if_not_exists(self):
        with open("migrations/domains_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())


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
#         print(f"started '{sql}' request")
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
#         print(f"ended '{sql}' request")
        return result


    # insert_info = {"domain": domain, "index": index, "zone": zone, "start_time": start_time}
    async def __insert_domain(self, insert_info):   
        session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = self.timeout, sock_read = self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
        url = "http://" + insert_info["domain"]

        try:
            async with session.get(url, headers=self.__get_headers()) as response: # Тут есть allow_redirects=true/false
                if insert_info["index"] % self.every_printable == 0: print(f'№{insert_info["index"]} - {insert_info["domain"]} выполнен за {time.time() - insert_info["start_time"]} - {response.status}')
                real_domain = response.url
                status = response.status
                if status != 200: real_domain = ''

        except Exception as error:
            if insert_info["index"] % self.every_printable == 0: print(f'№{insert_info["index"]} - {insert_info["domain"]} выполнен за {time.time() - insert_info["start_time"]} - {error}')
            real_domain = ''
            status = 404

        finally:
            sql = f"""
                INSERT INTO {self.table_name} (domain, zone, real_domain, status) 
                VALUES ('{insert_info['domain']}', '{insert_info['zone']}', '{real_domain}', '{status}')
                ON DUPLICATE KEY UPDATE real_domain='{real_domain}', status='{status}'
            """
            self.__make_db_request(sql)
            await session.close()


    # TODO: Придумать настройку чтобы была возможность продолжить парсинг после остановки не обновляя предыдущие записи
    async def __request_all(self):
        start_index = 0
        #start_index = self.__get_number_of_existing_rows()
        
        requests = []
        start_time = time.time()

        print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')
        for portion in range(start_index, self.domains_count+self.step, self.step):
            if portion == 0: continue # Скип первого шага
            # print(f'Создаю {self.step} задач...')
            for domain_index in range(start_index, portion):
                if domain_index > self.domains_count-1: break # Фикс скипа последнего шага и index out of range error
                insert_info = {
                    "domain": self.domains[domain_index]['domain'],
                    "index": self.domains[domain_index]['id'],
                    "zone": self.domains[domain_index]['zone'],
                    "start_time": start_time
                }
                requests.append(self.__insert_domain(insert_info))
#             print(f"Парсинг c {start_index} по {portion} начался")
            await asyncio.gather(*requests)
#             print(f'---------------------------------- Обработано ссылок с {start_index} до {portion} за {time.time() - start_time} ---------------------------------- ')
            start_index = portion
            requests.clear()
            
        print(f'---------------------------------- Обработка {len(self.domains)} запросов заняла  {time.time() - start_time} секунд ---------------------------------- ')


def main():
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_table_name = 'domains'


    status_parser = StatusParser(db_host, db_name, db_user, db_password, db_table_name)
    status_parser.run()


if __name__ == "__main__":
    main()
