import warnings
from genericpath import exists
import os
import time
from urllib.request import urlretrieve
import zipfile
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pymysql
from dotenv import load_dotenv
import argparse

from table_creator import TableCreator
from validator import Validator

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
warnings.filterwarnings("ignore", category=DeprecationWarning)


class MixedParser:
    def __init__(self):
        # Данные для скачивания файла
        self.download_link = "https://statonline.ru/domainlist/file?tld="
        self.archives_path = "archives"
        self.ru_archive_path = "archives/ru.zip"
        self.extracted_files_path = "archives/extracted"
        self.file_path = "archives/extracted/ru_domains.txt"

        # Подключение к бд
        self.db_host = os.environ.get("DB_HOST")
        self.db_name = os.environ.get("DB_DATABASE")
        self.db_user = os.environ.get("DB_USER")
        self.db_password = os.environ.get("DB_PASSWORD")
        self.statuses_table_name = "domains"
        self.domain_info_table_name = 'domain_info'
        self.domain_phones_table_name = 'domain_phones'
        self.domain_emails_table_name = 'domain_emails'
        self.connection = self.__create_connection()

        # Настройка аргумента --offset при запуске через консоль
        self.offset = 0
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument("--offset")
        # 1 - table_exists = True, 0 - table_exists = False
        arg_parser.add_argument("--table")
        args = arg_parser.parse_args()
        if args.offset:
            self.offset = int(args.offset)

        # Настройка аргумента --table при запуске через консоль
        self.is_table_exists = True
        if args.table:
            self.is_table_exists = bool(int(args.table))

        ### Параметры парсера ###
        # Можно разбить на connection и readtimeout
        self.timeout = 5
        self.step = 10000
        self.every_printable = 10

        # Получение списка доменов и создание таблиц
        request_time = time.time()
        if self.is_table_exists:
            print(f"Запрос c OFFSET={self.offset} отправлен")
            self.domains_count = self.__make_db_request(
                f"SELECT count(*) FROM {self.statuses_table_name}")[0]['count(*)']
            self.domains = self.__make_db_request(
                f"SELECT * FROM {self.statuses_table_name} LIMIT {self.domains_count} OFFSET {self.offset}")
            print(f"Запрос выполнен за {time.time() - request_time} секунд")
        else:
            table_creator = TableCreator()
            table_creator.create_tables()
            self.__download_ru_domains_file_if_not_exists()
            self.domains = self.__get_rows_from_txt()
            self.domains_count = len(self.domains)


        # Данные для парсинга
        self.categories = self.__make_db_request("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        self.regions = self.__make_db_request("""
                    SELECT * FROM regions
                """)        
        self.validator = Validator(self.categories, self.regions)

        
        


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__parse_all_domains())



    async def __save_site_info(self, id, domain, real_url, html):
        bs4 = BeautifulSoup(html, "lxml")        
    
        valid = await self.validator.is_valid(html)
        if not valid:  return

        title = await self.validator.find_title(bs4)  # Возврат: string
        description = await self.validator.find_description(bs4)  # Возврат: string
        cms = await self.validator.identify_cms(html)  # Возврат: string
        numbers, emails = await self.validator.find_contacts(bs4) # Возврат: {'mobile_numbers': [], 'emails:': []}
        inns = await self.validator.find_inn(bs4.text)  # Возврат: ['ИНН1', 'ИНН2', ...]
        tag_id = await self.validator.identify_category(title, description) # Возврат: id из таблицы tags
        cities_via_number = await self.validator.identify_city_by_number(numbers) # Возврат: ['Город1', 'Город2', ...]
        cities_via_inn = await self.validator.identify_city_by_inn(inns) # Возврат: ['Москва', 'Калининградская область', 'Архангельская область'...]

        # Тут можно попробовать убрать проверку, может быть оно не сломается
        if len(cities_via_inn) > 0:
            city = ",".join(cities_via_inn)
        elif len(cities_via_number) > 0:
            city = ",".join(cities_via_number)
        else:
            city = ""
        inn = ",".join(inns)

        # TODO: Записывать "Zone"
        # TODO: Сделать запись в domains
        # Информация в таблицу domains
        self.__make_db_request(f"""
            INSERT INTO {self.statuses_table_name} (id, domain, real_domain, status) 
            VALUE ('{id}', '{domain}', '{real_url}', {200})
            ON DUPLICATE KEY UPDATE real_domain='{real_url}', status=200
        """)


        # Информация в таблицу domain_info
        self.__make_db_request(f"""
            INSERT INTO {self.domain_info_table_name} (id, domain_id, title, description, city, inn, cms, tag_id) 
            VALUE ({id}, {id}, '{title}', '{description}', '{city}', '{inn}', '{cms}', {tag_id})
            ON DUPLICATE KEY UPDATE title='{title}', description='{description}', city='{city}', inn='{inn}', cms='{cms}', tag_id={tag_id}
        """)

        # Информация в таблицу domain_phones
        for number in numbers:
            self.__make_db_request(f"""
                INSERT INTO {self.domain_phones_table_name} (id, domain_id, number) 
                VALUE ({id}, {id}, {number})
                ON DUPLICATE KEY UPDATE number='{number}'
            """)

        # Информация в таблицу domain_emails
        for email in emails:
            email = email.strip()
            self.__make_db_request(f"""
                INSERT INTO {self.domain_emails_table_name} (id, domain_id, email) 
                VALUE ({id}, {id}, '{email}')
                ON DUPLICATE KEY UPDATE email='{email}'
            """)


    async def __parse_domain(self, domain_base_info):
        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
        url = "http://" + domain_base_info['domain']
        id = domain_base_info['id']
        domain = domain_base_info['domain']
        try:
            async with session.get(url, headers=self.__get_headers()) as response:
                if response.status == 200:
                    real_url = response.url
                    html = await response.text()
                    await self.__save_site_info(id, domain, real_url, html)
                    if id % self.every_printable == 0: print(f"{id} - {response.url}")
        except Exception as error:
            if id % self.every_printable == 0: print(f"{id} - {error}")
        finally:
            await session.close()


    async def __parse_all_domains(self):
        start_index = 0
        requests = []
        start_time = time.time()

        print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')
        for portion in range(start_index, self.domains_count+self.step, self.step):
            if portion == 0: continue  # Скип первого шага
            for domain_index in range(start_index, portion):
                if domain_index > self.domains_count-1-self.offset: break  # Фикс скипа последнего шага и index out of range error
                domain_base_info = {
                    "domain": self.domains[domain_index]['domain'],
                    "id": self.domains[domain_index]['id'],
                    "zone": self.domains[domain_index]['zone'],
                    "start_time": start_time
                }
                requests.append(self.__parse_domain(domain_base_info))
            print(f"Парсинг c {start_index} по {portion} начался")
            await asyncio.gather(*requests)
            print(f'---------------------------------- Обработано ссылок с {start_index} до {portion} за {time.time() - start_time} ---------------------------------- ')
            start_index = portion
            requests.clear()

        print(
            f'---------------------------------- Обработка {len(self.domains)} запросов заняла  {time.time() - start_time} секунд ---------------------------------- ')

    def __download_ru_domains_file_if_not_exists(self):
        if (exists(self.file_path)):
            return
        if not (exists(self.archives_path)):
            os.mkdir(self.archives_path)
        if not (exists(self.extracted_files_path)):
            os.mkdir(self.extracted_files_path)
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
        connection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password,
                                     database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        return connection

    def __make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result


def main():

    status_parser = MixedParser()
    status_parser.run()


if __name__ == "__main__":
    main()
