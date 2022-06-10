import gc
from multiprocessing import Process
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
from db_connector import DbConnector

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
warnings.filterwarnings("ignore", category=DeprecationWarning)


class MixedParser:
    def __init__(self, limit, offset, domains):
        # Данные для скачивания файла
        self.download_link = "https://statonline.ru/domainlist/file?tld="
        self.archives_path = "archives"
        self.ru_archive_path = "archives/ru.zip"
        self.extracted_files_path = "archives/extracted"
        self.file_path = "archives/extracted/ru_domains.txt"

        
        self.statuses_table_name = "domains"
        self.domain_info_table_name = "domain_info"
        self.domain_phones_table_name = "domain_phones"
        self.domain_emails_table_name = "domain_emails"

        self.db = DbConnector()
        self.connection = self.db.create_connection()

        self.is_table_exists = True

        ### Параметры парсера ###
        # Можно разбить на connection и readtimeout
        self.timeout = 5
        self.every_printable = 1000
        self.limit = limit
        self.offset = offset

        # Получение списка доменов и создание таблиц
        # request_time = time.time()
        if self.is_table_exists:
            # print(f"Запрос c OFFSET={self.offset} отправлен")
            self.domains_count = self.offset + self.limit
            self.domains = domains
            # print(f"Запрос выполнен за {time.time() - request_time} секунд")
        # TODO: Сделать чтобы в случае чтения с файла он тоже брал инфу порциями
        else:
            table_creator = TableCreator()
            table_creator.create_tables()
            self.__download_ru_domains_file_if_not_exists()
            self.domains = self.__get_rows_from_txt()
            self.domains_count = len(self.domains)


        # Подготовленные данные для парсинга
        self.categories = self.db.make_db_request("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        self.regions = self.db.make_db_request("""
                    SELECT * FROM regions
                """)        
        self.validator = Validator(self.categories, self.regions)


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__parse_all_domains())


    async def __save_site_info(self, id, domain, zone, real_url, html):
        bs4 = BeautifulSoup(html, "lxml")        
    
        # s = time.time()
        title = await self.validator.find_title(bs4)  # Возврат: string
        description = await self.validator.find_description(bs4)  # Возврат: string
        if not await self.validator.is_valid(bs4, title, description): return
        cms = await self.validator.identify_cms(html)  # Возврат: string
        numbers = await self.validator.find_phone_numbers(bs4) # ["number1", "number2"...]
        emails = await self.validator.find_emails(bs4) # Возврат: {"mobile_numbers": [], "emails:": []}
        inns = await self.validator.find_inn(bs4)  # Возврат: ["ИНН1", "ИНН2", ...]
        tag_id = await self.validator.identify_category(title, description) # Возврат: id из таблицы tags
        # tag_id = await self.validator.identify_real_category(bs4, title, description) # Возврат: id из таблицы tags
        cities_via_number = await self.validator.identify_city_by_number(numbers) # Возврат: ["Город1", "Город2", ...]
        cities_via_inn = await self.validator.identify_city_by_inn(inns) # Возврат: ["Москва", "Калининградская область", "Архангельская область"...]
        # print(f"{id} - Времени прошло - {time.time() - s}")

        # Приоритет определения города
        if len(cities_via_inn) > 0:
            city = ",".join(cities_via_inn)
        elif len(cities_via_number) > 0:
            city = ",".join(cities_via_number)
        else:
            city = ""
        inn = ",".join(inns)


        # Информация в таблицу domains
        self.db.make_db_request(f"""
            INSERT INTO {self.statuses_table_name} (id, domain, zone, real_domain, status) 
            VALUE ('{id}', '{domain}', '{zone}', '{real_url}', {200})
            ON DUPLICATE KEY UPDATE real_domain='{real_url}', status=200
        """)


        # Информация в таблицу domain_info
        self.db.make_db_request(f"""
            INSERT INTO {self.domain_info_table_name} (domain_id, title, description, city, inn, cms, tag_id) 
            VALUE ({id}, '{title}', '{description}', '{city}', '{inn}', '{cms}', {tag_id})
            ON DUPLICATE KEY UPDATE title='{title}', description='{description}', city='{city}', inn='{inn}', cms='{cms}', tag_id={tag_id}
        """)

        # Информация в таблицу domain_phones
        for number in numbers:
            self.db.make_db_request(f"""
                INSERT INTO {self.domain_phones_table_name} (domain_id, number) 
                VALUE ({id}, {number})
                ON DUPLICATE KEY UPDATE number='{number}'
            """)

        # Информация в таблицу domain_emails
        for email in emails:
            email = email.strip()
            self.db.make_db_request(f"""
                INSERT INTO {self.domain_emails_table_name} (domain_id, email) 
                VALUE ({id}, '{email}')
                ON DUPLICATE KEY UPDATE email='{email}'
            """)


    async def __make_domain_request(self, domain_base_info):
        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout, trust_env=True)
        id = domain_base_info["id"]
        domain = domain_base_info["domain"]
        url = "http://" + domain
        zone = domain_base_info["zone"]
        try:
            async with session.get(url, headers=self.__get_headers()) as response:
                if response.status == 200:
                    real_url = response.url
                    html = await response.text()
                    await self.__save_site_info(id, domain, zone, real_url, html)
                    if id % self.every_printable == 0: print(f"{id} - {response.url}")
                
                elif response.status:
                    self.db.make_db_request(f"""
                        UPDATE {self.statuses_table_name} 
                        SET status={response.status}
                        WHERE id = {id}    
                    """)
                    if id % self.every_printable == 0: print(f"{id} - {response.status}")
        
        except Exception as error:
            # ! На таймаут ставлю 408 status
            # ! Timeout = 408 status
            if "timeout" in str(error):
                self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 408
                    WHERE id = {id}
                """)
            else:
                # Остальным ошибкам ставлю 404-й статус
                self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 404
                    WHERE id = {id}
                """)
            if id % self.every_printable == 0: print(f"{id} - {error}")

        finally:
            await session.close()


    async def __parse_all_domains(self):
        requests = []
        start_time = time.time()

        # print(f"\n---------------------------------- Начал обработку запросов ----------------------------------\n")
        for domain in self.domains:
            domain_base_info = {
                "domain": domain["domain"],
                "id": domain["id"],
               "zone": domain["zone"],
               "start_time": start_time
            }
            requests.append(self.__make_domain_request(domain_base_info))
        print(f"Парсинг с {self.offset} по {self.offset+self.limit} начался")
        # TODO: Разбить на 4 части и запустить 4 процесса  
        await asyncio.gather(*requests)
        requests.clear()

        print(f"---------------------------------- Обработка {self.domains_count} запросов заняла  {time.time() - start_time} секунд ---------------------------------- ")


    def __download_ru_domains_file_if_not_exists(self):
        if (exists(self.file_path)): return
        if not (exists(self.archives_path)): os.mkdir(self.archives_path)
        if not (exists(self.extracted_files_path)): os.mkdir(self.extracted_files_path)
        start_time = time.time()
        print("Началась загрузка архива с доменами...")
        urlretrieve(self.download_link, self.ru_archive_path)
        print(f"Файл загружен за {time.time() - start_time}")

        print("Начата распаковка файла")
        with zipfile.ZipFile(self.ru_archive_path, "r") as zip_file:
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
                row = row.split(";")
                domains.append({
                    "id": counter,
                    "domain": row[0],
                    "zone": row[1].split("-")[-1],
                })
                counter += 1
        print(f"Файл прочитан за {time.time() - start_time}")
        return domains


    def __get_headers(self):
        user_agents = {
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
            "User-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"
        }
        return user_agents



def main():
    # Настройка аргумента --offset при запуске через консоль
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--offset")
    args = arg_parser.parse_args()

    domains_count = DbConnector().make_single_db_request("SELECT count(*) FROM domains")["count(*)"]
    first_id = DbConnector().make_single_db_request("SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]


    # * Одновременно обрабатываемая порция
    portion = 2500
    process_number = 4
    
    # * Начальный индекс для парсинга
    offset = 0
    if args.offset: offset = int(args.offset)

    start_index = first_id + offset
    
    start_time = time.time() 
    processes = []

    for offset in range(start_index, domains_count, portion):
        domains = DbConnector().make_db_request(f"SELECT * FROM domains WHERE id > {offset} LIMIT {portion}")    
        process = Process(target=create_parser, args=(portion, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == process_number:
            for process in processes:
                process.join()
            processes.clear()

    print(f"Парсинг закончился за {time.time() - start_time}")


def create_parser(portion, offset, domains):
    parser = MixedParser(portion, offset, domains)
    parser.run()


if __name__ == "__main__":
    main()
