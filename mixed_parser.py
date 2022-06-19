import warnings
warnings.filterwarnings("ignore")

import os
import ssl
import time
import zipfile
import socket
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from multiprocessing import Process
from urllib.request import urlretrieve
from genericpath import exists
import argparse

import pymysql

from db_connector import DbConnector
from table_creator import TableCreator
from validator import Validator


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
        # self.connection = self.db.create_connection()

        self.is_table_exists = True

        ### Параметры парсера ###
        # Можно разбить на connection и readtimeout
        self.timeout = 5
        self.every_printable = 10000
        self.limit = limit
        self.offset = offset

        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
        https_connector = aiohttp.TCPConnector(verify_ssl=True, limit=10000)
        http_connector = aiohttp.TCPConnector(verify_ssl=False, limit=10000)
        self.https_session = aiohttp.ClientSession(connector=https_connector, timeout=session_timeout)
        self.http_session = aiohttp.ClientSession(connector=http_connector, timeout=session_timeout, trust_env=True)


        # Получение списка доменов и создание таблиц
        # request_time = time.time()
        if self.is_table_exists:
            self.domains_count = self.offset + self.limit
            self.domains = domains
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
        

    async def __save_site_info(self, id, domain, zone, response, is_ssl, is_https_redirect, html):
        real_domain = str(response.real_url.human_repr())
        bs4 = BeautifulSoup(html, "lxml")

        # s = time.time()
        title = await self.validator.find_title(bs4)  
        description = await self.validator.find_description(bs4)
        if not await self.validator.is_valid(bs4, title, description, id, real_domain):
            #! invalid site: status = 000
            self.db.make_db_request(f"""
                INSERT INTO {self.statuses_table_name} (id, domain, zone, real_domain, status) 
                VALUE ('{id}', '{domain}', '{zone}', '{real_domain}', {000})
                ON DUPLICATE KEY UPDATE real_domain='{real_domain}', status=000
            """)
            return
        
        keywords = self.validator.find_keywords(bs4)
        cms = self.validator.identify_cms(html) 
        numbers = self.validator.find_phone_numbers(bs4)
        emails = self.validator.find_emails(bs4)
        inns = self.validator.find_inn(bs4)
        tag_id = 0

        data = await asyncio.gather(keywords, cms, numbers, emails, inns)
        keywords = data[0]
        cms = data[1]
        numbers = data[2]
        emails = data[3]
        inns = data[4]
        cities = await self.validator.identify_city_by_inn(inns) if inns else await self.validator.identify_city_by_number(numbers)

        www = 1 if "www." in real_domain else 0
        ip = socket.gethostbyname(response.host)

        inn = ",".join(inns) if inns else ""
        cities = ",".join(cities) if cities else ""
        

        # Информация в таблицу domains
        self.db.make_db_request(f"""
            INSERT INTO {self.statuses_table_name} (id, domain, zone, real_domain, status) 
            VALUE ('{id}', '{domain}', '{zone}', '{real_domain}', {200})
            ON DUPLICATE KEY UPDATE real_domain='{real_domain}', status=200
        """)

        # Информация в таблицу domain_info
        self.db.make_db_request(f"""
            INSERT INTO {self.domain_info_table_name} (domain_id, title, description, keywords, city, inn, cms, is_www, is_ssl, is_https_redirect, ip, tag_id) 
            VALUE ({id}, '{title}', '{description}', '{keywords}', '{cities}', '{inn}', '{cms}', '{www}', '{is_ssl}', '{is_https_redirect}', '{ip}', {tag_id})
            ON DUPLICATE KEY UPDATE title='{title}', description='{description}', keywords='{keywords}', city='{cities}', inn='{inn}', cms='{cms}', is_www='{www}', is_ssl='{is_ssl}', is_https_redirect='{is_https_redirect}',  ip='{ip}', tag_id={tag_id}
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


    async def __http_request(self, domain):
        http_url = "http://" + domain
        response = await self.http_session.get(http_url, headers=self.__get_headers())
        return response


    async def __https_request(self, domain):
        try:
            https_url = "https://" + domain
            response = await self.https_session.get(https_url, headers=self.__get_headers())
            return response
        
        except (ssl.CertificateError, aiohttp.client_exceptions.ClientConnectorCertificateError):
            return False

  
    async def __make_domain_request(self, domain_base_info):
        id = domain_base_info["id"]
        domain = domain_base_info["domain"]
        zone = domain_base_info["zone"]

        try:
            results = await asyncio.gather(self.__http_request(domain_base_info["domain"]), self.__https_request(domain_base_info["domain"]), return_exceptions=False)
            http_response = results[0]
            https_response = results[1]

            # https redirect check
            if not http_response: return
            is_https_redirect = 1 if "https://" in http_response.real_url.human_repr() else 0
            is_ssl = is_https_redirect

            # no https redirect check but with ssl
            if https_response: is_ssl = 1 if "https://" in https_response.real_url.human_repr() else 0

            if http_response.status == 200:
                html = await http_response.text()
                await self.__save_site_info(id, domain, zone, http_response, is_ssl, is_https_redirect, html)
                
            elif http_response.status:
                self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name} 
                    SET status={http_response.status}
                    WHERE id = {id}    
                """)

        # status = 404
        except (
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            ConnectionResetError,
        ) as error:
            self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 404
                    WHERE id = {id}
                """)

        # status = 400
        except (
            aiohttp.client_exceptions.ClientPayloadError,
            aiohttp.client_exceptions.ClientResponseError
        ) as error:
            self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 400
                    WHERE id = {id}
                """)

        # status TimeoutError = 408 status
        except aiohttp.client_exceptions.ServerTimeoutError as error:
            self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 408
                    WHERE id = {id}
                """)

        except aiohttp.client_exceptions.ServerDisconnectedError as error:
            # TODO: Придумать как обойти это
            # Сайт либо заблокироан, либо без ssl сертификата
            pass
        
        # status = 888
        except (UnicodeDecodeError, UnicodeEncodeError,) as error:
            self.db.make_db_request(f"""
                    UPDATE {self.statuses_table_name}
                    SET status = 888
                    WHERE id = {id}
                """)

        except ssl.CertificateError:
            pass
  
        except (pymysql.err.ProgrammingError, pymysql.err.DataError, ValueError) as error:
            # logging.error(error)
            print(error)

        
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
        # print(f"Парсинг с {self.offset} по {self.offset+self.limit} начался")
        # TODO: Разбить на 4 части и запустить 4 процесса
        await asyncio.gather(*requests)
        await self.http_session.close()
        await self.https_session.close()
        requests.clear()
        # print(f"-------- Обработка {self.domains_count} запросов заняла  {time.time() - start_time} секунд --------")



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


def load_dot_env():
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)


def create_parser(portion, offset, domains):
    # logging.basicConfig(filename="logs.log", encoding="utf-8")
    parser = MixedParser(portion, offset, domains)
    parser.run()


def main():
    ssl.match_hostname = lambda cert, hostname: True
    load_dot_env()

    # Настройка аргументов при запуске через консоль
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--offset")
    arg_parser.add_argument("--cores")
    arg_parser.add_argument("--portion")
    args = arg_parser.parse_args()

    domains_count = DbConnector().make_single_db_request(
        "SELECT count(*) FROM domains")["count(*)"]
    first_id = DbConnector().make_single_db_request(
        "SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]

    # * Начальный индекс для парсинга
    offset = 0
    if args.offset:
        offset = int(args.offset)
    # * Количество процессов парсера
    cores_number = 4
    if args.cores:
        cores_number = int(args.cores)
    # * Одновременно обрабатываемая порция
    portion = 10000
    if args.portion:
        portion = int(args.portion)

    start_index = first_id + offset

    global_start_time = time.time()
    processes = []

    step = portion // cores_number + portion % cores_number

    # Для небольшого количества записей
    if step > domains_count:
        step = domains_count
        cores_number = 1

    for offset in range(start_index, domains_count + start_index, step):
        portion_start_time = time.time()
        domains = DbConnector().make_db_request(f"SELECT * FROM domains WHERE id >= {offset} LIMIT {step}")
        process = Process(target=create_parser, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == cores_number:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {offset-(step*3)} по {offset+step} за {time.time() - portion_start_time} - Общее время парсинга: {time.time() - global_start_time}")

    print(f"Парсинг c {start_index} по {domains_count} закончился за {time.time() - global_start_time}")


if __name__ == "__main__":
    main()
