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

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


class StatusParser:
    def __init__(self, db_host, db_name, db_user, db_password, db_table_name, is_update):
        self.download_link = "https://statonline.ru/domainlist/file?tld="
        self.archives_path = "archives"
        self.ru_archive_path = "archives/ru.zip"
        self.extracted_files_path = "archives/extracted"
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
        self.every_printable = 10000

        self.is_update = is_update

        if is_update: 
            self.domains = self.__make_db_request(f"SELECT * FROM {self.table_name}")
        else:
            self.__create_table_if_not_exists()
            self.__download_ru_domains_file_if_not_exists()
            self.domains = self.__get_rows_from_txt()
        
    def run(self):
      
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.__request_all())
        except OSError:
            print("Тут была OSerror")

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
            result = cursor.fetchall()
        self.connection.commit()
        return result


    def __get_number_of_existing_rows(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.table_name}")
            result = cursor.fetchall()
        self.connection.commit()
        return len(result)

    # TODO: Объединить с __update_domain чтобы не было дублирования кода
    # insert_info = {"domain": domain, "index": index, "zone": zone, "start_time": start_time}
    async def __insert_domain(self, insert_info):   
        session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = self.timeout, sock_read = self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
        url = "http://" + insert_info["domain"]

        try:
            async with session.get(url, headers=self.__get_headers()) as response: # Тут есть allow_redirects=true/false
                if insert_info["index"] % self.every_printable == 0: print(f'№{insert_info["index"]} - {insert_info["domain"]} выполнен за {time.time() - insert_info["start_time"]} - {response.status}')
                response_url = response.url
                if response.status != 200: response_url = ''
                sql = f"INSERT IGNORE INTO {self.table_name} (domain, zone, real_domain, status) VALUES ('{insert_info['domain']}', '{insert_info['zone']}', '{response_url}', '{response.status}')"
                self.__make_db_request(sql)
        except Exception as error:
            if insert_info["index"] % self.every_printable == 0: print(f'№{insert_info["index"]} - {insert_info["domain"]} выполнен за {time.time() - insert_info["start_time"]} - {error}')
            sql = f"INSERT IGNORE INTO {self.table_name} (domain, zone, real_domain, status) VALUES ('{insert_info['domain']}', '{insert_info['zone']}', '', 404)"
            self.__make_db_request(sql)
        finally:
            await session.close()

    # TODO: Объединить с __insert_domain чтобы не было дублирования кода
    # update_info = {"domain": domain, "index": index, "zone": zone, "start_time": start_time}
    async def __update_domain(self, update_info):   
        session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = self.timeout, sock_read = self.timeout)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
        url = "http://" + update_info["domain"]

        try:
            async with session.get(url, headers=self.__get_headers()) as response: # Тут есть allow_redirects=true/false
                if update_info["index"] % self.every_printable == 0: print(f'№{update_info["index"]} - {update_info["domain"]} выполнен за {time.time() - update_info["start_time"]} - {response.status}')
                response_url = response.url
                if response.status != 200: response_url = ''
                sql = f"UPDATE {self.table_name} SET real_domain='{response_url}', status={response.status} WHERE id={update_info['index']}"
                self.__make_db_request(sql)
        except Exception as error:
            if update_info["index"] % self.every_printable == 0: print(f'№{update_info["index"]} - {update_info["domain"]} выполнен за {time.time() - update_info["start_time"]} - {error}')
            sql = f"UPDATE {self.table_name} SET real_domain='', status=404 WHERE id={update_info['index']}"
            self.__make_db_request(sql)
        finally:
            await session.close()


    async def __request_all(self):
        if self.is_update:
            start_index = 0
        else:
            start_index = self.__get_number_of_existing_rows()
            
        requests = []
        start_time = time.time()
        domains_count = len(self.domains)

        print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')
        for portion in range(start_index, domains_count+self.step, self.step):
            if portion == 0: continue # Скип первого шага
            print(f'Создаю {self.step} задач...')
            for domain_index in range(start_index, portion):
                if domain_index > domains_count-1: break # Фикс скипа последнего шага и index out of range error
                insert_info = {
                    "domain": self.domains[domain_index]['domain'],
                    "index": self.domains[domain_index]['id'],
                    "zone": self.domains[domain_index]['zone'],
                    "start_time": start_time
                }
                if self.is_update:
                    requests.append(self.__update_domain(insert_info))
                else:
                    requests.append(self.__insert_domain(insert_info))

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


    status_parser = StatusParser(db_host, db_name, db_user, db_password, db_table_name, False)
    status_parser.run()


if __name__ == "__main__":
    main()
