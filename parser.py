from sqlite3 import connect
import time
import asyncio
import aiohttp
import pymysql
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


def create_connection(host_name, user_name, user_password, db_name):
    connection = pymysql.connect(host=host_name, user=user_name, password=user_password, database=db_name, cursorclass=pymysql.cursors.DictCursor)
    return connection

def insert_into_db(connection, table_name, domain, zone, real_domain, status):
    with connection.cursor() as cursor:
        sql = f"INSERT INTO {table_name} (domain, zone, real_domain, status) VALUES ('{domain}', '{zone}', '{real_domain}', '{status}')"
        cursor.execute(sql)
    connection.commit()


def select_from_db(connection):
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT * FROM `users` WHERE `status`= 200"
        cursor.execute(sql)
        result = cursor.fetchone()
        print(result)


def get_headers():
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


def get_rows_from_txt():
    domains_array = []
    start_time = time.time()
    with open("archives/extracted/ru_domains.txt", "r") as file:
    # with open("archives/extracted/test_10000.txt", "r") as file:
    # with open("archives/extracted/test_10.txt", "r") as file:
        for row in file:
            row = row.split(';')
            domains_array.append('http://' + row[0])
            number = +1
    print(f'Файл прочитан за {time.time() - start_time}')
    return domains_array


async def process_domain(domain, counter, connection, start_time):   
    timeout_sec = 5
    session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = timeout_sec, sock_read = timeout_sec)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
    
    try:
        # TODO: Вот тут без async with вылетали ошибки с unclosed connection
        async with session.get(domain, headers=get_headers()) as response: # Тут есть allow_redirects=true/false
            print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - со статусом {response.status} и реальным url - {response.url}')
            insert_into_db(connection, 'domains', domain, 'ru', response.url, response.status)
            # if counter % 10 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - со статусом {task.status}')
    except Exception as e:
        print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - с ошибкой {e}')
        insert_into_db(connection, 'domains', domain, 'ru', '', 404)

        # if counter % 10 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - с ошибкой {e}')
    finally:
        await session.close()


async def request_all(domains):
    start_time = time.time()
    requests = []
    step = 1000

    connection = create_connection("localhost", "root", "password", "admin_domains")

    start = time.time()
    print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')
    for portion in range(0, len(domains) + step, step):
        if portion - step < 0:
            start_index = 0
        else:
            start_index = portion - step
        for domain_index in range(start_index, portion):
            requests.append(process_domain(domains[domain_index], domain_index, connection, start_time))
            if domain_index % 10000 == 0: print(f'Добавлена задача №{domain_index}')
        await asyncio.gather(*requests)
        print(f'---------------------------------- Обработано ссылок с {start_index} до {portion} за {time.time() - start} ---------------------------------- ')
        requests.clear()
    print(f'---------------------------------- Обработка заняла {time.time() - start} секунд ---------------------------------- ')


def main():
    db_user = 'root'
    db_password = 'password'
    db_name = 'lemon_domains'
    table_name = 'domains'

    loop = asyncio.get_event_loop()
    loop.run_until_complete(request_all(get_rows_from_txt()))


if __name__ == "__main__":
    main()
    
    
