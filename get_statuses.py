import os, time
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


def create_connection(db_host, db_name, db_user, db_password):
    connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name, cursorclass=pymysql.cursors.DictCursor)
    return connection


def insert_into_db(connection, table_name, domain, zone, real_domain, status):
    with connection.cursor() as cursor:
        sql = f"INSERT INTO {table_name} (domain, zone, real_domain, status) VALUES ('{domain}', '{zone}', '{real_domain}', '{status}')"
        cursor.execute(sql)
    connection.commit()


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
        for row in file:
            row = row.split(';')
            domains_array.append('http://' + row[0])
    print(f'Файл прочитан за {time.time() - start_time}')
    return domains_array


async def process_domain(domain, counter, connection, start_time):   
    timeout_sec = 5
    session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = timeout_sec, sock_read = timeout_sec)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)

    try:
        async with session.get(domain, headers=get_headers()) as response: # Тут есть allow_redirects=true/false
            if counter % 10000 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - {response.status}')
            insert_into_db(connection, 'domains', domain, 'ru', response.url, response.status)
    except Exception as e:
        if counter % 10000 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - {e}')
        insert_into_db(connection, 'domains', domain, 'ru', '', 404)
    finally:
        await session.close()

async def request_all(domains, db_host, db_name, db_user, db_password):
    start_index = 0
    step = 10000
    requests = []

    connection = create_connection(db_host, db_name, db_user, db_password)
    start_time = time.time()

    print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')

    for portion in range(0, len(domains), step):
        for domain_index in range(start_index, portion):
            requests.append(process_domain(domains[domain_index], domain_index, connection, start_time))
            if domain_index % 10000 == 0: print(f'Добавлена задача №{domain_index}')
        await asyncio.gather(*requests)
        print(f'---------------------------------- Обработано ссылок с {start_index} до {portion} за {time.time() - start_time} ---------------------------------- ')
        start_index = portion
        requests.clear()
        
    print(f'---------------------------------- Обработка заняла {time.time() - start_time} секунд ---------------------------------- ')


def main():
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_DATABASE")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(request_all(get_rows_from_txt(), db_host, db_name, db_user, db_password))
    

if __name__ == "__main__":
    main()
