import time
import asyncio
import aiohttp

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


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


async def process_domain(domain, counter):   
    timeout_sec = 5
    session_timeout = aiohttp.ClientTimeout(total = None, sock_connect = timeout_sec, sock_read = timeout_sec)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)

    start_time = time.time()
    
    try:
        # TODO: Вот тут без async with вылетали ошибки с unclosed connection
        async with session.get(domain, headers=get_headers()) as response: # Тут есть allow_redirects=true/false
            print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - со статусом {response.status}')
            # if counter % 10 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - со статусом {task.status}')
            del response
    except Exception as e:
        print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - с ошибкой {e}')
        # if counter % 10 == 0: print(f'№{counter} - {domain} выполнен за {time.time() - start_time} - с ошибкой {e}')
    finally:
        await session.close()


async def request_all(domains, limit):
    counter = 1
    start_time = time.time()
    requests = []
    limit = 10000

    for domain in domains:
        requests.append(process_domain(domain, counter))
        if counter % 1000 == 0: print(f'Добавлена задача №{counter}')
        if limit != 0 and counter == limit:
            print(f'Создано {limit} задач')
            break
        counter += 1

    start = time.time()
    print(f'\n---------------------------------- Начал обработку запросов ----------------------------------\n')
    responses = await asyncio.gather(*requests)
    print(f'---------------------------------- {len(requests)} запросов обработано за {time.time() - start} ---------------------------------- ')


async def insert_into_db(db_name, table_name, data):
    pass


def main():
    limit = 10000
    loop = asyncio.get_event_loop()
    loop.run_until_complete(request_all(get_rows_from_txt(), limit))
    
if __name__ == "__main__":
    main()
    
    
