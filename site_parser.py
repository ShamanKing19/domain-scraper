import warnings
import os
import time
import re
import json

from requests import head

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
try:
    from bs4 import BeautifulSoup
except:
    os.system("pip install beautifulsoup4")
try:
    import lxml
except:
    os.system("pip install lxml")
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
warnings.filterwarnings("ignore", category=DeprecationWarning)


def create_connection(host_name, user_name, user_password, db_name):
    connection = pymysql.connect(host=host_name, user=user_name, password=user_password,
                                 database=db_name, cursorclass=pymysql.cursors.DictCursor)
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


async def parse_site(domain, counter, start_time):
    timeout_sec = 30
    session_timeout = aiohttp.ClientTimeout(
        total=None, sock_connect=timeout_sec, sock_read=timeout_sec)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
        verify_ssl=False), timeout=session_timeout)
    try:
        async with session.get(domain, headers=get_headers()) as response:
            url = response.url
            headers = response.headers
            html = await response.text()

            print(
                f"№{counter} - {time.time() - start_time} ----------------------------------------------------")
            print(f"URL: {url}")
            await identify_site(html)
            counter += 1
    except Exception as error:
        print(error)
    finally:
        await session.close()


def select_from_db(connection):
    with connection.cursor() as cursor:
        sql = "SELECT real_domain FROM domains WHERE status=200"
        # sql = "SELECT real_domain FROM domains WHERE status=200 LIMIT 10000"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


async def identify_site(html):
    bs4 = BeautifulSoup(html, "lxml")
    valid = await check_valid(bs4)
    if valid:
        print(f"Valid: {valid}")
    if not valid:
        return
    # TODO: Проверить что будет быстрее: каждый раз проверять на наличие тэга или просто ловить исключение
    title = await find_title(bs4)
    description = await find_description(bs4)
    cms = await identify_cms(bs4)
    contacts = await find_contacts(bs4)
    inn = await find_inn(bs4.text)
    # cities = await find_cities(contacts['mobile_phones']) # не успевает схватывать и выводить номер

    print(f"Title: {title}")
    print(f"Description: {description}")
    print(f"CMS: {cms}")
    print(f"Контакты: {contacts}")
    print(f"ИНН: {inn}\n")
    # print(f"Город: {cities}\n")

    # print(f"Категория: {category}\n")
    # print(f"Подкатегория: {subcategory}\n")

async def find_inn(text):
    # TODO: Отсечь личшние символы вначеле и в конце, потому что схватывает рандомные числа из ссылок
    ideal_pattern = re.compile('\b\d{4}\d{6}\d{2}\\b|\\b\d{4}\d{5}\d{1}\\b')
    all_inns = list(set(re.findall(ideal_pattern, text)))
    correct_inns = []

    coefficients_10 = [2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
    coefficients_12_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0, 0]
    coefficients_12_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
    
    # Тут должны быть формулы проверки корректности ИНН
    for inn in all_inns:
        if len(inn) == 10:
            key = 0
            # Расчёт ключа по коэффициентам
            for i, digit in enumerate(inn, start=0):
                key += int(digit) * coefficients_10[i]
            # key mod 11
            key_value = key - (key // 11) * 11
            if key_value == int(inn[-1]):
                correct_inns.append(inn)
        
        elif len(inn) == 12:
            key1 = 0
            key2 = 0
            # Расчёт ключа №1 по коэффициентам
            for i, digit in enumerate(inn, start=0):
                key1 += int(digit) * coefficients_12_1[i]
            # key mod 11 №1
            key_value1 = key1 - (key1 // 11) * 11
            # Расчёт ключа №2 по коэффициентам
            for i, digit in enumerate(inn, start=0):
                key2 += int(digit) * coefficients_12_2[i]
            key_value2 = key2 - (key2 // 11) * 11
            # key mod 11 №2
            if key_value1 == int(inn[10]) and key_value2 == int(inn[11]):
                correct_inns.append(inn)
    return correct_inns

# TODO: Проверить скорость с более точным поиском
# Находить контакты можно чаще, но медленнее
# можно искать все классы, в которых будет phone, contacts
async def find_contacts(bs4):
    links = bs4.findAll('a')
    mobile_numbers = []
    emails = []
    for a in links:
        for attribute in a.attrs:
            if attribute == 'href':
                if 'tel:' in a[attribute]:
                    # TODO: Убрать все символы кроме цифр
                    mobile_number = a[attribute].split(':')[-1].strip()
                    s1 = re.sub("[^A-Za-z0-9]", "", mobile_number)
                    mobile_numbers.append(s1)
                elif 'mailto:' in a[attribute]:
                    email = a[attribute].split(':')[-1].strip()
                    emails.append(email)
    # Так удаляю дубликаты
    return {'mobile_numbers': list(set(mobile_numbers)), 'emails:': list(set(emails))}


async def identify_cms(html):
    cms_keywords = {
        '<link href="/bitrix/js/main': 'Bitrix',
        '/wp-content/themes/':  'Wordpress',
        '<meta name="modxru"':  'ModX',
        '<script type="text/javascript" src="/netcat':  'Netcat',
        '<script src="/phpshop':  'PhpShop',
        '<script type="text/x-magento-init':  'Magento',
        '/wa-data/public': 'Shop-Script',
        'catalog/view/theme':  'OpenCart',
        'data-drupal-':  'Drupal',
        '<meta name="generator" content="Joomla':  'Joomla',
        'var dle_admin': 'DataLife Engine'
    }
    for keyword in cms_keywords:
        if keyword in html:
            return cms_keywords[keyword]


async def check_valid(bs4):
    text = bs4.text
    valid = True
    invalid_keywords = ['reg.ru', 'линковка', 'купить домен', 'домен припаркован', 'только что создан', 'сайт создан', 'приобрести домен',
                        'получить домен', 'получи домен', 'домен продаётся', 'domain for sale', 'домен продается', 'домен продаётся',
                        'домен недоступен', 'домен временно недоступен', 'вы владелец сайта?', 'технические работы', 'сайт отключен', 'сайт заблокирован',
                        'сайт недоступен', 'это тестовый сервер', 'это тестовый сайт', 'срок регистрации', 'the site is',
                        '503 service', '404 not found', 'fatal error', 'настройте домен', 'under construction',  'не опубликован',
                        'домен зарегистрирован', 'доступ ограничен', 'Welcome to nginx', 'owner of this ']
    for keyword in invalid_keywords:
        if keyword in text.lower():
            print(f'Invalid cuz of: {keyword}\n')
            return False
    return valid


async def find_description(bs4):
    description = ''
    meta_tags = bs4.findAll('meta')
    for meta in meta_tags:
        for attribute in meta.attrs:
            if 'description' in meta[attribute]:
                description = meta['content'].strip()
                return description
    return ''


async def find_title(bs4):
    title = ''
    titles = bs4.findAll('title')
    for title in titles:
        return title.get_text().replace('\n', '').strip()
    return title


async def parse_all_sites(domains):
    # test_numbers = [79184251015, 79278921675, 89118535474]
    # for number in test_numbers:
    #     print(await check_phone(number))
    
    counter = 1
    start_index = 0
    step = 10000
    requests = []
    start_time = time.time()

    print(f"Парсинг начался")

    for portion in range(start_index, len(domains), step):
        for domain_index in range(start_index, portion):
            requests.append(parse_site(
                domains[domain_index]["real_domain"], counter, start_time))
            counter += 1
        await asyncio.gather(*requests)
        requests.clear()
        start_index = portion

    print(
        f"Парсинг {counter-1} сайтов закончился за {time.time() - start_time}")


def main():
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_DATABASE")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")

    connection = create_connection(db_host, db_user, db_password, db_name)
    domains = select_from_db(connection)  # domains with status = 200

    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_all_sites(domains))


if __name__ == "__main__":
    main()
