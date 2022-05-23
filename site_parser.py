import warnings
import os
import time
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


async def parse_site(domain, counter):
    timeout_sec = 10
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
                f"№{counter} -----------------------------------------------------")
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
        # sql = "SELECT real_domain FROM domains WHERE status=200 LIMIT 1000 OFFSET 3000"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


async def identify_site(html):
    bs4 = BeautifulSoup(html, "lxml")
    # TODO: Тут можно сразу скипать сайты если в них есть слова [reg.ru, купить домен, ...]

    valid = await check_valid(html)
    print(f"Valid: {valid}")
    if not valid: return 
    title = bs4.title.contents[0].strip()
    description = await find_description(bs4)
    cms = await identify_cms(html)
    town = await find_city(bs4) 
    

    print(f"Title: {title}")
    print(f"Description: {description}")
    print(f"CMS: {cms}")
    print(f"Город: {town}\n")

    
    # print(f"ИНН: {INN}\n")
    # print(f"Номера телефонов: {mobile_numbers_list}\n") # Искать через <a href="tel +123123123>+12313213</a>"
    # print(f"Список электронных почт: {emails_list}\n") # Пример <a href="mailto:100bumag@100bumag.ru">100bumag@100bumag.ru </a>
    # print(f"Категория: {category}\n")
    # print(f"Подкатегория: {subcategory}\n")
    


async def find_description(bs4):
    description = ''
    meta_tags = bs4.findAll('meta')
    for tag in meta_tags:
        for attribute in tag.attrs:
            if 'description' in tag[attribute]:
                description = tag['content'].strip()
                return description


async def find_city(bs4):
    # Вот такой класс видел "sp-contact-time"
    keywords = ['contact', 'address', 'city', 'town', 'street', 'country', 'location', 'located']
    for keyword in keywords:
        if keyword in bs4:
            return 'SCHEMA_IS_HERE - SCHEMA_IS_HERE - SCHEMA_IS_HERE'




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


async def check_valid(html):
    valid = True
    invalid_keywords = ['reg.ru', 'домен', 'купи домен', 'купить домен', 'приобрести домен', 'приобрети домен', 'получить домен', 'получи домен', 'домен продаётся', 'domain for sale', 'домен продается', 'домен продаётся', 'домен недоступен', 'домен временно недоступен']
    for keyword in invalid_keywords:
        if keyword in html:
            return False
    return valid


async def parse_all_sites(domains):
    counter = 1
    start_index = 0
    step = 10000
    requests = []
    start_time = time.time()
    
    print(f"Парсинг начался")
    
    for portion in range(start_index, len(domains), step):
        for domain_index in range(start_index, portion):
            requests.append(parse_site(domains[domain_index]["real_domain"], counter))
            counter += 1
        await asyncio.gather(*requests)
        start_index = portion
    
    print(f"Парсинг {counter-1} сайтов закончился за {time.time() - start_time}")


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
