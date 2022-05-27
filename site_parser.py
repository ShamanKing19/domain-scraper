import warnings
import os
import time
import re

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
try:
    import phonenumbers
    from phonenumbers import geocoder
    from phonenumbers import carrier
    from phonenumbers import timezone
except:
    os.system("pip install phonenumbers")


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
warnings.filterwarnings("ignore", category=DeprecationWarning)


class Site_parser:
    def __init__(self, db_host, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.timeout_sec = 30
        self.step = 10000

        self.connection = self.__create_connection()

        self.statuses_table_name = 'domains'
        self.domain_info_table_name = 'domain_info'
        self.domain_phones_table_name = 'domain_phones'
        self.domain_emails_table_name = 'domain_emails'


        # Начальное создание страниц
        self.__create_tables()
        
        # Получение инфы для парсинга
        self.domains = self.__make_db_request("SELECT id, real_domain FROM domains WHERE status=200 GROUP BY real_domain")
        self.categories = self.__make_db_request("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        self.regions = self.__make_db_request("""
                    SELECT * FROM regions
                """)

    
    def parse(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__parse_all_sites())


    def __create_tables(self):
        # categories TABLE creation
        with open("migrations/categories_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # categories VALUES creation
        with open("migrations/categories_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())                
        
        # subcategories TABLE creation
        with open("migrations/subcategories_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # subcategories VALUES creation
        with open("migrations/subcategories_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())   

        # tags TABLE creation
        with open("migrations/tags_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # tags VALUES creation
        with open("migrations/tags_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())  
        
        # domain_info TABLE creation
        with open("migrations/domain_info_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())
        
        # domain_phones TABLE creation
        with open("migrations/domain_phones_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())

        # domain_emails TABLE creation
        with open("migrations/domain_emails_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())

        # regions TABLE creation
        with open("migrations/regions_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # regions VALUES creation
        with open("migrations/region_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())    



    def __create_connection(self):
        connection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password,
                                     database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        return connection


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


    def __make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result


    def __make_db_insert(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        self.connection.commit()


    async def __parse_all_sites(self):
        start_index = 0
        requests = []
        start_time = time.time()
        domains_count = len(self.domains)
        domain_index = 0

        print(f"Получено {domains_count} уникальных доменов, шаг = {self.step}")
        
        # Для маленького количества записей
        if self.step > domains_count:
             self.step = domains_count-1

        # TODO: Починить. Прогоняется всего 10к записей (в первом парсере будет такая же ошибка под конец парсинга)        
        for portion in range(start_index, domains_count+self.step, self.step):
            for domain_index in range(start_index, portion):
                if domain_index == domains_count: break
                requests.append(self.__parse_site(self.domains[domain_index]["real_domain"], self.domains[domain_index]["id"], domain_index+1, start_time))
            
            await asyncio.gather(*requests)
            start_index = portion
            requests.clear()

            # Фикс последнего шага
            if domain_index + self.step >= domains_count: self.step = domains_count - domain_index - 1
        
        print(f"Парсинг {domain_index+1} сайтов закончился за {time.time() - start_time}")



    async def __parse_site(self, domain, domain_id, counter, start_time):
        session_timeout = aiohttp.ClientTimeout(
            total=None, sock_connect=self.timeout_sec, sock_read=self.timeout_sec)
        session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False), timeout=session_timeout)
        try:
            async with session.get(domain, headers=self.__get_headers()) as response:
                url = response.url
                html = await response.text() # TODO: Возможно ошибка кодировки здесь и надо использовать content
                # print(f"№{counter} - {time.time() - start_time} - URL: {url}")
                await self.__identify_site(html, domain_id)
                counter += 1
        # Даже к сайтам со статусом 200 парсер может не подключиться, поэтому тут обработка
        except Exception as error:
            print(f"№{counter} - {time.time() - start_time} - URL: {domain}")
            print(error, end="\n")
        finally:
            await session.close()


    async def __identify_site(self, html, domain_id):
        # TODO: Исправить проблему с кодировкой (нужно сделать кодировку utf8 для html)
        bs4 = BeautifulSoup(html, "lxml")

        valid = await self.__check_valid(html)
        if not valid:
            return

        title = await self.__find_title(bs4)  # Возврат: string
        description = await self.__find_description(bs4)  # Возврат: string
        cms = await self.__identify_cms(html)  # Возврат: string
        numbers, emails = await self.__find_contacts(bs4) # Возврат: {'mobile_numbers': [], 'emails:': []}
        inns = await self.__find_inn(bs4.text)  # Возврат: ['ИНН1', 'ИНН2', ...]
        tag_id = await self.__identify_category(title, description) # Возврат: id из таблицы tags
        cities_via_number = await self.__identify_city_by_number(numbers) # Возврат: ['Город1', 'Город2', ...]
        cities_via_inn = await self.__identify_city_by_inn(inns) # Возврат: ['Москва', 'Калининградская область', 'Архангельская область'...]

        # Тут можно попробовать убрать проверку, может быть оно не сломается
        if len(cities_via_inn) > 0:
            city = ",".join(cities_via_inn)
        elif len(cities_via_number) > 0:
            city = ",".join(cities_via_number)
        else:
            city = ""
        inn = ",".join(inns)

        # Информация в таблицу domain_info
        self.__make_db_insert(f"""
            INSERT IGNORE INTO {self.domain_info_table_name} (id, domain_id, title, description, city, inn, cms, tag_id) 
            VALUE ({domain_id}, {domain_id}, '{title}', '{description}', '{city}', '{inn}', '{cms}', {tag_id})
        """)

        # Информация в таблицу domain_phones
        for number in numbers:
            self.__make_db_insert(f"""
                INSERT IGNORE INTO {self.domain_phones_table_name} (id, domain_id, number) 
                VALUE ({domain_id}, {domain_id}, {number})
            """)

        # Информация в таблицу domain_emails
        for email in emails:
            email = email.strip()
            self.__make_db_insert(f"""
                INSERT IGNORE INTO {self.domain_emails_table_name} (id, domain_id, email) 
                VALUE ({domain_id}, {domain_id}, '{email}')
            """)
        
        # print(f"Title: {title}")
        # print(f"Description: {description}")
        # print(f"CMS: {cms}")
        # print(f"Номера: {numbers}")
        # print(f"Почты: {emails}")
        # print(f"ИНН: {inns}\n")
        # print(f"Категория: {category}")
        # print(f"Города через номер: {cities_via_number}")
        # print(f"Города через ИНН: {cities_via_inn}\n")
        

    async def __identify_category(self, title, description):
        # tags = []
        for item in self.categories:
            for tag in item['tag'].split(','):
                if tag in title or tag in description:
                    # print(f'Совпало по ключевому слову: {tag}')
                    return item['id']
                    #Это если надо будет добавлять список ключевых слов от разных подкатегорий
                    # tags.append(tag)
        return 0


    async def __identify_city_by_inn(self, inns):
        result_regions = []
        for inn in inns:
            for region in self.regions:
                if int(inn[:2]) == region['id']:
                    result_regions.append(region['region'].strip())
        return result_regions


    async def __identify_city_by_number(self, numbers):
        cities = []
        # TODO: Проверить ошибки
        try:
            for number in numbers:
                valid_number = phonenumbers.parse(number, "RU")
                location = geocoder.description_for_number(valid_number, "ru")
                operator = carrier.name_for_number(valid_number, "en")
                cities.append(location)
        except:
            print(numbers)
        finally:
            return list(set(cities))


    async def __find_inn(self, text):
        # TODO: Отсечь личшние символы вначале и в конце, потому что схватывает рандомные числа из ссылок
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
        if len(correct_inns) > 0:
            return correct_inns
        else:
            return []


    # TODO: Проверить скорость с более точным поиском
    # TODO: Исправить. Может выдать в такую строку ['74955427679brbr79169573046']
    # Такое тоже выдало ['79313526492', 'Email', '88313355101', '79200520290', '79253136201']
    # ['882002010120000020000']
    # можно искать все классы, в которых будет phone, contacts
    async def __find_contacts(self, bs4):
        links = bs4.findAll('a')
        mobile_numbers = []
        emails = []
        for a in links:
            for attribute in a.attrs:
                if attribute == 'href':
                    if 'tel:' in a[attribute]:
                        mobile_number = a[attribute].split(':')[1].strip().replace("%20", "")
                        number = re.sub("[^A-Za-z0-9]", "", mobile_number)
                        if len(number) > 0: mobile_numbers.append(number)
                    elif 'mailto:' in a[attribute]:
                        email = a[attribute].split(':')[-1].strip()
                        if len(email) > 0: emails.append(email)
        # Так удаляю дубликаты
        return list(set(mobile_numbers)),  list(set(emails))


    async def __identify_cms(self, html):
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
        return "" 


    async def __check_valid(self, bs4):
        text = bs4
        valid = True
        invalid_keywords = ['reg.ru', 'линковка', 'купить домен', 'домен припаркован', 'только что создан', 'сайт создан', 'сайт в разработке', 'приобрести домен',
                            'получить домен', 'получи домен', 'домен продаётся', 'domain for sale', 'домен продается', 'домен продаётся',
                            'домен недоступен', 'домен временно недоступен', 'вы владелец сайта?', 'технические работы', 'сайт отключен', 'сайт заблокирован',
                            'сайт недоступен', 'это тестовый сервер', 'это тестовый сайт', 'срок регистрации', 'the site is',
                            '503 service', '404 not found', 'fatal error', 'настройте домен', 'under construction',  'не опубликован',
                            'домен зарегистрирован', 'доступ ограничен', 'welcome to nginx', 'owner of this ', 'Купите короткий домен',
                            ]
        for keyword in invalid_keywords:
            if keyword in text:
                # print(f'Invalid cuz of: {keyword}\n')
                return False
        return valid

    # TODO: Исправить
    # Описание ещё могут засунуть в <meta name="keywords" content"тут писание" ...>
    async def __find_description(self, bs4):
        description = ''
        meta_tags = bs4.findAll('meta')
        for meta in meta_tags:
            for attribute in meta.attrs:
                if 'description' in meta[attribute]:
                    # TODO: Тут может вылететь ошибка если не аттрибута 'content' 
                    description = meta['content'].replace('\n', '').replace('"', '').replace("'", '').strip()
                    return description
        return ''


    async def __find_title(self, bs4):
        title = ''
        titles = bs4.findAll('title')
        for title in titles:
            return title.get_text().replace('\n', '').replace('"', '').replace("'", '').strip()
        return title


if __name__ == "__main__":
    db_host = os.environ.get("DB_HOST")
    db_name = os.environ.get("DB_DATABASE")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")

    parser = Site_parser(db_host, db_name, db_user, db_password)
    parser.parse()