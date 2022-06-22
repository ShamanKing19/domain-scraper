import json
import os
from pprint import pprint
import re
import time
import warnings
import aiohttp
from dotenv import load_dotenv
import phonenumbers
from phonenumbers import geocoder
from phonenumbers import carrier
from phonenumbers import timezone
import copy

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
warnings.filterwarnings("ignore", category=DeprecationWarning)


class Validator():
    def __init__(self, categories, regions):
        self.categories = self.get_categories_with_stripped_tags(categories) 
        self.compiled_tags_categories = self.get_compiled_tags(copy.deepcopy(categories))
        self.compiled_banwords = self.get_compiled_banwords()
        self.compiled_status_banwords = self.get_compiled_status_banwords()
        self.regions = regions
                
       # TODO: Довести до идеала регулярки
        self.re_numbers_template = re.compile(r"\+?[78]{1}[\s\(-]{0,2}[0-9]{3,4}[\s\)]?[\s-]?[0-9-\s]{0,4}[0-9-\s]{0,5}")
        self.re_sub_number_template = re.compile(r"[^0-9]")
        self.re_emails_template = re.compile(r"[a-zA-Z0-9\.\-_]+@[a-zA-Z0-9_\-]+\.[a-zA-Z]+\.?[a-zA-Z]*\.?[a-zA-Z]*")

        self.re_inn_template = re.compile(r"\b\d{4}\d{6}\d{2}\b|\b\d{4}\d{5}\d{1}\b")
        self.re_company_template = re.compile(r"\b[ОПАЗНК]{2,3}\b\s+\b\w+\b")


    # TODO: Удалить после проверки корректности status_banwords
    def get_banwords(self):
        banwords = {
            # без \b...\b
            "title": [
                "timeweb", "срок регистрации", "404", "403", "welcome to nginx", "сайт в разработке", "доменное имя продается", "временно недоступен", "в разработке", "сайт заблокирован", "document", "как вы здесь оказались", "under construction", "домен продается", "домен продаётся", "just a moment", "домен не прилинкован", "for sale", "домен уже", "площадке интернет", "access denied", "витрина домена", "to centos", "доменное имя", "сайт создан",  "купить домен",
                "недоступен", "доступ ограничен", "вы владелец сайта", "отключен", "это тестовый", "продаётся домен", "домен не добавлен", "domain name", "не опубликован", "на технической площадке", "blank page", "припаркован", "website", "данный домен", "loading", "captcha", "домен зарегистрирован", "закрыто", "не работает", "доступ к сайту", "default page", "没有找到站点", "сайт успешно", "ещё один сайт", "который можно купить", "по умолчанию", "на реконстркции", "заглушка для сайта", "index of", "not found",
                "хостинг vps", "файл отсутствует", "report", "без названия", "coming soon",  "error", "домен не настроен", "сайт не запущен", "are not published",
                "порно", "porn", "sex", "секс", "проститутки", "шлюхи", "хентай", "бдсм", "гей", "геи", "увеличить член", 
                "1x", "1х", "казино", "casino", "brazzers", "займ", "букмекер", "действие аккаунта приостановлено", 
                "welcome to adminvps!", "упс! домен не видит хостинг", "запрошенный сайт отсутствует на нашем хостинге", "ukit — сайт не оплачен", "coming soon",
                "pages are not published", "fastpanel", "lptrend | конструктор лендингов", "страница входа", "здоровая росссия", 
                "РќРµ РѕРїСѓР±Р»РёРєРѕРІР°РЅ", "SpaceWeb", "Success!", "To Bet, игровая платформа", "Alle Bikes", "Настройте домен правильно",
            ],
            # с \b...\b
            "description" : [
                "описание сайта", "магазин доменных имен", "ставки", "ставка",  "default index page", "ещё один сайт на wordpress",
                "закрытый форум", 
            ],
            # без \b...\b
            "content" : [
                "reg.ru", "we'll be back soon!", "пусть домен работает", "добро пожаловать в wordpress", "403 forbidden", "эта страница генерируется автоматически",
                "цифирные домены от", "если этот сайт принадлежит вам", "этот домен продается", "account has been suspended", "сайт находится в стадии разработки",
            ]
        }
        return banwords


    # TODO: Удалить после проверки корректности status_banwords
    def get_compiled_banwords(self):
        banwords = self.get_banwords()
        compiled_banwords = {
            "title": [re.compile(fr"{word.lower()}") for word in banwords["title"]], 
            "description": [re.compile(fr"\b{word.lower()}\b") for word in banwords["description"]],
            "content": [re.compile(fr"{word.lower()}") for word in banwords["content"]],
        }
        return compiled_banwords


    def get_status_banwords(self):
        # Ключевые слова для заголовка
        status_banwords_title = [
            # Пустые
            {
                "status": 1000,
                "keywords" : [
                    "404", "403", "welcome to nginx", "blank page",
                    "website", "loading", "not found", "error",
                
                ]   
            },
            # Хостинги
            {
                "status": 1100,
                "keywords" : [
                    "timeweb", "срок регистрации", "доменное имя продается",  "домен продается",
                    "домен продаётся",  "домен не прилинкован", "for sale", "домен уже", "площадке интернет",
                    "витрина домена", "to centos", "доменное имя", "купить домен",
                    "вы владелец сайта", "отключен", "это тестовый", "продаётся домен",
                    "домен не добавлен", "domain name", "не опубликован", "на технической площадке", "припаркован", "данный домен",
                    "домен зарегистрирован", "не работает", "сайт успешно",  "который можно купить",   
                    "хостинг vps", "файл отсутствует", "домен не настроен", "сайт не запущен", "are not published",
                    "действие аккаунта приостановлено", "ukit — сайт не оплачен", 
                    "welcome to adminvps!", "упс! домен не видит хостинг", "запрошенный сайт отсутствует на нашем хостинге", 
                    "pages are not published", "fastpanel", "lptrend | конструктор лендингов",
                    "SpaceWeb", "Success!", "To Bet, игровая платформа", "Alle Bikes", "Настройте домен правильно",
                ]
            },
            # Порно
            {
                "status": 1200,
                "keywords" : [
                    "порно", "porn", "sex", "секс", "проститутки", "шлюхи", "brazzers",
                    "хентай", "бдсм", "гей", "геи", "увеличить член",
                ]
            },

            # Ставки + фишинг
            {
                "status": 1300,
                "keywords" : [
                    "1x", "1х", "казино", "casino", "займ", "букмекер",
                ]
            },

            # Кал
            {
                "status": 1400,
                "keywords" : [
                    "здоровая росссия", "РќРµ РѕРїСѓР±Р»РёРєРѕРІР°РЅ", "в разработке",
                    "сайт заблокирован", "document", "как вы здесь оказались", "under construction", "временно недоступен",
                    "access denied", "сайт создан", "недоступен", "доступ ограничен", "just a moment",
                    "закрыто", "доступ к сайту", "default page", "没有找到站点", "ещё один сайт", "по умолчанию",
                    "на реконстркции", "заглушка для сайта", "index of",
                    "без названия", "coming soon", "report",
                ]
            },       
            # Обработать вручную
            {
                "status": 1500,
                "keywords": [
                    "страница входа", "captcha",
                ]
            }     
        ]

        # Ключевые слова для описания
        status_banwords_description = [
             # Пустые
            {
                "status": 2000,
                "keywords" : [
                    "default index page",
                ]
            },
            # Нерабочие
            {
                "status": 2100,
                "keywords" : [
                    
                ]
            },
            # Продажа доменов
            {
                "status": 2200,
                "keywords" : [
                    "магазин доменных имен",
                ]
            },
            # Ставки + фишинг
            {
                "status": 2300,
                "keywords" : [
                    "ставки", "ставка",
                ]
            },
            # Кал
            {
                "status": 2400,
                "keywords" : [
                    "ещё один сайт на wordpress",
                ]
            },
            #  Обработать вручную
            {
                "status": 2500,
                "keywords" : [
                    "закрытый форум",
                ]
            },
        ]

        # Ключевые слова для контента
        status_banwords_content = [
            # Пустые
            {
                "status": 3000,
                "keywords" : [
                    "we'll be back soon!", "403 forbidden", "эта страница генерируется автоматически",
                    "если этот сайт принадлежит вам", "сайт находится в стадии разработки",
                    "добро пожаловать в wordpress",
                ]
            },
            # Хостинги
            {
                "status": 3100,
                "keywords" : [
                    "reg.ru",
                ]
            },
            # Продажа доменов
            {
                "status": 3100,
                "keywords" : [
                    "пусть домен работает", "цифирные домены от", "этот домен продается", "account has been suspended",
                ]
            },
            # Ставки + фишинг
            {
                "status": 3300,
                "keywords" : [
                    
                ]
            },
             # Кал
            {
                "status": 3400,
                "keywords" : [
                    
                ]
            },
            #  Обработать вручную
            {
                "status": 3500,
                "keywords" : [
                    
                ]
            },
        ]

        banwords = {
            "title": status_banwords_title,
            "description": status_banwords_description,
            "content": status_banwords_content,
        }
        return banwords


    def get_compiled_status_banwords(self):
        status_banwords = self.get_status_banwords()
        compiled_status_banwords = {
            "title": [],
            "description": [],
            "content": [],
        }

        for part in compiled_status_banwords.keys():
            for category in status_banwords[part]:
                if part == "description":
                    keywords = [re.compile(fr"\b{word.lower()}\b") for word in category["keywords"]]   
                else:
                    keywords = [re.compile(f"{word.lower()}") for word in category["keywords"]]

                compiled_status_banwords[part].append({
                    "status": category["status"],
                    "keywords": keywords
                })
        return compiled_status_banwords


    async def check_invalid_status(self, bs4, title, description, id, url):
        search_parts = {
            "title": title,
            "description": description,
            "content": bs4.text,
        }

        for part in self.compiled_status_banwords.keys():
            for category in self.compiled_status_banwords[part]:
                for banword in category["keywords"]:
                    if re.search(banword, search_parts[part].lower()):
                        print(id, category["status"], banword, url, sep=" - ")
                        return category["status"]
        return False


    # TODO: Удалить после проверки корректности status_banwords
    async def is_valid(self, bs4, title, description, id, url):
        if not title:
            return False
        
        for banword in self.compiled_banwords["title"]:
            if re.search(banword, title.lower()):
                # print(f"{id} - invalid cuz of: {banword} - {url}")
                return False

        for banword in self.compiled_banwords["description"]:
            if re.search(banword, description.lower()):
                # print(f"{id} - invalid cuz of: {banword} - {url}")
                return False

        for banword in self.compiled_banwords["content"]:
            if re.search(banword, bs4.text.lower()):
                # print(f"{id} - invalid cuz of: {banword} - {url}")
                return False
            
        return True


    def get_categories_with_stripped_tags(self, categories):
        for subcategory in categories:
            tags = [tag.strip() for tag in subcategory["tag"].split(",")]
            subcategory["tag"] = tags
        return categories


    def get_compiled_tags(self, categories):
        for subcategory in categories:
            tags = []
            # Компиляция тэгов подкатегории
            for tag in subcategory["tag"]:
                tags.append(re.compile(fr"\b{tag}\b"))
            subcategory["tag"] = tags
        return categories


    # ! Мегапрожорливый и медленный, но точный
    async def identify_category(self, bs4, title, description, url):
        rating_dict = {}
        # Установки начального рейтинга
        for subcategory in self.compiled_tags_categories:
            rating_dict[subcategory["id"]] = 0
        
        s = time.time()
        for subcategory in self.compiled_tags_categories:
                for tag in subcategory["tag"]:
                    text = title + description + bs4.text
                    rating_dict[subcategory["id"]] += len(re.findall(tag, text))
        
        if max(rating_dict) == 0:
            return 0
        
        result = max(rating_dict, key=rating_dict.get)  
        print(f"{result} - {time.time() - s}")
        return result


    # TODO: Можно будет удалить и брать данные из запроса по ИНН
    async def identify_city_by_inn(self, inns):
        result_regions = []
        for inn in inns:
            for region in self.regions:
                if int(inn[:2]) == region["id"]:
                    result_regions.append(region["region"].strip())
        return result_regions


    async def identify_city_by_number(self, numbers):
        cities = []
        try:
            for number in numbers:
                valid_number = phonenumbers.parse(number, "RU")
                location = geocoder.description_for_number(valid_number, "ru")
                # operator = carrier.name_for_number(valid_number, "en")
                if location: cities.append(location)
            return list(set(cities))
        except phonenumbers.phonenumberutil.NumberParseException:
            return cities


    # TODO: Возращать нужные данные и сохранять в бд
    async def get_info_by_inn(self, inns, session):
        for inn in inns:
            url = f"https://egrul.itsoft.ru/{inn}.json"
            # print(url)
            response = await session.get(url)
            response_text = await response.json()
            # file = open(f"company_info/{inn}.json", "w", encoding="utf-8")
            # finances = response.json()["fin"] 
            # text = json.dumps(response_text, ensure_ascii=False, indent=4) 
            # file.write(text)
            # file.close()


    # Бесплатные API для проверки организации по ИНН
    # 1). https://htmlweb.ru/service/organization_api.php#api
    # 2). https://egrul.itsoft.ru/{inn}.json или .xml #* Использую его
    async def find_inn(self, bs4):
        all_inns = list(set(re.findall(self.re_inn_template, bs4.text)))
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


    async def find_emails(self, bs4):
        # Поиск по тексту
        raw_emails = re.findall(self.re_emails_template, bs4.text)
        valid_emails = []

        links = [a for a in bs4.findAll("a", {"href": True}) if "mailto:" in a.get("href")]
        for a in links:
            if a.text:
                raw_emails.append(a.text)
            else:
                raw_emails.append(a.get("href").split(":")[-1])

        for email in raw_emails:
            matched_email = re.match(self.re_emails_template, email)
            if matched_email:
                valid_email = matched_email.string
                valid_emails.append(valid_email.strip("\"\'"))

        return list(set(valid_emails))


    async def find_phone_numbers(self, bs4):
        # Поиск номеров в тексте
        raw_numbers = re.findall(self.re_numbers_template, bs4.text)
        valid_numbers = []

        # Поиск номеров в тэгах
        links = [a for a in bs4.findAll("a", {"href": True}) if "tel:" in a.get("href")]
        for a in links:
            if a.text:
                raw_numbers.append(a.text)
            else:
                raw_numbers.append(a.get("href").split(":")[-1])

        for raw_number in raw_numbers:
            no_symbols_number = re.sub(self.re_sub_number_template, "", raw_number)
            if len(no_symbols_number) == 11: 
                if no_symbols_number[0] == "8" or no_symbols_number[0] == "7":
                    valid_numbers.append(no_symbols_number)
        return list(set(valid_numbers))
        

    async def identify_cms(self, html):
        cms_keywords = {
            '/bitrix/js/main': "Bitrix",
            '/wp-content/themes/':  "Wordpress",
            '<meta name="modxru':  "ModX",
            '<script type="text/javascript" src="/netcat':  "Netcat",
            '<script src="/phpshop':  "PhpShop",
            '<script type="text/x-magento-init':  "Magento",
            '/wa-data/public': "Shop-Script",
            'catalog/view/theme':  "OpenCart",
            'data-drupal-':  "Drupal",
            '<meta name="generator" content="Joomla':  "Joomla",
            'var dle_admin': "DataLife Engine",
            'UCOZ-JS': "Ucoz",
            '<script src="https://static.tilda': 'Tilda',
            '<meta name="generator" content="Wix': 'Wix',
            'href="https://nethouse.ru/?p': 'Nethouse', #? Проверить
            'data-muse-uid': 'Adobe Muse',
            'img src="/images/cms/': 'UMI', #? Проверить
            '-= Amiro.CMS (c) =-': 'Amiro',
            'content="CMS EDGESTILE SiteEdit">': 'SiteEdit',
            'meta name="generator" content="OkayCMS': 'Okay'
        }
        for keyword in cms_keywords:
            if keyword in html:
                return cms_keywords[keyword]
        return "" 


    async def find_keywords(self, bs4):
        keywords = bs4.find("meta", {"name": "keywords"})
        if not keywords or "content" not in keywords.attrs.keys(): return ""
        return keywords["content"].replace("\n", "").replace('"', "").replace("'", "").strip()


    async def find_description(self, bs4):
        description = bs4.find("meta", {"name": "description"})
        if not description or "content" not in description.attrs.keys(): return ""
        return description["content"].replace("\n", "").replace('"', "").replace("'", "").strip()
        

    async def find_title(self, bs4):
        title = bs4.find("title")
        if not title: return "" 
        return title.get_text().replace("\n", "").replace('"', "").replace("'", "").strip()
