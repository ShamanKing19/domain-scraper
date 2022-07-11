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

dotenvPath = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenvPath):
    load_dotenv(dotenvPath)
warnings.filterwarnings("ignore", category=DeprecationWarning)


class Validator():
    def __init__(self, categories, regions):
        self.categories = self.getCategoriesWithStrippedTags(categories) 
        self.compiledTagsCategories = self.getCompiledTags(copy.deepcopy(categories))
        self.compiledStatusBanwords = self.getCompiledStatusBanwords()
        self.regions = regions
                
       # TODO: Довести до идеала регулярки
        self.reNumbersTemplate = re.compile(r"\+?[78]{1}[\s\(-]{0,2}[0-9]{3,4}[\s\)]?[\s-]?[0-9-\s]{0,4}[0-9-\s]{0,5}")
        self.reSubNumberTemplate = re.compile(r"[^0-9]")
        self.reEmailsTemplate = re.compile(r"[a-zA-Z0-9\.\-_]+@[a-zA-Z0-9_\-]+\.[a-zA-Z]+\.?[a-zA-Z]*\.?[a-zA-Z]*")

        self.reInnTemplate = re.compile(r"\b\d{4}\d{6}\d{2}\b|\b\d{4}\d{5}\d{1}\b")
        self.reCompanyTemplate = re.compile(r"\b[ОПАЗНК]{2,3}\b\s+\b\w+\b")


    # TODO: Переставить слова местами для оптимизации
    def getStatusBanwords(self):
        # Ключевые слова для заголовка
        statusBanwordsTitle = [
            {
                "name": "Empty",
                "status": 1000,
                "keywords" : [
                    "404", "403", "welcome to nginx", "blank page", "coming soon",
                    "loading", "not found", "error", "To Bet, игровая платформа",
                    "в разработке", "under construction", "没有找到站点", "доступ заблокирован"
                ]   
            },
            {
                "name": "Hosting",
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
                    "pages are not published", "fastpanel", "FASTPANEL2", "lptrend | конструктор лендингов",
                    "SpaceWeb", "Success!",  "Alle Bikes", "Настройте домен правильно",
                    "сайт заблокирован", "сайт создан", "купить красивый домен",
                ]
            },
            {
                "name": "Porn",
                "status": 1200,
                "keywords" : [
                    "порно", "porn", "sex", "секс", "проститутки", "шлюхи", "brazzers",
                    "хентай", "бдсм", "геи", "увеличить член",
                ]
            },
            {
                "name": "Bets + fishing",
                "status": 1300,
                "keywords" : [
                    "1x", "1х", "казино", "casino", "займ", "букмекер", "Займы до 500 000"
                ]
            },

            # ! ПРИДУМАТЬ КУДА ДЕТЬ
            {
                "name": "ПРИДУМАААААААТЬ",
                "status": 1400,
                "keywords" : [
                    "здоровая росссия", "РќРµ РѕРїСѓР±Р»РёРєРѕРІР°РЅ",
                    "как вы здесь оказались", "временно недоступен",
                    "недоступен", "доступ ограничен", 
                    "закрыто", "доступ к сайту", "default page",  "ещё один сайт", 
                    "на реконстркции", "заглушка для сайта", "index of",
                    "без названия", "report",
                ]
            },       
            {
                "name": "Process anually",
                "status": 1500,
                "keywords": [
                    "страница входа", "captcha", "just a moment",
                ]
            },     
            {
                "name": "РАНДОМ. ПРОВЕРИТЬ. БЫВАЕТ ПОРНО)))",
                "status": 1600,
                "keywords": [
                    "access denied", "website",
                ]
            },   
            {
                "name": "Domains shop",
                "status": 1700,
                "keywords": [
                    "Domenomania",
                ]
            }     
        ]

        # Ключевые слова для описания
        statusBanwordsDescription = [
            {
                "name": "Empty",
                "status": 2000,
                "keywords" : [
                    "default index page",
                ]
            },
            {
                "name": "Hosting",
                "status": 2100,
                "keywords" : [
                    
                ]
            },
            {
                "name": "Porn",
                "status": 2200,
                "keywords" : [
                    "порно", "porn", "sex", "секс", "проститутки", "шлюхи", "brazzers",
                    "хентай", "бдсм", "геи", "увеличить член",
                ]
            },
            {
                "name": "Bets + fishing",
                "status": 2300,
                "keywords" : [
                    "ставки", "ставка",
                ]
            },
            {
                "name": "ПРИДУМАААААААТЬ",
                "status": 2400,
                "keywords" : [
                    
                ]
            },
            {
                "name": "Manually",
                "status": 2500,
                "keywords" : [
                    "закрытый форум",
                ]
            },
            {
                "name": "РАНДОМ. ПРОВЕРИТЬ!",
                "status": 2600,
                "keywords" : [
                    "ещё один сайт на wordpress"
                ]
            },
                        {
                "name": "Domains shop",
                "status": 2700,
                "keywords" : [
                    "магазин доменных имен",
                ]
            },
        ]

        # Ключевые слова для контента
        statusBanwordsContent = [
            {
                "name": "Empty",
                "status": 3000,
                "keywords" : [
                    "we'll be back soon!", "403 forbidden", "эта страница генерируется автоматически",
                    "если этот сайт принадлежит вам", "сайт находится в стадии разработки",
                ]
            },
            {
                "name": "Hosting",
                "status": 3100,
                "keywords" : [
                    "reg.ru", "ISPsystem © 1997-",
                ]
            },
            {
                "name": "Domains shop",
                "status": 3200,
                "keywords" : [
                    "пусть домен работает", "цифирные домены от", "этот домен продается", "account has been suspended",
                ]
            },
            {
                "name": "Bets + fishing",
                "status": 3300,
                "keywords" : [
                    
                ]
            },
            {
                "name": "ПРИДУМАААААТЬ",
                "status": 3400,
                "keywords" : [
                    
                ]
            },
            {
                "name": "Manually",
                "status": 3500,
                "keywords" : [
                    
                ]
            },
        ]

        banwords = {
            "title": statusBanwordsTitle,
            "description": statusBanwordsDescription,
            "content": statusBanwordsContent,
        }
        return banwords


    def getCompiledStatusBanwords(self):
        statusBanwords = self.getStatusBanwords()
        
        additionalTitleRegularExpressions = [
            {
                    "name": "Domains shop",
                    "status": 1700,
                    "keywords": [r"\b[Дд]омен\b[a-zA-Zа-яА-Я0-9\.\-\s]*\bпрода[её]тся\b"]
            }
        ]

        compiledStatusBanwords = {
            "title": [*additionalTitleRegularExpressions],
            "description": [],
            "content": [],
        }
        
        for part in compiledStatusBanwords.keys():
            for category in statusBanwords[part]:
                keywords = [re.compile(fr"\b{word.lower()}\b") for word in category["keywords"]]
                compiledStatusBanwords[part].append({
                    "name": category["name"],
                    "status": category["status"],
                    "keywords": keywords
                })
        return compiledStatusBanwords


    async def checkInvalidStatus(self, bs4, title, description, id, url):
        searchParts = {
            "title": title,
            "description": description,
            "content": bs4.text,
        }

        for part in self.compiledStatusBanwords.keys():
            for category in self.compiledStatusBanwords[part]:
                for banword in category["keywords"]:
                    if re.search(banword, searchParts[part].lower()):
                        # print(id, category["name"], category["status"], banword, url, sep=" - ")
                        return [category["status"], banword]
        return [False]


    def getCategoriesWithStrippedTags(self, categories):
        for subcategory in categories:
            tags = [tag.strip() for tag in subcategory["tag"].split(",")]
            subcategory["tag"] = tags
        return categories


    def getCompiledTags(self, categories):
        for subcategory in categories:
            tags = []
            # Компиляция тэгов подкатегории
            for tag in subcategory["tag"]:
                tags.append(re.compile(fr"\b{tag}\b"))
            subcategory["tag"] = tags
        return categories


    # ! Мегапрожорливый и медленный, но точный
    async def identifyCategory(self, bs4, title, description, url):
        ratingDict = {}
        # Установки начального рейтинга
        for subcategory in self.compiledTagsCategories:
            ratingDict[subcategory["id"]] = 0
        
        s = time.time()
        for subcategory in self.compiledTagsCategories:
            for tag in subcategory["tag"]:
                text = title + description + bs4.text
                ratingDict[subcategory["id"]] += len(re.findall(tag, text))
        
        if max(ratingDict) == 0:
            return 0
        
        result = max(ratingDict, key=ratingDict.get)  
        print(f"{result} - {time.time() - s}")
        return result


    # TODO: Можно будет удалить и брать данные из запроса по ИНН
    async def identifyCityByInn(self, inns):
        resultRegions = []
        for inn in inns:
            for region in self.regions:
                if int(inn[:2]) == region["id"]:
                    resultRegions.append(region["region"].strip())
        return resultRegions


    async def identifyCityByNumber(self, numbers):
        cities = []
        try:
            for number in numbers:
                validNumber = phonenumbers.parse(number, "RU")
                location = geocoder.description_for_number(validNumber, "ru")
                # operator = carrier.name_for_number(valid_number, "en")
                if location: cities.append(location)
            return list(set(cities))
        except phonenumbers.phonenumberutil.NumberParseException:
            return cities


    # Бесплатные API для проверки организации по ИНН
    # 1). https://htmlweb.ru/service/organization_api.php#api
    # 2). https://egrul.itsoft.ru/{inn}.json или .xml #* Использую его
    async def findInn(self, bs4):
        allInns = list(set(re.findall(self.reInnTemplate, bs4.text)))
        correctInns = []
        coefficients_10 = [2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
        coefficients_12_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0, 0]
        coefficients_12_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
        
        # Тут должны быть формулы проверки корректности ИНН
        for inn in allInns:
            if len(inn) == 10:
                key = 0
                # Расчёт ключа по коэффициентам
                for i, digit in enumerate(inn, start=0):
                    key += int(digit) * coefficients_10[i]
                # key mod 11
                key_value = key - (key // 11) * 11
                if key_value == int(inn[-1]):
                    correctInns.append(inn)

            elif len(inn) == 12:
                key1 = 0
                key2 = 0
                # Расчёт ключа №1 по коэффициентам
                for i, digit in enumerate(inn, start=0):
                    key1 += int(digit) * coefficients_12_1[i]
                # key mod 11 №1
                keyValue_1 = key1 - (key1 // 11) * 11
                # Расчёт ключа №2 по коэффициентам
                for i, digit in enumerate(inn, start=0):
                    key2 += int(digit) * coefficients_12_2[i]
                keyValue_2 = key2 - (key2 // 11) * 11
                # key mod 11 №2
                if keyValue_1 == int(inn[10]) and keyValue_2 == int(inn[11]):
                    correctInns.append(inn)
        if len(correctInns) > 0:
            return correctInns
        else:
            return []


    async def findEmails(self, bs4):
        # Поиск по тексту
        rawEmails = re.findall(self.reEmailsTemplate, bs4.text)
        validEmails = []

        links = [a for a in bs4.findAll("a", {"href": True}) if "mailto:" in a.get("href")]
        for a in links:
            if a.text:
                rawEmails.append(a.text)
            else:
                rawEmails.append(a.get("href").split(":")[-1])

        for email in rawEmails:
            matchedEmail = re.match(self.reEmailsTemplate, email)
            if matchedEmail:
                validEmail = matchedEmail.string
                validEmails.append(validEmail.strip("\"\'.,"))
        return list(set(validEmails))


    async def findPhoneNumbers(self, bs4):
        # Поиск номеров в тексте
        rawNumbers = re.findall(self.reNumbersTemplate, bs4.text)
        validNumbers = []

        # Поиск номеров в тэгах
        links = [a for a in bs4.findAll("a", {"href": True}) if "tel:" in a.get("href")]
        for a in links:
            if a.text:
                rawNumbers.append(a.text)
            else:
                rawNumbers.append(a.get("href").split(":")[-1])

        for rawNumber in rawNumbers:
            noSymbolsNumber = re.sub(self.reSubNumberTemplate, "", rawNumber)
            if len(noSymbolsNumber) == 11: 
                if noSymbolsNumber[0] == "8" or noSymbolsNumber[0] == "7":
                    validNumbers.append(noSymbolsNumber)
        return list(set(validNumbers))
        

    async def identifyCms(self, html):
        cmsKeywords = {
            'src="/bitrix/': "Bitrix",
            '<link href="/bitrix':"Bitrix",
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
            'href="https://nethouse.ru/?p': 'Nethouse', # TODO: Проверить
            'data-muse-uid': 'Adobe Muse',
            'img src="/images/cms/': 'UMI', # TODO: Проверить
            '-= Amiro.CMS (c) =-': 'Amiro',
            'content="CMS EDGESTILE SiteEdit">': 'SiteEdit',
            'meta name="generator" content="OkayCMS': 'Okay'
        }
        for keyword in cmsKeywords:
            if keyword in html:
                return cmsKeywords[keyword]
        return "" 


    async def findKeywords(self, bs4):
        keywords = bs4.find("meta", {"name": "keywords"})
        if not keywords or "content" not in keywords.attrs.keys(): return ""
        return keywords["content"].replace("\n", "").replace('"', "").replace("'", "").strip()


    async def findDescription(self, bs4):
        description = bs4.find("meta", {"name": "description"})
        if not description or "content" not in description.attrs.keys(): return ""
        return description["content"].replace("\n", "").replace('"', "").replace("'", "").strip()
        

    async def findTitle(self, bs4):
        title = bs4.find("title")
        if not title: return "" 
        return title.get_text().replace("\n", "").replace('"', "").replace("'", "").strip()
