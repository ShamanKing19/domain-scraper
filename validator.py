import re
from bs4 import BeautifulSoup
import phonenumbers
from phonenumbers import geocoder
from phonenumbers import carrier
from phonenumbers import timezone

class Validator():
    def __init__(self, categories, regions):
        self.categories = categories
        self.subcategories = categories
        self.regions = regions

# TODO: сделать чтобы искало только отдельным словам, а не по вхождениям
    # TODO: То есть сделать регулярку вместо "for tag in title"... 
    async def identify_category(self, title, description):
        # tags = []
        for item in self.categories:
            for tag in item['tag'].split(','):
                if tag in title or tag in description:
                    # print(f'Совпало по ключевому слову: {tag}')
                    return item['id']
                    #Это если надо будет добавлять список ключевых слов от разных подкатегорий
                    # tags.append(tag)
        return 0


    async def identify_city_by_inn(self, inns):
        result_regions = []
        for inn in inns:
            for region in self.regions:
                if int(inn[:2]) == region['id']:
                    result_regions.append(region['region'].strip())
        return result_regions


    async def identify_city_by_number(self, numbers):
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


    async def find_inn(self, bs4):
        # TODO: Отсечь личшние символы вначале и в конце, потому что схватывает рандомные числа из ссылок
        ideal_pattern = re.compile('\b\d{4}\d{6}\d{2}\\b|\\b\d{4}\d{5}\d{1}\\b')
        all_inns = list(set(re.findall(ideal_pattern, bs4.text)))
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
        raw_emails = []
        valid_emails = []

        links = [a for a in bs4.findAll('a', {'href': True}) if 'mailto:' in a.get('href')]
        for a in links:
            if a.text:
                raw_emails.append(a.text)
            else:
                raw_emails.append(a.get('href').split(':')[-1])

        for email in raw_emails:
            matched_email = re.match(r"^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,3})$", email)
            valid_email = matched_email.string
            valid_emails.append(valid_email)

        return list(set(valid_emails))


    # TODO: Поменять регулярку для телефонов. 
    # TODO: Сохранять номера только с валидным количеством цифр (11 или 6)
    # можно искать все классы, в которых будет phone, contacts
    async def find_phone_numbers(self, bs4):
        raw_numbers = []
        valid_numbers = []

        links = [a for a in bs4.findAll('a', {'href': True}) if 'tel:' in a.get('href')]
        for a in links:
            if a.text:
                raw_numbers.append(a.text)
            else:
                raw_numbers.append(a.get('href').split(':')[-1])

        for number in raw_numbers:
            no_symbols_number = re.sub("[^0-9]", "", number)
            matched_number = re.match(r"^((8|\+7)[\- ]?)?(\(?\d{3}\)?[\- ]?)?[\d\- ]{7,10}$", no_symbols_number)
            valid_number = matched_number.string
            # if len(valid_number) == 11 or (valid_number) == 6: valid_numbers.append(valid_number)
            valid_numbers.append(valid_number)

        return list(set(valid_numbers))
        



    async def identify_cms(self, html):
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


    async def is_valid(self, bs4):
        text = bs4
        valid = True
        invalid_keywords = ['reg.ru', 'линковка', 'купить домен', 'домен припаркован', 'только что создан', 'сайт создан', 'сайт в разработке', 'приобрести домен',
                            'получить домен', 'получи домен', 'домен продаётся', 'domain for sale', 'домен продается', 'домен продаётся', 'доступ ограничен',
                            'домен недоступен', 'домен временно недоступен', 'вы владелец сайта?', 'технические работы', 'сайт отключен', 'сайт заблокирован',
                            'сайт недоступен', 'это тестовый сервер', 'это тестовый сайт', 'срок регистрации', 'the site is', '503 service', '404 not found',
                             'fatal error', 'настройте домен', 'under construction',  'не опубликован', 'домен зарегистрирован', 'доступ ограничен', 'welcome to nginx', 
                             'owner of this ', 'Купите короткий домен', 'порно', 'porn', 'sex','секс'
                            ]
        for keyword in invalid_keywords:
            if keyword in text:
                # print(f'Invalid cuz of: {keyword}\n')
                return False
        return valid

    # TODO: Исправить
    # Описание ещё могут засунуть в <meta name="keywords" content"тут писание" ...>
    async def find_description(self, bs4):
        description = ''
        meta_tags = bs4.findAll('meta')
        for meta in meta_tags:
            for attribute in meta.attrs:
                if 'description' in meta[attribute]:
                    # TODO: Тут может вылететь ошибка если не аттрибута 'content' 
                    description = meta['content'].replace('\n', '').replace('"', '').replace("'", '').strip()
                    return description
        return ''


    async def find_title(self, bs4):
        title = ''
        titles = bs4.findAll('title')
        for title in titles:
            return title.get_text().replace('\n', '').replace('"', '').replace("'", '').strip()
        return title