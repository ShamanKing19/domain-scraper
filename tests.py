import unittest
from unittest import IsolatedAsyncioTestCase
import asyncio, os, re, pymysql, requests
from bs4 import BeautifulSoup
import phonenumbers
from phonenumbers import geocoder
from phonenumbers import carrier
from phonenumbers import timezone

from validator import Validator


class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.connection = self.__create_connection()
        # Подготовленные данные для парсинга
        self.categories = self.__make_db_request("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        self.regions = self.__make_db_request("""
                    SELECT * FROM regions
                """)
        self.validator = Validator(self.categories, self.regions)


    async def test_find_title(self):
        html = self.get_bs4("https://habr.com/ru/company/otus/blog/433358/")
        result = await self.validator.find_title(html)
        correct_result = "Знакомство с тестированием в Python. Ч.1 / Хабр"
        self.assertEqual(result, correct_result)


    async def test_find_description(self):
        html = self.get_bs4("http://1.shtolfit.ru/")
        result = await self.validator.find_description(html)
        correct_result = 'Вы находитесь на сайте проекта ShtolFitness Проект создан для того, чтобы помочь Вам привести своё тело в порядок, без известных проблем, касающихся похода в фитнес клуб! Вход Регистрация Меня зовут Юлия Штоль Я Ваш фитнес тренер! Мой 12-летний опыт работы в области фитнес индустрии, даёт Мне возможность помочь каждой женщине дойти до желаемого результата с … Читать далее «Главная»'
        self.assertEqual(result, correct_result)


    async def test_find_contacts1(self):
        html = self.get_bs4("http://1z4.ru/")
        result = await self.validator.find_phone_numbers(html)
        correct_result = (["8172022126", "8172022128"])
        self.assertEqual(result, correct_result)


    async def test_find_contacts2(self):
        html = self.get_bs4("http://102doma.com/")
        result = await self.validator.find_phone_numbers(html)
        correct_result = (["83472991269"])
        self.assertEqual(result, correct_result)


    async def test_find_contacts3(self):
        html = self.get_bs4("http://2-bn.ru/")
        result = await self.validator.find_phone_numbers(html)
        correct_result = (["88129889706", "88000000000"])
        self.assertEqual(result, correct_result)


    # без telto:
    async def test_find_contacts4(self):
        html = self.get_bs4("http://1c.ds-t.ru/")
        result = await self.validator.find_phone_numbers(html)
        correct_result = (["79885655660", "78638328118"])
        self.assertEqual(result, correct_result)


    # без telto:
    async def test_find_contacts5(self):
        html = self.get_bs4("http://20futov.ru/")
        result = await self.validator.find_phone_numbers(html)
        correct_result = (["74994040744"])
        self.assertEqual(result, correct_result)


    # Вот тут регуляркой можно достать ИНН
    async def test_find_inn(self):
        html = self.get_bs4("http://20flexplus.ru/")
        result = await self.validator.find_inn(html)
        correct_result = (["7724392813"])
        self.assertEqual(result, correct_result)
    
    
    
    def get_bs4(self, url):
        with requests.get(url) as response:
            return BeautifulSoup(response.text, "lxml")


    def __create_connection(self):
        connection = pymysql.connect(host=os.environ.get("DB_HOST"),
                                     user=os.environ.get("DB_USER"),
                                     password=os.environ.get("DB_PASSWORD"),
                                     database=os.environ.get("DB_DATABASE"),
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection


    def __make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result



if __name__ == "__main__":
    unittest.main()