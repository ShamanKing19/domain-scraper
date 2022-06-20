import unittest
from unittest import IsolatedAsyncioTestCase
import asyncio
import os
import re
import pymysql
import requests
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
        tests = {
            "https://habr.com/ru/company/otus/blog/433358/": "Знакомство с тестированием в Python. Ч.1 / Хабр",
        }
        for url in tests:
            with self.subTest(url):
                html = self.get_bs4(url)
                result = await self.validator.find_title(html)
                self.assertEqual(sorted(result), sorted(tests[url]))


    async def test_find_description(self):
        tests = {
            "http://1.shtolfit.ru/": 'Вы находитесь на сайте проекта ShtolFitness Проект создан для того, чтобы помочь Вам привести своё тело в порядок, без известных проблем, касающихся похода в фитнес клуб! Вход Регистрация Меня зовут Юлия Штоль Я Ваш фитнес тренер! Мой 12-летний опыт работы в области фитнес индустрии, даёт Мне возможность помочь каждой женщине дойти до желаемого результата с … Читать далее «Главная»'
        }
        for url in tests:
            with self.subTest(url):
                html = self.get_bs4(url)
                result = await self.validator.find_description(html)
                self.assertEqual(sorted(result), sorted(tests[url]))


    async def test_find_phones(self):
        tests = {
            "http://1z4.ru/": ["8172022126", "8172022128"],
            "http://102doma.com/": ["83472991269"],
            "http://2-bn.ru/": ["88129889706", "88000000000"],
            "http://1c.ds-t.ru/": ["78638328118", "79885655660"],
            "http://20futov.ru/": ["74994040744"],
            "https://xn--90aeg8ak.xn--p1ai/": ["74956409390"]
        }
        for url in tests:
            with self.subTest(url):
                html = self.get_bs4(url)
                result = await self.validator.find_phone_numbers(html)
                self.assertListEqual(sorted(result), sorted(tests[url]))


    async def test_find_emails(self):
        tests = {
            "http://102doma.com/": ["info@102doma.com"],
            "http://1c.ds-t.ru/": ["28118@bk.ru"],
            "http://2-begemota.ru/": ["mebel2b@yandex.ru"],
            "http://2-okna.ru/": ["info@2-okna.ru"],
            "http://176.62.67.68:8000/": ["pihtelka@mail.ru"]
        }
        for url in tests:
            with self.subTest(url):
                html = self.get_bs4(url)
                result = await self.validator.find_emails(html)
                self.assertListEqual(sorted(result), sorted(tests[url]))


    async def test_find_inn(self):
        tests = {
            "http://2-buh.ru/": ["7810593760"],
            "http://203-03-03.ru/": ["2464233672"],
            # "http://2050at.ru": [], # ! Вот тут определяет какое-то число, ИНН тут нет
            "http://2080105.ru": ["2320230737"],
            "http://1.shtolfit.ru/": [],
        }
        for url in tests:
            with self.subTest(url):
                html = self.get_bs4(url)
                result = await self.validator.find_inn(html)
                self.assertListEqual(sorted(result), sorted(tests[url]))


    async def test_identify_tag(self):
        tests = {
            "http://1profnastil.ru/": 32,
            "http://www.blamperfo.ru/": 0,
            "http://cbunalog.ru": 60,
            "http://3cpt.ru": 61,

        }
        for url in tests:
            with self.subTest(url):
                bs4 = self.get_bs4(url)
                result = await self.validator.identify_category(await self.validator.find_title(bs4), await self.validator.find_title(bs4))
                self.assertEqual(result, tests[url])


    def get_bs4(self, url):
        with requests.get(url, headers=self.get_headers()) as response:
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


    def get_headers(self):
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


if __name__ == "__main__":
    unittest.main()
