import asyncio
import csv
from pprint import pprint
import time

import aiohttp

from modules.db_connector import DbConnector
from modules.validator import Validator


class InnInfoParser:
    def __init__(self):
        self.db = DbConnector()
        self.session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=5)
        self.connector = aiohttp.TCPConnector(ssl=False, limit=10000)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=self.session_timeout, trust_env=True)

        self.categories = self.db.make_db_request("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        self.regions = self.db.make_db_request("""
                    SELECT * FROM regions
                """)
        self.validator = Validator(self.categories, self.regions)


    async def parse_inns(self):
        inns_dict = self.db.make_db_request("""
            SELECT inn FROM company_info
        """)

        requests = []

        start = time.time()
        print("Парсинг началася...")
        for i, item in enumerate(inns_dict):
            inn = item["inn"]
            request = self.validator.get_info_by_inn([inn], self.session)
            requests.append(request)
            if i % 1000 == 0: 
                results = await asyncio.gather(*requests)
                requests.clear()
                print(i)
        
        
        print(f"Парсинг закончился за {time.time() - start}")

        for result in results:
            for item in result:
                print(item["boss_name"])
        await self.session.close()

        

    def load_rows(self):
        print("Загрузка началась...")
        start = time.time()
        with open("scripts/INNS.csv", newline="") as file:
            reader = csv.DictReader(file)
            
            inns = set()
            for i, row in enumerate(reader):
                inns.add(row["inn"])
                if i % 100000 == 0: print(i)

            for i, inn in enumerate(inns):
                self.db.make_db_request(f"INSERT INTO company_info (inn) VALUE ({inn})")
                if i % 100000 == 0: print(i)
                
        print(f"Загрузка закончилась за {time.time() - start}")