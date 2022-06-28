import asyncio
from pprint import pprint
import time

import aiohttp
import pymysql

from modules.db_connector import DbConnector


class InnInfoParser:
    def __init__(self, inns):
        self.db = DbConnector()
        self.session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=5)
        self.connector = aiohttp.TCPConnector(ssl=False, limit=10000)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=self.session_timeout, trust_env=True)
        self.inns = inns

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.parse_inns())
            

    async def parse_inns(self):
        requests = []

        for item in self.inns:
            inn = item["inn"]
            request = self.get_info_by_inn(inn, self.session)
            requests.append(request)    
            await asyncio.gather(*requests)
            requests.clear()
        await self.session.close()


    async def get_info_by_inn(self, inn, session):
        url = f"https://egrul.itsoft.ru/{inn}.json"
        
        try:
            response = await session.get(url)
        except (ConnectionError, aiohttp.ServerTimeoutError)  as error:
            return

        try:
            response_text = await response.json()
        except aiohttp.ContentTypeError as error:
            return
        
        # Address
        try:
            address = response_text["СвЮЛ"]["СвАдресЮЛ"]["АдресРФ"]
            try:
                index = address["@attributes"]["Индекс"]
            except:
                index = ""
            try:
                region_code = address["@attributes"]["КодРегион"]
            except:
                region_code = ""
            try:    
                building_number = address["@attributes"]["Дом"]
            except:
                building_number = ""
            try:    
                building_floor = address["@attributes"]["Корпус"]
            except:
                building_floor = ""
            try:    
                building_room_number = address["@attributes"]["Кварт"]
            except:
                building_room_number = ""
        except Exception as e:
            index = ""
            region_code = ""
            building_number = ""
            building_floor = ""
            building_room_number = ""

        try:
            region_name = address["Регион"]["@attributes"]["НаимРегион"]
        except:
            region_name = ""
        try:
            region_type = address["Регион"]["@attributes"]["ТипРегион"]
        except:
            region_type = ""

        try:
            city_name = address["Город"]["@attributes"]["НаимГород"]
        except:
            city_name = ""
        try:
            city_type = address["Город"]["@attributes"]["ТипГород"]
        except:
            city_type = ""

        try:
            street_name = address["Улица"]["@attributes"]["НаимУлица"]
        except:
            street_name = ""
        try:
            street_type = address["Улица"]["@attributes"]["ТипУлица"]
        except:
            street_type = ""

        # Название компании
        try:
            company_type = response_text["СвЮЛ"]["@attributes"]["ПолнНаимОПФ"]
        except:
            company_type = ""
        try:
            company_full_name = response_text["СвЮЛ"]["СвНаимЮЛ"]["@attributes"]["НаимЮЛПолн"].replace('\\"', "").replace("'", "")
        except:
            company_full_name = ""
        try:
            company_short_name = response_text["СвЮЛ"]["СвНаимЮЛ"]["@attributes"]["НаимЮЛСокр"].replace('\\"', "").replace("'", "")
        except:
            company_short_name = ""

        # ФИО, ИНН и должность руководителя
        try:
            boss_lastname = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвФЛ"]["@attributes"]["Фамилия"]
        except:
            boss_lastname = ""
        try:
            boss_firstname = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвФЛ"]["@attributes"]["Имя"]
        except:
            boss_firstname = ""
        try:
            boss_middlename = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвФЛ"]["@attributes"]["Отчество"]
        except:
            boss_middlename = ""
        try:
            boss_inn = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвФЛ"]["@attributes"]["ИННФЛ"]
        except:
            boss_inn = ""
        try:
            boss_post_type_number = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвДолжн"]["@attributes"]["ВидДолжн"]
        except:
            boss_post_type_number = ""
        try:
            boss_post_type_name = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвДолжн"]["@attributes"]["НаимВидДолжн"]
        except:
            boss_post_type_name = ""
        try:
            boss_post_name = response_text["СвЮЛ"]["СведДолжнФЛ"]["СвДолжн"]["@attributes"]["НаимДолжн"]
        except:
            boss_post_name = ""

        # Дата регистрации
        try: 
            registration_date = response_text["СвЮЛ"]["СвОбрЮЛ"]["@attributes"]["ДатаОГРН"]
        except:
            registration_date = "0000-00-00"

        # Уставный капитал
        try:
            authorized_capital_amount = response_text["СвЮЛ"]["СвУстКап"]["@attributes"]["СумКап"]
        except:
            authorized_capital_amount = 0
        try:
            authorized_capital_type = response_text["СвЮЛ"]["СвУстКап"]["@attributes"]["НаимВидКап"]
        except:
            authorized_capital_type = ""


        # try:
            # Учредители
        founders_info = []
        try:
            founders = response_text["СвЮЛ"]["СвУчредит"]["УчрФЛ"]
            for founder in founders:
                if not isinstance(founder, dict): continue

                # Имя
                try:
                    first_name = founder["СвФЛ"]["@attributes"]["Имя"]
                except:
                    first_name = ""
                try:
                    last_name = founder["СвФЛ"]["@attributes"]["Фамилия"]
                except:
                    last_name = ""
                try:
                    middle_name = founder["СвФЛ"]["@attributes"]["Отчество"]
                except:
                    middle_name = ""
                founder_full_name = f'{last_name} {first_name} {middle_name}' if middle_name else f'{last_name} {first_name}'
                # инн
                founder_inn = founder["СвФЛ"]["@attributes"]["ИННФЛ"]
                
                # Доля (тыс. руб.)
                try:
                    founder_capital_part_amount = founder["ДоляУстКап"]["@attributes"]["НоминСтоим"]
                    try:
                        founder_capital_part_percent = founder["ДоляУстКап"]["РазмерДоли"]["Процент"]
                    except:
                        founder_capital_part_percent = 0
                except:
                    founder_capital_part_amount = 0
                finally:
                    founders_info.append(
                        {
                            "founder_full_name": founder_full_name,
                            "founder_inn": founder_inn,
                            "founder_capital_part_amount": founder_capital_part_amount,
                            "founder_capital_part_percent": founder_capital_part_percent,
                        }
                    ) 
        except Exception as e:
            founders = [] 


        #! Реестр СМСП микропредприятие
        try:
            registry_category = response_text["fin"]["msp"]["@attributes"]["cat"] # Тут номер 1 это "микропредприятие"
        except:
            registry_category = ""
        try:
            registry_date = response_text["fin"]["msp"]["@attributes"]["inc_date"]
        except:
            registry_date = "0000-00-00"
        
        try:
            # Сотрудники
            employees_number = response_text["fin"]["y2020"]["@attributes"]["n"]
        except:
            employees_number = 0

        try:
            # Финансы в млн. руб. за каждый код
            finance_years_data = []
            last_finance_year = 0
            for key in response_text["fin"].keys():
                if key[0] == "y":
                    income = int(response_text["fin"][key]["@attributes"]["income"])
                    outcome = int(response_text["fin"][key]["@attributes"]["outcome"])
                    data = {
                        "year": key[1:],
                        "income": round(income / 1000000, 2),
                        "outcome": round(outcome / 1000000, 2),
                        "profit": round((income - outcome) / 1000000, 2)
                    }
                    finance_years_data.append(data)
                    last_finance_year = int(data["year"])
        except Exception as e:
            pass

        ### Виды деятельности
        # Основной вид деятельности
        try:
            main_type_of_actives_name = response_text["СвЮЛ"]["СвОКВЭД"]["СвОКВЭДОсн"]["@attributes"]["НаимОКВЭД"]
        except:
            main_type_of_actives_name = ""
        try:
            main_type_of_actives_code = response_text["СвЮЛ"]["СвОКВЭД"]["СвОКВЭДОсн"]["@attributes"]["КодОКВЭД"]
        except:
            main_type_of_actives_code = ""
        try:
            main_type_of_actives_date = response_text["СвЮЛ"]["СвОКВЭД"]["СвОКВЭДОсн"]["@attributes"]["ПрВерсОКВЭД"]
        except:
            main_type_of_actives_date = ""

        try:
            # Дополнительные виды деятельности
            additional_activities = []
            for activity in response_text["СвЮЛ"]["СвОКВЭД"]["СвОКВЭДДоп"]:
                data = {
                    "code": activity["@attributes"]["КодОКВЭД"],
                    "name": activity["@attributes"]["НаимОКВЭД"],
                    "date": activity["@attributes"]["ПрВерсОКВЭД"],
                }
                additional_activities.append(data)
        except:
            pass

        street = f"{street_type} {street_name}"
        building = f"{building_number}, {building_floor}, {building_room_number}"
        full_address = f"{street}, {building}".strip(" ,") if building_number else street 
        boss_full_name = f"{boss_lastname} {boss_firstname} {boss_middlename}"
        
        location = city_name if city_name else region_name
        comp_name = company_short_name if company_short_name else company_full_name
        request_data = [comp_name, location, index]
        request_data_string = "+".join(request_data).replace(" ", "%20").replace("\"", "").replace("'", "")
        reviews_yandex_maps = f"https://yandex.ru/maps/?text={request_data_string}"
        reviews_google_maps = f"https://www.google.ru/maps?q={request_data_string}"

        
        # company_finances
        item = ""
        for item in finance_years_data:
            self.db.make_db_request(f"""
                INSERT INTO company_finances (inn, year, income, outcome, profit) 
                VALUE ('{inn}', {item['year']}, {item['income']}, {item['outcome']}, {item['profit']})
                ON DUPLICATE KEY UPDATE inn='{inn}', year={item['year']}, income={item['income']}, outcome={item['outcome']}, profit={item['profit']}
            """)
        segment = self.get_segment(item)


        # company_info
        # TODO: Возможно переделать на UPDATE
        city = city_name if city_name.strip() else region_name
        self.db.make_db_request(f"""
            INSERT INTO company_info (inn, name, type, segment, region, city, address, post_index, registration_date, boss_name, boss_post, yandex_reviews, google_reviews, authorized_capital_type, authorized_capital_amount, registry_date, registry_category, employees_number, main_activity, last_finance_year) 
            VALUE ('{inn}', '{company_full_name}', '{company_type}', '{segment}', '{region_name}', '{city}', '{full_address}', '{index}', '{registration_date}', '{boss_full_name}', '{boss_post_name}', '{reviews_yandex_maps}', '{reviews_google_maps}', '{authorized_capital_type}', '{authorized_capital_amount}', '{registry_date}', '{registry_category}', '{employees_number}', '{main_type_of_actives_name}', '{last_finance_year}')
            ON DUPLICATE KEY UPDATE inn='{inn}', name='{company_full_name}', type='{company_type}', segment='{segment}', region='{region_name}', city='{city}', address='{full_address}', post_index='{index}', registration_date='{registration_date}', boss_name='{boss_full_name}', boss_post='{boss_post_name}', yandex_reviews='{reviews_yandex_maps}', google_reviews='{reviews_google_maps}', authorized_capital_type='{authorized_capital_type}', authorized_capital_amount='{authorized_capital_amount}', registry_date='{registry_date}', registry_category='{registry_category}', employees_number='{employees_number}', main_activity='{main_type_of_actives_name}', last_finance_year={last_finance_year}
        """)
 
        # company_additional_activities
        for item in additional_activities:
            self.db.make_db_request(f"""
                INSERT INTO company_additional_activities (inn, activity_name) 
                VALUE ('{inn}', '{item['name']}')
                ON DUPLICATE KEY UPDATE inn='{inn}', activity_name='{item['name']}'
            """)

        # company_founders
        for item in founders_info:
            try:
                self.db.make_db_request(f"""
                    INSERT INTO company_founders (inn, founder_full_name, founder_inn, founder_capital_part_amount, founder_capital_part_percent) 
                    VALUE ('{inn}', '{item['founder_full_name']}', '{item['founder_inn']}', '{item['founder_capital_part_amount']}', '{item['founder_capital_part_percent']}')
                    ON DUPLICATE KEY UPDATE inn='{inn}', founder_full_name='{item['founder_full_name']}', founder_inn='{item['founder_inn']}', founder_capital_part_amount='{item['founder_capital_part_amount']}', founder_capital_part_percent='{item['founder_capital_part_percent']}'
                """)
            except pymysql.err.ProgrammingError as e:
                file = open("logs.txt", "a", encoding="utf-8")
                file.write(e)
                file.close()
            
    def get_segment(self, item):
        if not item: return None  
        # В милилонах!
        segments = [
            {
                "id": 0,
                "turnover": 0.1
            },
            {
                "id": 1,
                "turnover": 1
            },
            {
                "id": 2,
                "turnover": 3
            },
            {
                "id": 3,
                "turnover": 5
            },
            {
                "id": 4,
                "turnover": 10
            },
            {
                "id": 5,
                "turnover": 20
            },
            {
                "id": 6,
                "turnover": 40
            },
            {
                "id": 7,
                "turnover": 80
            },
            {
                "id": 8,
                "turnover": 160
            },
            {
                "id": 9,
                "turnover": 320
            },
            {
                "id": 10,
                "turnover": 9999999
            },
        ]
        for segment in segments:
            turnover = item["income"] + item["outcome"]
            if turnover <= segment["turnover"]:
                return segment["id"]
