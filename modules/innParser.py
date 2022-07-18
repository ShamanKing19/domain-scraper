import asyncio
import json
from pprint import pprint
import time

import aiohttp
import pymysql
from fake_useragent import UserAgent

from modules.dbConnector import DbConnector


class InnParser:
    def __init__(self, inns):
        self.inns = inns
        self.db = DbConnector()
        self.userAgent = UserAgent().random
        self.headers = {"user-agent": self.userAgent}
        self.sessionTimeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=5)
        self.connector = aiohttp.TCPConnector(ssl=False, limit=1000)
        self.session = aiohttp.ClientSession(connector=self.connector, timeout=self.sessionTimeout, trust_env=True, headers=self.headers)


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.parseInns())
            

    async def parseInns(self):
        requests = []

        for item in self.inns:
            id = item["id"]
            inn = item["inn"]
            request = self.getInfoByInn(id, inn, self.session)
            requests.append(request)    
            await asyncio.gather(*requests)
            requests.clear()
        await self.session.close()


    async def getInfoByInn(self, id, inn, session):
        url = f"https://egrul.itsoft.ru/{inn}.json"
        
        try:
            response = await session.get(url)
        except (ConnectionError, aiohttp.ServerTimeoutError)  as error:
            return

        try:
            responseJson = await response.json()
        except aiohttp.ContentTypeError as error:
            return
        
        
        # Address
        address = responseJson.get("СвЮЛ", {}).get("СвАдресЮЛ", {}).get("АдресРФ", {})
        index = address.get("@attributes", {}).get("Индекс", "")
        regionCode = address.get("@attributes", {}).get("КодРегион", "")
        buildingNumber = address.get("@attributes", {}).get("Дом", "")
        buildingFloor = address.get("@attributes", {}).get("Корпус", "")
        buildingRoomNumber = address.get("@attributes", {}).get("Кварт", "")

        regionName = address.get("Регион", {}).get("@attributes", {}).get("НаимРегион", "")
        regionType = address.get("Регион", {}).get("@attributes", {}).get("ТипРегион", "")
        cityName = address.get("Город", {}).get("@attributes", {}).get("НаимГород", "")
        cityType = address.get("Город", {}).get("@attributes", {}).get("ТипГород", "")
        streetName = address.get("Улица", {}).get("@attributes", {}).get("НаимУлица", "")
        streetType = address.get("Улица", {}).get("@attributes", {}).get("ТипУлица", "")

        # Название компании
        companyType = responseJson.get("СвЮЛ", {}).get("@attributes", {}).get("ПолнНаимОПФ", "")
        companyFullName = responseJson.get("СвЮЛ", {}).get("СвНаимЮЛ", {}).get("@attributes", {}).get("НаимЮЛПолн", "").replace('\\"', "").replace("'", "")
        companyShortName = responseJson.get("СвЮЛ", {}).get("СвНаимЮЛ", {}).get("@attributes", {}).get("НаимЮЛСокр", "").replace('\\"', "").replace("'", "")

        # ФИО, ИНН и должность руководителя
        
        officialPerson = responseJson.get("СвЮЛ", {}).get("СведДолжнФЛ", {}) # может быть массивом из нескольких должностных лиц
        if isinstance(officialPerson, list):
            officialPerson = officialPerson[0]
        
        bossLastname = officialPerson.get("СвФЛ", {}).get("@attributes", {}).get("Фамилия", "")
        bossFirstname = officialPerson.get("СвФЛ", {}).get("@attributes", {}).get("Имя", "")
        bossMiddlename = officialPerson.get("СвФЛ", {}).get("@attributes", {}).get("Отчество", "")
        bossInn = officialPerson.get("СвФЛ", {}).get("@attributes", {}).get("ИННФЛ", "")
        bossPostTypeNumber = officialPerson.get("СвДолжн", {}).get("@attributes", {}).get("ВидДолжн", "")
        bossPostTypeName = officialPerson.get("СвДолжн", {}).get("@attributes", {}).get("НаимВидДолжн", "")
        bossPostName = officialPerson.get("СвДолжн", {}).get("@attributes", {}).get("НаимДолжн", "")

        # Дата регистрации
        registrationDate = responseJson.get("СвЮЛ", {}).get("СвОбрЮЛ", {}).get("@attributes", {}).get("ДатаОГРН", "0000-00-00")

        # Уставный капитал
        authorizedCapitalAmount = responseJson.get("СвЮЛ", {}).get("СвУстКап", {}).get("@attributes", {}).get("СумКап", 0)
        authorizedCapitalType = responseJson.get("СвЮЛ", {}).get("СвУстКап", {}).get("@attributes", {}).get("НаимВидКап", "")


        # Учредители
        foundersInfo = []
        founders = responseJson.get("СвЮЛ", {}).get("СвУчредит", {}).get("УчрФЛ", [])
        for founder in founders:
            if not isinstance(founder, dict): continue

            # Имя
            firstName = founder.get("СвФЛ", {}).get("@attributes", {}).get("Имя", "")
            lastName = founder.get("СвФЛ", {}).get("@attributes", {}).get("Фамилия", "")
            middleName = founder.get("СвФЛ", {}).get("@attributes", {}).get("Отчество", "")

            founderFullName = f'{lastName} {firstName} {middleName}'.strip()
            founderInn = founder.get("СвФЛ", {}).get("@attributes", {}).get("ИННФЛ", "")
            
            # Доля (тыс. руб.)
            founderCapitalPartAmount = founder.get("ДоляУстКап", {}).get("@attributes", {}).get("НоминСтоим", 0)
            founderCapitalPartPercent = founder.get("ДоляУстКап", {}).get("РазмерДоли", {}).get("Процент", 0)

            foundersInfo.append(
                {
                    "founder_full_name": founderFullName,
                    "founder_inn": founderInn,
                    "founder_capital_part_amount": founderCapitalPartAmount,
                    "founder_capital_part_percent": founderCapitalPartPercent,
                }
            ) 

        #! Реестр СМСП микропредприятие
        registryCategory = responseJson.get("fin", {}).get("msp", {}).get("@attributes", {}).get("cat", "") # Тут номер 1 это "микропредприятие"
        registryDate = responseJson.get("fin", {}).get("msp", {}).get("@attributes", {}).get("inc_date", "0000-00-00")

        # Сотрудники
        employeesNumber = responseJson.get("fin", {}).get("y2020", {}).get("@attributes", {}).get("n", 0)

        # Финансы в млн. руб. за каждый код
        financeYearsData = []
        lastFinanceYear = 0
        for key in responseJson.get("fin", {}).keys():
            if key[0] == "y":
                income = int(responseJson.get("fin", {}).get(key).get("@attributes", {}).get("income", 0))
                outcome = int(responseJson.get("fin", {}).get(key).get("@attributes", {}).get("outcome", 0))
                data = {
                    "year": key[1:],
                    "income": round(income / 1000000, 2),
                    "outcome": round(outcome / 1000000, 2),
                    "profit": round((income - outcome) / 1000000, 2)
                }
                financeYearsData.append(data)
                lastFinanceYear = int(data.get("year", ""))


        ### Виды деятельности
        # Основной вид деятельности
        mainTypeOfActivesName = responseJson.get("СвЮЛ", {}).get("СвОКВЭД", {}).get("СвОКВЭДОсн", {}).get("@attributes", {}).get("НаимОКВЭД", "")
        mainTypeOfActivesCode = responseJson.get("СвЮЛ", {}).get("СвОКВЭД", {}).get("СвОКВЭДОсн", {}).get("@attributes", {}).get("КодОКВЭД", "")
        mainTypeOfActivesDate = responseJson.get("СвЮЛ", {}).get("СвОКВЭД", {}).get("СвОКВЭДОсн", {}).get("@attributes", {}).get("ПрВерсОКВЭД", "")


        # Дополнительные виды деятельности
        additionalActivities = []
        additionalActivitiesItems = responseJson.get("СвЮЛ", {}).get("СвОКВЭД", {}).get("СвОКВЭДДоп", []) # может оказаться словарём
        if isinstance(additionalActivitiesItems, dict):
            additionalActivitiesItems = [additionalActivitiesItems]
        for activity in additionalActivitiesItems:
            data = {
                "code": activity.get("@attributes", {}).get("КодОКВЭД", {}),
                "name": activity.get("@attributes", {}).get("НаимОКВЭД", {}),
                "date": activity.get("@attributes", {}).get("ПрВерсОКВЭД", {}),
            }
            additionalActivities.append(data)


        street = f"{streetType} {streetName}"
        building = f"{buildingNumber}, {buildingFloor}, {buildingRoomNumber}"
        fullAddress = f"{street}, {building}".strip(" ,") if buildingNumber else street 
        bossFullName = f"{bossLastname} {bossFirstname} {bossMiddlename}"
        
        location = cityName if cityName else regionName
        compName = companyShortName if companyShortName else companyFullName
        requestData = [compName, location, index]
        requestDataString = "+".join(requestData).replace(" ", "%20").replace("\"", "").replace("'", "")
        reviewsYandexMaps = f"https://yandex.ru/maps/?text={requestDataString}"
        reviewsGoogleMaps = f"https://www.google.ru/maps?q={requestDataString}"

        
        # company_finances
        item = ""
        for item in financeYearsData:
            self.db.insertIntoCompanyFinances(id, item['year'], item['income'], item['outcome'], item['profit'])
        segment = self.getSegment(item)


        # company_info
        city = cityName if cityName.strip() else regionName
        self.db.insertIntoCompanyInfo(id, companyFullName, companyType, segment, regionName, city, fullAddress, index, registrationDate, bossFullName, bossPostName, reviewsYandexMaps, reviewsGoogleMaps, authorizedCapitalAmount, registryDate, registryCategory, employeesNumber, mainTypeOfActivesName, lastFinanceYear)
 
        # company_additional_activities
        for item in additionalActivities:
            print(item)
            # self.db.insertIntoAdditionalActivities(inn, item['name'])

        

        # # company_founders
        for item in foundersInfo:
            self.db.insertIntoCompanyFounders(id, founderFullName, founderInn, founderCapitalPartAmount, founderCapitalPartPercent)

            

    def getSegment(self, item):
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


def logJson(filename, content):
    file = open(filename, "w", encoding="utf-8")
    file.write(json.dumps(content, ensure_ascii=False, indent=4))
    file.close()