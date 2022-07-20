import asyncio
import json
import os
import string
import aiohttp
from tqdm import tqdm
from bitrix24 import *


class CrmClient:
    def __init__(self, filepath):
        self.bitrixUrl = self._createBitrixUrl("portal.skillline.ru", 10, "6zjmz9rvzkbr9b8t")
        self.b24 = Bitrix24(self.bitrixUrl)    
        self.session = self._createSession()
        self.filepath = filepath


    async def run(self):
        # В список можно внести другие поля, которые нужно будет изменить
        # В таком случае нужно будет подогнать ключи в функции под прочитанный фа
        await self.updateDealTitlesWithContacts(["title"])
        await self.session.close()


    # dealsList = [{"real_domain": "http://станки-с-чпу.su/", "bitrix_id": 501070}]
    async def updateDealTitlesWithContacts(self, updatedFields = []):
        dealsList = self._getItemsFromFile(self.filepath)
        updateRequests = []

        if isinstance(dealsList, dict):
            dealsList = [dealsList]

        for deal in tqdm(dealsList):
            for fieldKey in updatedFields:
                bitrixID = deal["bitrix_id"]            
                newValue = deal["real_domain"].split(":")[1].strip("/")
                params = {
                    self._getUserFieldCode(fieldKey): newValue 
                }

                updateRequests.append(self.updateDealData(bitrixID, params))
        
        print("Поехали...")
        rawResponses = [await item for item in tqdm(asyncio.as_completed(updateRequests), total=len(updateRequests))]


    async def deleteDeal(self, id) -> aiohttp.ClientResponse:
        method = "crm.deal.delete"
        paramsString = self._getFormattedParamsString(id)
        return await self._makeCRMRequest(method=method + paramsString)

    
    #* oldField должен быть в self.getUserFieldCode(key)
    async def updateDealData(self, dealID, params) -> aiohttp.ClientResponse:
        updateDealRequest = await self.updateDeal(dealID, params)
        dealContactsRequest = await self.getContacts(dealID, params)

        dealResponse = await asyncio.gather(updateDealRequest, dealContactsRequest)
        updateDealResponse = dealResponse[0]
        dealContactsResponse = dealResponse[1]

        dealContactsList = dealContactsResponse.get("result", [])
        
        #! ЭТО КАСТОМ В ТОЛЬКО ДЛЯ ОДНОГО СЛУЧАЯ
        updateContactRequests = []
        for contact in dealContactsList:
            contactID = contact["CONTACT_ID"]
            contactParams = {
                "LAST_NAME": params["TITLE"]
            }
            updateContactRequests.append(self.updateContact(contactID, contactParams))
        updateContactResponses = await asyncio.gather(*updateContactRequests)
        
        return [updateDealResponse, updateContactResponses]


    async def updateContact(self, id, params):
        method = "crm.contact.update"
        paramsString = self._getFormattedParamsString(id, params)
        return await self._makeCRMRequest(method + paramsString)


    async def getContacts(self, id, params):
        method = "crm.deal.contact.items.get"
        paramsString = self._getFormattedParamsString(id, params)
        response = await self._makeCRMRequest(method + paramsString)
        return response


    async def updateDeal(self, id, params):
        method = "crm.deal.update"
        paramsString = self._getFormattedParamsString(id, params)
        return await self._makeCRMRequest(method + paramsString)


    async def getDeals(self, count) -> list[dict]:
        rawDeals = await self._getDealsList(count)
        deals = await self._getDealsProperties(rawDeals)
        return deals
    

    async def getDealById(self, id):
        method = "crm.deal.get"
        paramsString = self._getFormattedParamsString(id)
        return await self._makeCRMRequest(method + paramsString)


    def _getFormattedParamsString(self, id, params={}):
        paramsString = f".json?id={id}&"
        for key, value in params.items():
            paramsString += f"fields[{key}]={value}&"
        return paramsString


    async def _getDealsProperties(self, rawDeals) -> list[dict]:
        requests = []
        for deal in rawDeals:
            id = deal.get("ID", "")
            requests.append(self.getDealById(id))
        rawResponses = [await deal for deal in tqdm(asyncio.as_completed(requests), total=len(requests))]
        responses = [deal["result"] for deal in rawResponses]
        return responses     


    # Сделки без полей, тут нужны только их id
    async def _getDealsList(self, dealsCount) -> list[dict]:
        method = "crm.deal.list"
        deals = []
        next = 0
        
        with tqdm(total=dealsCount/50) as bar:
            while next < dealsCount:
                data = {
                    "start": next,
                }
                responseJson = await self._makeCRMRequest(method, data)
                next = int(responseJson["next"])
                
                deals += responseJson["result"]
                totalDealsCount = responseJson["total"]
    
                bar.update()
        
        return deals[:dealsCount]


    async def _makeCRMRequest(self, method, data={}, params="") -> aiohttp.ClientResponse:
        if data:
            async with self.session.post(self.bitrixUrl + method, data=data) as response:
                return await response.json()
        
        else:
            async with self.session.get(self.bitrixUrl + method, params=params) as response:
                try: 
                    return await response.json()
                except aiohttp.ContentTypeError as e:
                    # print(await response.text())
                    return {}


    def _createSession(self) -> aiohttp.ClientSession:
        sessionTimeout = aiohttp.ClientTimeout(total=None, sock_connect=0, sock_read=0)
        connector = aiohttp.TCPConnector(verify_ssl=True, limit=200)
        session = aiohttp.ClientSession(connector=connector, timeout=sessionTimeout)
        return session

    
    def _createBitrixUrl(self, host, userID, secretKey) -> string:
        return f"https://{host}/rest/{userID}/{secretKey}/"


    def _getUserFieldCode(self, fieldName) -> string:
        userFieldCodes = {
            # default fields
            "id": "ID",
            "title": "TITLE",
            "dealType": "TYPE_ID",
            "categoryID": "CATEGORY_ID",
            "stageID": "STAGE_ID",
            "stageSemanticID": "STAGE_SEMANTIC_ID",
            "isNew": "IS_NEW",
            "isRecurring": "IS_RECURRING",
            "isReturnCustomer": "IS_RETURN_CUSTOMER",
            "isRepeatedApproach": "IS_REPEATED_APPROACH",
            "probability": "PROBABILITY",
            "currencyID": "CURRENCY_ID",
            "opportunity": "OPPORTUNITY",
            "isManualOpportunity": "IS_MANUAL_OPPORTUNITY",
            "taxValue": "TAX_VALUE",
            "companyID": "COMPANY_ID",
            "contactID": "CONTACT_ID",
            "contactIDS": "CONTACT_IDS",
            "quoteID": "QUOTE_ID",
            "assignedByID": "ASSIGNED_BY_ID",
            "createdByID": "CREATED_BY_ID",
            "modifyByID": "MODIFY_BY_ID",
            "leadID": "LEAD_ID",

            # custom fields
            "realDomain": "UF_CRM_1656402341",
            "siteTitle": "UF_CRM_1655668897",
            "siteDescription": "UF_CRM_1655668911",
            "keywords": "UF_CRM_1655668926",
            "category": "UF_CRM_1655194508",
            "region": "UF_CRM_1655194854",
            "cms": "UF_CRM_1655194921",
            "subcategory": "UF_CRM_1655200162",
            "isSsl": "UF_CRM_1655303903",
            "isWWW": "UF_CRM_1655303925",
            "ip": "UF_CRM_1655303954",
            "isHttpsRedirect": "UF_CRM_1655669236",
            "email": "UF_CRM_1655722376",
            "inn": "UF_CRM_1655722387",
            "phone": "UF_CRM_1655723513",
            "domainInfoID": "UF_CRM_1655814173",
            "migrationDate": "UF_CRM_1656327420",
            "segment": "UF_CRM_1656332862",
            "income": "UF_CRM_1656340157", # выручка
            "outcome": "UF_CRM_1656342316", # затраты
            "profit": "UF_CRM_1656342388", # прибыль (double)
            "companyFullName": "UF_CRM_1656513508",
            "companyType": "UF_CRM_1656513708",
            "legalAddress": "UF_CRM_1656513847",
            "postIndex": "UF_CRM_1656513996",
            "registryDate": "UF_CRM_1656514240",
            "bossName": "UF_CRM_1656514500",
            "bossPost": "UF_CRM_1656514646",
            "yandexMapsUrl": "UF_CRM_1656514823",
            "googleMapsUrl": "UF_CRM_1656515194",
            "authorizedCapitalAmount": "UF_CRM_1656515462",
            "registrationDate": "UF_CRM_1656515867",
            "registryType": "UF_CRM_1656516267",
            "employeeCount": "UF_CRM_1656516423",
            "mainActivityType": "UF_CRM_1656516561",
            "lastFinanceYear": "UF_CRM_1656516726",
            "founders": "UF_CRM_1656517374",
            "founderInns": "UF_CRM_1656517545",
            "founderParts": "UF_CRM_1656517752",
            "hosting": "UF_CRM_1656517982",
            "isInn": "UF_CRM_1656532638",
            "isEcommerce": "UF_CRM_1656938541",
            "isCatalog": "UF_CRM_1657794158",
            "hostingCountry": "UF_CRM_1657794396",
        }

        requiredKey = userFieldCodes.get(fieldName, False)

        if not requiredKey:
            raise KeyError(f"Поля {fieldName} не существует")

        return requiredKey


    def _getItemsFromFile(self, filePath):
        with open(filePath, "r", encoding="utf-8") as file:
            deals = json.loads(file.read())
        return deals




def logJson(filename, text):
    file = open("logs/" + filename, "w", encoding="utf-8")
    file.write(json.dumps(text, ensure_ascii=False, indent=4))
    file.close()
    



