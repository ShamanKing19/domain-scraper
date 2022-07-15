import datetime
import http
import json
from pprint import pprint
import warnings

warnings.filterwarnings("ignore")
import ssl
ssl.match_hostname = lambda cert, hostname: True

import os
import time
import zipfile
import socket
import aiohttp
import asyncio
import whois
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.request import urlretrieve
from genericpath import exists

import pymysql

from modules.dbConnector import DbConnector
from modules.validator import Validator
from modules.tableCreator import TableCreator


class Parser:
    def __init__(self, domains):
        self.statusesTableName = "domains"
        self.domainInfoTableName = "domain_info"
        self.domainPhonesTableName = "domain_phones"
        self.domainEmailsTableName = "domain_emails"

        self.db = DbConnector()
        # self.connection = self.db.create_connection()

        self.isTableExists = True

        ### Параметры парсера ###
        self.connectionTimeout = 5
        self.readTimeout = 30
        self.everyPrintable = 10000

        sessionTimeout = aiohttp.ClientTimeout(total=None, sock_connect=self.connectionTimeout, sock_read=self.readTimeout)
        httpsConnector = aiohttp.TCPConnector(verify_ssl=True, limit=10000)
        httpConnector = aiohttp.TCPConnector(verify_ssl=False, limit=10000)
        self.httpsSession = aiohttp.ClientSession(connector=httpsConnector, timeout=sessionTimeout)
        self.httpSession = aiohttp.ClientSession(connector=httpConnector, timeout=sessionTimeout, trust_env=True)

        
        # self.geoApi = "https://ipinfo.io/{{ip}}/json" #* Тут лимит 50к запросов в месяц, но определяет что угодно. САМАЯ ТОЧНАЯ
        # self.geoApi = "https://geolocation-db.com/jsonp/{{ip}}" #* не очень апишка, много ошибок
        # self.geoApi = "https://api.hostip.info/get_html.php?ip={{ip}}&position=true" #* Тоже не очень, мало находит
        self.geoApi = "https://ipgeolocation.abstractapi.com/v1/?api_key=f4b61e710fd846c48350bd35faacfc51&ip_address={{ip}}"

        # Получение списка доменов и создание таблиц
        # request_time = time.time()
        if self.isTableExists:
            self.domains = domains
        # TODO: Сделать чтобы в случае чтения с файла он тоже брал инфу порциями
        else:
            table_creator = TableCreator()
            table_creator.createTables()
            self.downloadRuDomainsFileIfNotExists()
            self.domains = self.getRowsFromTxt()
            self.domainsCount = len(self.domains)

        ### Подготовленные данные для парсинга
        # Категории
        self.categories = self.db.getCategories()
        
        # Регионы
        self.regions = self.db.getRegions()

        self.validator = Validator(self.regions)
        # self.validator = Validator(self.categories, self.regions)


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.parseAllDomains())
        

    async def saveSiteInfo(self, id, domain, zone, response, isSsl, isHttpsRedirect, html):
        try:
            realDomain = str(response.real_url.human_repr())
        except Exception as e:
            realDomain = "error"
            self.log("logs/realDomainErrorLogs.txt", f"url: {domain}, error: {e}")

        bs4 = BeautifulSoup(html, "lxml")

        title = await self.validator.findTitle(bs4)  
        description = await self.validator.findDescription(bs4)
        invalidResult =  await self.validator.checkInvalidStatus(bs4, title, description, id, realDomain)
        invalidStatus = invalidResult["status"]
        banword = "" if not invalidResult["banword"] else invalidResult["banword"]
        cms = await self.validator.identifyCms(html)
        if invalidStatus:
            if cms == "Bitrix":
                invalidStatus = 228
                self.log("logs/invalidBitrix.txt", f"{realDomain} - {invalidStatus} - {banword}")
            self.db.insertIntoDomains(id=id, domain=domain, zone=zone, realDomain=realDomain, status=invalidStatus)
            return
        
        keywords = await self.validator.findKeywords(bs4)
        numbers = await self.validator.findPhoneNumbers(bs4)
        emails = await self.validator.findEmails(bs4)
        inns = await self.validator.findInn(bs4)
        cities = await self.validator.identifyCityByInn(inns) if inns else await self.validator.identifyCityByNumber(numbers)
        
        # Дополнительный поиск контактов
        if not emails:
            contactsSections = ["contacts", "contact", "about", "aboutus"]
            contactsRequests = []
            
            for section in contactsSections:
                contactsRequests.append(self.httpSession.get(realDomain.strip("/") + "/" + section))
            try:
                contactsResponses = await asyncio.gather(*contactsRequests)
            except Exception as e:
                contactsResponses = []
            
            for response in contactsResponses:
                if response.status == 200: 
                    contactBS4 = BeautifulSoup(await response.text())
                    newEmails = await self.validator.findEmails(contactBS4)
                    newNumbers = await self.validator.findPhoneNumbers(contactBS4)
                    newInns = await self.validator.findInn(contactBS4)
                    emails += newEmails
                    numbers += newNumbers
                    inns += newInns
            emails = list(set(emails))
            numbers = list(set(numbers))
            inns = list(set(inns))

        tagId = 0
        inn = ",".join(inns) if inns else ""
        cities = ",".join(cities) if cities else ""
        lastUpdated = datetime.date.today().strftime('%Y-%m-%d')
        www = 1 if "www." in realDomain else 0

        try:
            ip = socket.gethostbyname(response.host)
        except:
            ip = ""
        
        try:
            country = ""
            if ip:
                pass
                # ipInfoResponse = await self.httpSession.get(self.geoApi.replace("{{ip}}", ip))
                # ipInfo = await ipInfoResponse.json()
                # pprint(ipInfo)
                # ipInfoFormatted = ipInfo.replace("callback", "").strip("()")
                # ipInfoJson = json.loads(ipInfoFormatted)
                # country = ipInfoJson.get("country_code", "")
        except Exception as e:
            print(f"Общая ошибка (Exception): {e}")
            

        try:
            hosting = whois.whois(realDomain)["registrar"]
        except:
            hosting = ""
        

        # TODO: Вынести отсюда как-нибудь или объединить с другими запросами
        # Проверка e-commerce сайтов
        isEcommerce = 0
        hasCatalog = 0
        hasCart = 0
        licenseType = ""
        if cms == "Bitrix" and "Авторизация" not in title and "auth" not in realDomain:
            catalogSections = ["catalog", "products", "shop", "katalog"]
            cartSections = ["cart", "personal/cart", "basket", "korzina"]
            
            # Поиск каталога
            catalogRequests = []
            for section in catalogSections:
                catalogRequests.append(self.httpSession.get(realDomain.strip("/") + "/" + section))
            
            # Поиск корзины
            cartRequests = []
            for section in cartSections:
                cartRequests.append(self.httpSession.get(realDomain.strip("/") + "/" + section))
            
            try:
                cartResponses = await asyncio.gather(*cartRequests)
            except:
                cartResponses = []
            
            for response in cartResponses:
                if response.status == 200: 
                    hasCart = 1
                    if hasCart:
                        hasCatalog = 1
                        break
            
            if not hasCart:
                try:
                    catalogResponses = await asyncio.gather(*catalogRequests)
                except:
                    catalogResponses = []
                
                for response in catalogResponses:
                    if response.status == 200: 
                        hasCatalog = 1
                        if hasCatalog: break        
        isEcommerce = hasCart

        # Информация в таблицу domains
        self.db.insertIntoDomains(id=id,
            domain=domain,
            zone=zone,
            realDomain=realDomain,
            status=200
        )

        # Информация в таблицу domain_info
        self.db.insertIntoDomainInfo(
            id=id,
            realDomain=realDomain,
            title=title,
            description=description,
            keywords=keywords,
            cities=cities,
            inn=inn,
            cms=cms,
            hosting=hosting,
            www=www,
            isSsl=isSsl,
            isHttpsRedirect=isHttpsRedirect,
            ip=ip,
            country=country,
            tagId=tagId,
            isEcommerce=isEcommerce,
            hasCatalog=hasCatalog,
            licenseType=licenseType,
            lastUpdated=lastUpdated
        )

        # Информация в таблицу domain_phones
        for number in numbers:
            self.db.insertIntoDomainPhones(id=id, number=number)

        # Информация в таблицу domain_emails
        for email in emails:
            email = email.strip()
            self.db.insertIntoDomainEmails(id=id, email=email)

        for inn in inns:
            self.db.inserIntoCompanyInfo(id=id, inn=inn)
            

    async def httpRequest(self, domain):
        httpUrl = "http://" + domain
        response = await self.httpSession.get(httpUrl, headers=self.getHeaders())
        return response


    async def httpsRequest(self, domain):
        try:
            httpsUrl = "https://" + domain
            response = await self.httpsSession.get(httpsUrl, headers=self.getHeaders())
            return response
        
        except (ssl.CertificateError, aiohttp.client_exceptions.ClientConnectorCertificateError):
            return False

  
    async def makeDomainRequest(self, domainBaseInfo):
        id = domainBaseInfo["id"]
        domain = domainBaseInfo["domain"]
        zone = domainBaseInfo.get("zone", "ru")
        previousStatus = domainBaseInfo["previousStatus"]
        newStatus = 200

        try:
            results = await asyncio.gather(self.httpRequest(domainBaseInfo["domain"]), self.httpsRequest(domainBaseInfo["domain"]), return_exceptions=False)
            httpResponse = results[0]
            httpsResponse = results[1]

            # https redirect check
            isHttpsRedirect = 1 if "https://" in httpResponse.real_url.human_repr() else 0
            isSsl = isHttpsRedirect

            # no https redirect check but with ssl
            if httpsResponse: 
                isSsl = 1 if "https://" in httpsResponse.real_url.human_repr() else 0

            if httpResponse.status == 200:
                html = await httpResponse.text()
                await self.saveSiteInfo(id, domain, zone, httpResponse, isSsl, isHttpsRedirect, html)

            #* Не меняю статус если сайт хоть раз отдавал 200
            elif previousStatus == 200:
                pass

            elif httpResponse.status:
                newStatus = httpResponse.status

        #! Обработка ошибок http и https запросов
        except (
            aiohttp.client_exceptions.ClientConnectorError,
            aiohttp.client_exceptions.ClientOSError,
            ConnectionResetError,
        ) as error:
            newStatus = 404

        except (
            aiohttp.client_exceptions.ClientPayloadError,
            aiohttp.client_exceptions.ClientResponseError
        ) as error:
            newStatus = 400

        # status TimeoutError = 408 status
        except aiohttp.client_exceptions.ServerTimeoutError as error:
            newStatus = 408

        # TODO: Придумать как обойти это
        # Сайт либо заблокироан, либо без ssl сертификата
        except aiohttp.client_exceptions.ServerDisconnectedError as error:
            newStatus = 800
            self.log("logs/ServerDisconnectedError.txt", f"{domain} - {error}")
        
        except (UnicodeDecodeError, UnicodeEncodeError,) as error:
            newStatus = 888
        
        #! Для работы на сервере
        except (ssl.CertificateError, ssl.SSLError, http.cookies.CookieError):
            self.log("logs/SSLError", f"{domain} - {error}")
  
        except (pymysql.err.DataError, ValueError) as error:
            self.log("logs/pymysql.err.DataError", f"{domain} - {error}")
        
        except pymysql.Error as error:
            print(f"pymysql.Error: {error}")

        finally:
            if previousStatus != 200 and newStatus != 200:
                self.db.insertIntoDomains(id=id, realDomain="", status=newStatus)
        
        
    async def parseAllDomains(self):
        requests = []
        startTime = time.time()

        for domain in self.domains:
            domainBaseInfo = {
                "domain": domain["domain"],
                "id": domain["id"],
                "zone": domain.get("zone", "ru"),
                "start_time": startTime,
                "previousStatus": domain.get("status", 0)
            }
            requests.append(self.makeDomainRequest(domainBaseInfo))
            
        # TODO: Разбить на 4 части и запустить 4 процесса
        await asyncio.gather(*requests)
        await self.httpSession.close()
        await self.httpsSession.close()
        requests.clear()


    def downloadRuDomainsFileIfNotExists(self):
        downloadLink = "https://statonline.ru/domainlist/file?tld="
        archivesPath = "archives"
        ruArchivePath = "archives/ru.zip"
        extractedFilesPath = "archives/extracted"
        filePath = "archives/extracted/ru_domains.txt"
        
        if (exists(filePath)):
            return
        if not (exists(archivesPath)):
            os.mkdir(archivesPath)
        if not (exists(extractedFilesPath)):
            os.mkdir(extractedFilesPath)
        start_time = time.time()
        print("Началась загрузка архива с доменами...")
        urlretrieve(downloadLink, ruArchivePath)
        print(f"Файл загружен за {time.time() - start_time}")

        print("Начата распаковка файла")
        with zipfile.ZipFile(ruArchivePath, "r") as zip_file:
            zip_file.extractall(extractedFilesPath)
        print("Файл распакован")
        os.remove(ruArchivePath)
        print("Архив удалён")


    def getRowsFromTxt(self):
        domains = []
        startTime = time.time()
        counter = 0
        with open(self.filePath, "r") as file:
            for row in file:
                row = row.split(";")
                domains.append({
                    "id": counter,
                    "domain": row[0],
                    "zone": row[1].split("-")[-1],
                })
                counter += 1
        print(f"Файл прочитан за {time.time() - startTime}")
        return domains


    def getHeaders(self):
        userAgents = {
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
            "User-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"
        }
        return userAgents


    def log(self, filename, content):
        file = open(filename, "a", encoding="utf-8")
        file.write(content + "\n")
        file.close()