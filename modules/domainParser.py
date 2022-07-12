import datetime
import http
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
from scripts.tableCreator import TableCreator


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
        self.categories = self.db.makeDbRequest("""
                    SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
                    RIGHT JOIN subcategory ON category.id = subcategory.category_id
                    INNER JOIN tags ON subcategory.id = tags.id
                """)
        # Регионы
        self.regions = self.db.makeDbRequest("""
                    SELECT * FROM regions
                """)
        self.validator = Validator(self.categories, self.regions)


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.parseAllDomains())
        

    async def saveSiteInfo(self, id, domain, zone, response, isSsl, isHttpsRedirect, html):
        realDomain = str(response.real_url.human_repr())
        bs4 = BeautifulSoup(html, "lxml")

        # s = time.time()
        title = await self.validator.findTitle(bs4)  
        description = await self.validator.findDescription(bs4)
        invalidResult =  await self.validator.checkInvalidStatus(bs4, title, description, id, realDomain)
        invalidStatus = invalidResult[0]
        banword = "" if len(invalidResult) == 1 else invalidResult[1]
        cms = await self.validator.identifyCms(html)
        if invalidStatus:
            if cms == "Bitrix":
                invalidStatus = 228
                file = open("bitrixInvalid.txt", "a", encoding="utf-8")
                file.write(f"{realDomain} - {invalidStatus} - {banword}\n")
                file.close()
            self.db.makeDbRequest(f"""
                INSERT INTO {self.statusesTableName} (id, domain, zone, real_domain, status) 
                VALUE ('{id}', '{domain}', '{zone}', '{realDomain}', {invalidStatus})
                ON DUPLICATE KEY UPDATE real_domain='{realDomain}', status={invalidStatus}
            """)
            return
        
        keywords = await self.validator.findKeywords(bs4)
        numbers = await self.validator.findPhoneNumbers(bs4)
        emails = await self.validator.findEmails(bs4)
        inns = await self.validator.findInn(bs4)
        cities = await self.validator.identifyCityByInn(inns) if inns else await self.validator.identifyCityByNumber(numbers)
        # companyInfo = await self.validator.get_info_by_inn(inns, self.http_session)

        tagId = 0

        www = 1 if "www." in realDomain else 0
        try:
            ip = socket.gethostbyname(response.host)
        except:
            ip = ""
        try:
            hosting = whois.whois(realDomain)["registrar"]
        except:
            hosting = ""

        inn = ",".join(inns) if inns else ""
        cities = ",".join(cities) if cities else ""
        lastUpdated = datetime.date.today().strftime('%Y-%m-%d')

        # TODO: Вынести отсюда как-нибудь
        # Проверка e-commerce сайтов
        isEcommerce = 0
        licenseType = ""
        if cms == "Bitrix" and "Авторизация" not in title:
            eshopSections = ["cart", "personal/cart", "shop", "products", "catalog", "basket", "katalog", "korzina"]
            requests = []
            
            for section in eshopSections:
                requests.append(self.httpSession.get(realDomain.strip("/") + "/" + section))
            try:
                responses = await asyncio.gather(*requests)
            except Exception as e:
                responses = []
            
            for response in responses:
                if response.status == 200: 
                    isEcommerce = 1
                    if isEcommerce: break


        # Информация в таблицу domains
        self.db.makeDbRequest(f"""
            INSERT INTO {self.statusesTableName} (id, domain, zone, real_domain, status) 
            VALUE ('{id}', '{domain}', '{zone}', '{realDomain}', {200})
            ON DUPLICATE KEY UPDATE real_domain='{realDomain}', status=200
        """)

        # Информация в таблицу domain_info
        self.db.makeDbRequest(f"""
            INSERT INTO {self.domainInfoTableName} (domain_id, real_domain, title, description, keywords, city, inn, cms, hosting, is_www, is_ssl, is_https_redirect, ip, tag_id, is_ecommerce, license_type, last_updated) 
            VALUE ({id}, '{realDomain}', '{title}', '{description}', '{keywords}', '{cities}', '{inn}', '{cms}', '{hosting}', '{www}', '{isSsl}', '{isHttpsRedirect}', '{ip}', {tagId}, {isEcommerce}, '{licenseType}', '{lastUpdated}')
            ON DUPLICATE KEY UPDATE real_domain='{realDomaiwn}', title='{title}', description='{description}', keywords='{keywords}', city='{cities}', inn='{inn}', cms='{cms}', hosting='{hosting}', is_www='{www}', is_ssl='{isSsl}', is_https_redirect='{isHttpsRedirect}',  ip='{ip}', tag_id={tagId}, is_ecommerce={isEcommerce}, license_type='{licenseType}',  last_updated='{lastUpdated}'
        """)

        # Информация в таблицу domain_phones
        for number in numbers:
            self.db.makeDbRequest(f"""
                INSERT INTO {self.domainPhonesTableName} (domain_id, number) 
                VALUE ({id}, {number})
                ON DUPLICATE KEY UPDATE number='{number}'
            """)

        # Информация в таблицу domain_emails
        for email in emails:
            email = email.strip()
            self.db.makeDbRequest(f"""
                INSERT INTO {self.domainEmailsTableName} (domain_id, email) 
                VALUE ({id}, '{email}')
                ON DUPLICATE KEY UPDATE email='{email}'
            """)

        for inn in inns:
            self.db.makeDbRequest(f"""
                INSERT INTO company_info (domain_id, inn) 
                VALUE ('{id}', '{inn}')
                ON DUPLICATE KEY UPDATE domain_id='{id}', inn='{inn}'
            """)
            

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
        newStatus = 0

        try:
            results = await asyncio.gather(self.httpRequest(domainBaseInfo["domain"]), self.httpsRequest(domainBaseInfo["domain"]), return_exceptions=False)
            httpResponse = results[0]
            httpsResponse = results[1]

            # https redirect check
            if not httpResponse: return
            isHttpsRedirect = 1 if "https://" in httpResponse.real_url.human_repr() else 0
            isSsl = isHttpsRedirect

            # no https redirect check but with ssl
            if httpsResponse: isSsl = 1 if "https://" in httpsResponse.real_url.human_repr() else 0

            if httpResponse.status == 200:
                html = await httpResponse.text()
                await self.saveSiteInfo(id, domain, zone, httpResponse, isSsl, isHttpsRedirect, html)

            #! Не меняю статус если сайт хоть раз отдавал 200
            elif previousStatus == 200:
                pass

            elif httpResponse.status:
                newStatus = httpResponse.status

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

        except aiohttp.client_exceptions.ServerDisconnectedError as error:
            # TODO: Придумать как обойти это
            # Сайт либо заблокироан, либо без ssl сертификата
            pass
        
        # status = 888
        except (UnicodeDecodeError, UnicodeEncodeError,) as error:
            newStatus = 888
        
        #! Для работы на сервере
        except (ssl.CertificateError, ssl.SSLError, http.cookies.CookieError):
            pass
  
        except (pymysql.err.ProgrammingError, pymysql.err.DataError, ValueError) as error:
            # print(error)
            pass


        finally:
            if previousStatus != 200:
                self.db.makeDbRequest(f"""
                    UPDATE {self.statusesTableName}
                    SET status = {newStatus}
                    WHERE id = {id}
                """)
        
    async def parseAllDomains(self):
        requests = []
        startTime = time.time()

        # print(f"\n---------------------------------- Начал обработку запросов ----------------------------------\n")
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
        # print(f"-------- Обработка {self.domains_count} запросов заняла  {time.time() - start_time} секунд --------")


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