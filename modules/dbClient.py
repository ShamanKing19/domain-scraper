from datetime import datetime
import os
import time
import pymysql


class DbClient:
    def __init__(self):
        self.dbHost = os.environ.get("DB_HOST", "localhost")
        self.dbName = os.environ.get("DB_DATABASE", "admin_domains")
        self.dbUser = os.environ.get("DB_USER", "admin_domains")
        self.dbPassword = os.environ.get("DB_PASSWORD", "vHVLHeoSrk")
        self.connection = self.createConnection()


    def insertIntoCompanyFounders(self, id, founderFullName, founderInn, founderCapitalPartAmount, founderCapitalPartPercent):
        sql = f"""
            INSERT INTO company_founders (inn_id, founder_full_name, founder_inn, founder_capital_part_amount, founder_capital_part_percent) 
            VALUE ('{id}', '{founderFullName}', '{founderInn}', '{founderCapitalPartAmount}', '{founderCapitalPartPercent}')
            ON DUPLICATE KEY UPDATE inn_id='{id}', founder_full_name='{founderFullName}', founder_inn='{founderInn}', founder_capital_part_amount='{founderCapitalPartAmount}', founder_capital_part_percent='{founderCapitalPartPercent}'
        """
        self.makeDbRequest(sql)


    def insertIntoAdditionalActivities(self, inn, name):
        sql = f"""
            INSERT INTO company_additional_activities (inn, activity_name) 
            VALUE ('{inn}', '{name}')
            ON DUPLICATE KEY UPDATE inn='{inn}', activity_name='{name}'
        """
        self.makeDbRequest(sql)


    # TODO: маппить это
    def insertIntoCompanyInfo(self, innID, companyFullName, companyType, segment, regionName, city, fullAddress, index, registrationDate, bossFullName, bossPostName, reviewsYandexMaps, reviewsGoogleMaps, authorizedCapitalAmount, registryDate, registryCategory, employeesNumber, mainActivity, lastFinanceYear):
        sql = f"""
            INSERT INTO company_info (inn_id, name, type, segment, region, city, address, post_index, registration_date, boss_name, boss_post, yandex_reviews, google_reviews, authorized_capital_amount, registry_date, registry_category, employees_number, main_activity, last_finance_year) 
            VALUE ({innID}, '{companyFullName}', '{companyType}', '{segment}', '{regionName}', '{city}', '{fullAddress}', '{index}', '{registrationDate}', '{bossFullName}', '{bossPostName}', '{reviewsYandexMaps}', '{reviewsGoogleMaps}', '{authorizedCapitalAmount}', '{registryDate}', '{registryCategory}', '{employeesNumber}', '{mainActivity}', '{lastFinanceYear}')
            ON DUPLICATE KEY UPDATE inn_id={innID}, name='{companyFullName}', type='{companyType}', segment='{segment}', region='{regionName}', city='{city}', address='{fullAddress}', post_index='{index}', registration_date='{registrationDate}', boss_name='{bossFullName}', boss_post='{bossPostName}', yandex_reviews='{reviewsYandexMaps}', google_reviews='{reviewsGoogleMaps}', authorized_capital_amount='{authorizedCapitalAmount}', registry_date='{registryDate}', registry_category='{registryCategory}', employees_number='{employeesNumber}', main_activity='{mainActivity}', last_finance_year={lastFinanceYear}
        """
        self.makeDbRequest(sql)


    def insertIntoCompanyFinances(self, innID, year, income, outcome, profit):
        sql = f"""
                INSERT INTO company_finances (inn_id, year, income, outcome, profit) 
                VALUE ('{innID}', {year}, {income}, {outcome}, {profit})
                ON DUPLICATE KEY UPDATE inn_id='{innID}', year={year}, income={income}, outcome={outcome}, profit={profit}
        """
        self.makeDbRequest(sql)


    def getInnsPortion(self, startID, limit):
        sql = f"SELECT * FROM inns WHERE id >= {startID} LIMIT {limit}"
        return self.makeDbRequest(sql)


    def insertIntoInns(self, inn, domainID = None):
        if domainID:
            sql = f"""
                INSERT INTO inns (inn, domain_id) VALUE ({inn}, {domainID})
                ON DUPLICATE KEY UPDATE inn={inn}, domain_id={domainID}    
            """
        else:
            sql = f"""
                INSERT INTO inns (inn) VALUE ({inn})  
            """
        self.makeDbRequest(sql)


    def getLastInnID(self):
        sql = "SELECT id FROM inns ORDER BY id DESC LIMIT 1"
        return self.makeSingleDbRequest(sql)["id"]


    def getFirstInnID(self):
        sql = "SELECT id FROM inns ORDER BY id ASC LIMIT 1"
        return self.makeSingleDbRequest(sql)["id"]


    def insertIntoDomainEmails(self, id, email):
        sql = f"""
            INSERT INTO domain_emails (domain_id, email) 
            VALUE ({id}, '{email}')
            ON DUPLICATE KEY UPDATE email='{email}'
        """
        self.makeDbRequest(sql)


    def insertIntoDomainPhones(self, id, number):
        sql = f"""
            INSERT INTO domain_phones (domain_id, number) 
            VALUE ({id}, {number})
            ON DUPLICATE KEY UPDATE number='{number}'
        """
        self.makeDbRequest(sql)


    # TODO: маппить это
    def insertIntoDomainInfo(self, id, realDomain, title, description, keywords, cities, inn, cms, hosting, www, isSsl, isHttpsRedirect, ip, country, tagId, isEcommerce, hasCatalog, licenseType, lastUpdated):
        rows = ["domain_id", "real_domain", "title", "description", "keywords", "city", "inn", "cms", "hosting", "is_www", "is_ssl", "is_https_redirect", "ip", "country", "tag_id", "is_ecommerce", "has_catalog", "license_type", "last_updated"]
        
        sql = f"""
            INSERT INTO domain_info (domain_id, real_domain, title, description, keywords, city, inn, cms, hosting, is_www, is_ssl, is_https_redirect, ip, country, tag_id, is_ecommerce, has_catalog, license_type, last_updated) 
            VALUE ({id}, '{realDomain}', '{title}', '{description}', '{keywords}', '{cities}', '{inn}', '{cms}', '{hosting}', '{www}', '{isSsl}', '{isHttpsRedirect}', '{ip}', '{country}', {tagId}, {isEcommerce}, '{hasCatalog}', '{licenseType}', '{lastUpdated}')
            ON DUPLICATE KEY UPDATE real_domain='{realDomain}', title='{title}', description='{description}', keywords='{keywords}', city='{cities}', inn='{inn}', cms='{cms}', hosting='{hosting}', is_www='{www}', is_ssl='{isSsl}', is_https_redirect='{isHttpsRedirect}',  ip='{ip}', country='{country}', tag_id={tagId}, is_ecommerce={isEcommerce}, has_catalog={hasCatalog}, license_type='{licenseType}',  last_updated='{lastUpdated}'
        """
        self.makeDbRequest(sql)


    def insertIntoDomains(self, id, realDomain, status, domain="", zone=""):
        sql = f"""
            INSERT INTO domains (id, domain, zone, real_domain, status) 
            VALUE ('{id}', '{domain}', '{zone}', '{realDomain}', {status})
            ON DUPLICATE KEY UPDATE real_domain='{realDomain}', status={status}
        """
        self.makeDbRequest(sql)


    def getRegions(self):
        sql = "SELECT * FROM regions"
        return self.makeDbRequest(sql)


    def getCategories(self):
        sql = """
            SELECT category.name, subcategory.name, tags.tag, tags.id FROM category
            RIGHT JOIN subcategory ON category.id = subcategory.category_id
            INNER JOIN tags ON subcategory.id = tags.id
        """
        return self.makeDbRequest(sql)


    def getDomainsPortion(self, fromID, limit):
        sql = f"SELECT * FROM domains WHERE id >= {fromID} LIMIT {limit}"
        return self.makeDbRequest(sql)


    def getLastDomainID(self):
        sql = "SELECT id FROM domains ORDER BY id DESC LIMIT 1"
        return self.makeSingleDbRequest(sql)["id"]


    def getFirstDomainID(self):
        sql = "SELECT id FROM domains ORDER BY id ASC LIMIT 1"
        return self.makeSingleDbRequest(sql)["id"]


    def getDomainsCount(self):
        sql = "SELECT count(*) FROM domains"
        return self.makeSingleDbRequest(sql)["count(*)"]


    def makeDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.connection.commit()
        return result


    def makeSingleDbRequest(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
        return result


    def createConnection(self):
        connection = pymysql.connect(
            host=self.dbHost,
            user=self.dbUser,
            password=self.dbPassword,
            database=self.dbName,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection


    def log(self, filename, content):
        file = open(filename, "a", encoding="utf-8")
        file.write(content + "\n")
        file.close()
