from datetime import datetime
import os
import time
import pymysql


class DbConnector:
    def __init__(self):
        self.dbHost = os.environ.get("DB_HOST", "localhost")
        self.dbName = os.environ.get("DB_DATABASE", "admin_domains")
        self.dbUser = os.environ.get("DB_USER", "admin_domains")
        self.dbPassword = os.environ.get("DB_PASSWORD", "vHVLHeoSrk")
        self.connection = self.createConnection()



    def inserIntoCompanyInfo(self, id, inn):
        sql = f"""
            INSERT INTO company_info (domain_id, inn) 
            VALUE ('{id}', '{inn}')
            ON DUPLICATE KEY UPDATE domain_id='{id}', inn='{inn}'
        """
        self.makeDbRequest(sql)


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
        sql = f"SELECT * FROM domains WHERE id > {fromID} LIMIT {limit}"
        return self.makeDbRequest(sql)


    def getLastID(self):
        sql = "SELECT id FROM domains ORDER BY id DESC LIMIT 1"
        return self.makeSingleDbRequest(sql)["id"]


    def getFirstID(self):
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
