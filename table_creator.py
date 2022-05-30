import os
import pymysql


class TableCreator:
    def __init__(self):
        
        self.db_host = os.environ.get("DB_HOST")
        self.db_name = os.environ.get("DB_DATABASE")
        self.db_user = os.environ.get("DB_USER")
        self.db_password = os.environ.get("DB_PASSWORD")

        self.connection = self.__create_connection()


    def create_tables(self):
        # domains TABLE creation
        with open("migrations/domains_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())

        #  categories TABLE creation
        with open("migrations/categories_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # categories VALUES creation
        with open("migrations/categories_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())                
        
        # subcategories TABLE creation
        with open("migrations/subcategories_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # subcategories VALUES creation
        with open("migrations/subcategories_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())   

        # tags TABLE creation
        with open("migrations/tags_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # tags VALUES creation
        with open("migrations/tags_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())  
        
        # domain_info TABLE creation
        with open("migrations/domain_info_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())
        
        # domain_phones TABLE creation
        with open("migrations/domain_phones_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())

        # domain_emails TABLE creation
        with open("migrations/domain_emails_migration.sql", "r", encoding="utf-8") as migration:
            self.__make_db_request(migration.read())

        # regions TABLE creation
        with open("migrations/regions_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())         
            
        # regions VALUES creation
        with open("migrations/region_values_migration.sql", "r", encoding='utf-8') as migration:
            self.__make_db_request(migration.read())    


    def __make_db_request(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result


    def __create_connection(self):
        connection = pymysql.connect(host=self.db_host, user=self.db_user, password=self.db_password, database=self.db_name, cursorclass=pymysql.cursors.DictCursor)
        return connection

def main():
    table_creator = TableCreator()
    table_creator.create_tables()


if __name__ == "__main__":
    main()