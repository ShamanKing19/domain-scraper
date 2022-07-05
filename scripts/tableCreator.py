import os
import time
import pymysql

try:
    from dbConnector import DbConnector
except:
    from scripts.dbConnector import DbConnector

class TableCreator:
    def __init__(self):
        self.db = DbConnector()


    def CreateTables(self):
        # domains TABLE creation
        with open("migrations/domains_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())

        #  categories TABLE creation
        with open("migrations/categories_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())         
            
        # categories VALUES creation
        with open("migrations/categories_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())                
        
        # subcategories TABLE creation
        with open("migrations/subcategories_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())         
            
        # subcategories VALUES creation
        with open("migrations/subcategories_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())   

        # tags TABLE creation
        with open("migrations/tags_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())         
            
        # tags VALUES creation
        with open("migrations/tags_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())  
        
        # domain_info TABLE creation
        with open("migrations/domain_info_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())
        
        # domain_phones TABLE creation
        with open("migrations/domain_phones_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())

        # domain_emails TABLE creation
        with open("migrations/domain_emails_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())

        # regions TABLE creation
        with open("migrations/regions_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())         
            
        # regions VALUES creation
        with open("migrations/region_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read()) 

        # company_info TABLE creation
        with open("migrations/company_info_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read()) 
        
        # company_finances TABLE creation
        with open("migrations/company_finances_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read()) 
        
        # additional_activities_migration TABLE creation
        with open("migrations/additional_activities_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())
        
        # company_additional_activities_migration TABLE creation
        with open("migrations/company_additional_activities_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())
         
        # company_founders_migration TABLE creation
        with open("migrations/company_founders_migration.sql", "r", encoding="utf-8") as migration:
            self.db.MakeDbRequest(migration.read())



def main():
    table_creator = TableCreator()
    table_creator.CreateTables()
    print("Таблицы созданы и заполнены!")  


if __name__ == "__main__":
    main()