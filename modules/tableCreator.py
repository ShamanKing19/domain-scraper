try:
    from dbClient import DbClient 
except:
    from modules.dbClient import DbClient

    
class TableCreator:
    def __init__(self):
        self.db = DbClient()


    def createTables(self):
        # domains TABLE creation
        with open("migrations/domains_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())

        #  categories TABLE creation
        with open("migrations/categories_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())         
            
        # categories VALUES creation
        with open("migrations/categories_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())                
        
        # subcategories TABLE creation
        with open("migrations/subcategories_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())         
            
        # subcategories VALUES creation
        with open("migrations/subcategories_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())   

        # tags TABLE creation
        with open("migrations/tags_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())         
            
        # tags VALUES creation
        with open("migrations/tags_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())  
        
        # domain_info TABLE creation
        with open("migrations/domain_info_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())
        
        # domain_phones TABLE creation
        with open("migrations/domain_phones_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())

        # domain_emails TABLE creation
        with open("migrations/domain_emails_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())

        # regions TABLE creation
        with open("migrations/regions_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())         
            
        # regions VALUES creation
        with open("migrations/region_values_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read()) 

        # company_info TABLE creation
        with open("migrations/company_info_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read()) 
        
        # company_finances TABLE creation
        with open("migrations/company_finances_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read()) 
        
        # additional_activities TABLE creation
        with open("migrations/additional_activities_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())
        
        # company_additional_activities TABLE creation
        with open("migrations/company_additional_activities_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())
         
        # company_founders TABLE creation
        with open("migrations/company_founders_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())

        # domain_inns TABLE creation    
        with open("migrations/domain_inns_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())

        # inns TABLE creation    
        with open("migrations/inns_migration.sql", "r", encoding="utf-8") as migration:
            self.db.makeDbRequest(migration.read())


def main():
    table_creator = TableCreator()
    table_creator.createTables()
    print("Таблицы созданы и заполнены!")  


if __name__ == "__main__":
    main()