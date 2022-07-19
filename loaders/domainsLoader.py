import csv

from tqdm import tqdm
from modules.dbClient import DbClient


class CSVLoader:
    def __init__(self):
        self.filePath = "data/secondLevelDomains.csv"
        self.domains = []
        self.db = DbClient()


    def run(self):
        self.domains = self.getDataFromFile()
        self.loadDomains()


    def loadDomains(self):
        print("Переношу в базу...")
        for domain in tqdm(self.domains):
            zone = ".".join(domain.split(".")[1:])
            self.db.makeDbRequest(f"""
                INSERT INTO domains (domain, zone) 
                VALUE ('{domain}', '{zone}')
                ON DUPLICATE KEY UPDATE id=id
            """)
        print(f"Всё. Добавил {len(self.domains)} записей")

    
    def getDataFromFile(self):
        print("Читаю из файла...")
        domains = set()
        with open(self.filePath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in tqdm(reader):
                domains.add(row["domain"])
        return list(domains)




def main():
    loader = CSVLoader()
    loader.run()


if __name__ == "__main__":
    main()