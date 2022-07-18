import csv

from tqdm import tqdm
from modules.dbConnector import DbConnector


class InnLoader:
    def __init__(self):
        self.filePath = "data/inns.csv"
        self.db = DbConnector()


    def run(self):
        inns = self.getDataFromFile()
        self.loadInns(inns)


    def loadInns(self, inns):
        print("Переношу в базу...")
        for inn in tqdm(inns, total=24089103):
            self.db.insertIntoInns(inn)
        print(f"Всё. Добавил {len(inns)} записей")

    
    def getDataFromFile(self):
        print("Читаю из файла...")
        inns = set()
        with open(self.filePath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in tqdm(reader):
                inns.add(row["inn"])
        return list(inns)


def main():
    loader = InnLoader()
    loader.run()


if __name__ == "__main__":
    main()
