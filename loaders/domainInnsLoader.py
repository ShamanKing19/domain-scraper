import csv
import json
from pprint import pprint

from tqdm import tqdm
from modules.dbClient import DbClient

class DomainInnsLoader:
    def __init__(self):
        self.db = DbClient()
        self.filename = "inns.json"


    def run(self):
        self.loadInns()
        # pprint(inns)


    def loadInns(self):
        rows = self.getDataFromFile()
        for row in tqdm(rows):
            self.db.insertIntoDomainInns(row["inn"], row["domain_id"])


    def getDataFromFile(self):
        file = open(self.filename, "r", encoding="utf-8")
        rows = json.loads(file.read())
        file.close()
        return rows


    def createInnsFile(self):
        inns = self.db.getInnsWithDomains()
        file = open("inns.json", "w", encoding="utf-8")
        file.write(json.dumps(inns, ensure_ascii=False, indent=4))
        file.close()


if __name__ == "__main__":
    loader = DomainInnsLoader()
    loader.run()