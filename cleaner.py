import asyncio
import datetime
from multiprocessing import Process
import time

from bs4 import BeautifulSoup
from modules.dbClient import DbClient
from modules.validator import Validator


class Cleaner:
    def __init__(self, domains):
        self.db = DbClient()
        self.regions = self.db.makeDbRequest("SELECT * FROM regions")
        self.validator = Validator(self.regions)
        
        self.domains = domains
        

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.clean())


    async def clean(self):
        deletedRows = 0
        for domain in self.domains:
            id = domain["id"]
            domainId = domain["domain_id"]
            realDomain = domain["real_domain"]
            title = domain["title"]
            description = domain["description"]

            validateResult = await self.validator.checkInvalidStatus(BeautifulSoup(""), title, description, id, realDomain)
            
            if not validateResult["status"]: 
                continue
            
            self.db.makeDbRequest(f"""
                DELETE FROM domain_info
                WHERE id={id}
            """)

            deletedRows += 1
            log("logs/deletedRows.txt", f"id: {id}, domain_id: {domainId}, real_domain: {realDomain}, title: {title[:100]}, description: {description[:100]}")
        print(f"Удалено {deletedRows} записей")






def log(filename, content):
    file = open(filename, "a", encoding="utf-8")
    file.write(content + "\n")
    file.close()


def runCleaner(step, offset, domains):
    cleaner = Cleaner(domains)
    cleaner.run()


if __name__ == "__main__":
    db = DbClient()

    domainsCount = DbClient().makeSingleDbRequest("SELECT count(*) FROM domain_info")["count(*)"]
    firstId = DbClient().makeSingleDbRequest("SELECT id FROM domain_info ORDER BY id ASC LIMIT 1")["id"]
    lastId = DbClient().makeSingleDbRequest("SELECT id FROM domain_info ORDER BY id DESC LIMIT 1")["id"]

    offset = 0
    portion = 10000
    coresNumber = 1

    startIndex = firstId + offset
    step = portion

    processes = []
    globalStartTime = time.time()

    while offset < lastId:
        # Информация для вывода промежутка парсинга
        startId = offset
        portionStartTime = time.time()

        # Выборка
        domains = db.makeDbRequest(f"""
            SELECT id, domain_id, real_domain, title, description
            FROM domain_info
            WHERE id > {offset}
            LIMIT {step}
        """)
        offset = domains[-1]["id"]

        process = Process(target=runCleaner, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            infoString = f"С {startId - (step*(coresNumber-1))} по {offset} за {time.time() - portionStartTime} - Общее время чистки: {time.time() - globalStartTime} - {datetime.datetime.now()}"
            print(infoString)
            log("logs/cleanStats.txt", infoString)


        
        

