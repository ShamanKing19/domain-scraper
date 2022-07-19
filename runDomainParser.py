import argparse
from datetime import datetime
from multiprocessing import Process
import os
from pprint import pprint
import time

from dotenv import load_dotenv

from modules.dbClient import DbClient
from modules.domainParser import Parser


def log(filename, content):
    if not os.path.exists("logs"): os.mkdir("logs")
    file = open(filename, "a", encoding="utf-8")
    file.write(content + "\n")
    file.close()


def loadDotEnv():
    dotenvPath = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenvPath):
        load_dotenv(dotenvPath)


def runParser(portion, offset, domains):
    parser = Parser(domains)
    parser.run()


def main():
    loadDotEnv()
    db = DbClient()

    # Настройка аргументов при запуске через консоль
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--offset")
    argParser.add_argument("--cores")
    argParser.add_argument("--portion")
    args = argParser.parse_args()

    domainsCount = db.getDomainsCount()
    firstId = db.getFirstDomainID()
    lastId = db.getLastDomainID()

    # * Начальный индекс для парсинга
    offset = 0
    if args.offset:
        offset = int(args.offset)
    # * Количество процессов парсера
    coresNumber = 4
    if args.cores:
        coresNumber = int(args.cores)
    # * Одновременно обрабатываемая порция
    portion = 1000
    if args.portion:
        portion = int(args.portion)

    startIndex = firstId + offset

    globalStartTime = time.time()
    processes = []

    step = portion // coresNumber + portion % coresNumber

    # Для небольшого количества записей
    if step > domainsCount:
        step = domainsCount
        coresNumber = 1

    while startIndex < lastId:
        # Только для вывода
        infoStartID = startIndex
        portionStartTime = time.time()

        # Парсинг всех сайтов
        try:
            domains = db.getDomainsPortion(fromID=startIndex, limit=step)
            startIndex = domains[-1]["id"]
        except Exception as e:
            domains = []
            startIndex = infoStartID
            log("requestError.txt", e)

        process = Process(target=runParser, args=(step, startIndex, domains))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            infoString = f"С {infoStartID - (step*(coresNumber-1))} по {startIndex} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime} - {datetime.now()}"
            log("logs/stats200.txt", infoString)

    print(f"Парсинг c {startIndex} по {domainsCount} закончился за {time.time() - globalStartTime}")


if __name__ == "__main__":
    main()