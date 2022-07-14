import argparse
from datetime import datetime
from multiprocessing import Process
import os
from pprint import pprint
import time

from dotenv import load_dotenv

from modules.dbConnector import DbConnector
from modules.domainParser import Parser


def log(filename, content):
    file = open(filename, "a", encoding="utf-8")
    file.write(content + "\n")
    file.close()


def loadDotEnv():
    dotenvPath = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenvPath):
        load_dotenv(dotenvPath)


def runParser(portion, offset, domains):
    parser = Parser(domains)
    parser.connectionTimeout = 30
    parser.readTimeout = 60
    parser.run()


def main():
    loadDotEnv()

    # Настройка аргументов при запуске через консоль
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--offset")
    argParser.add_argument("--cores")
    argParser.add_argument("--portion")
    args = argParser.parse_args()

    domainsCount = DbConnector().makeSingleDbRequest(
        "SELECT count(*) FROM domains")["count(*)"]
    firstId = DbConnector().makeSingleDbRequest("SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]
    lastId = DbConnector().makeSingleDbRequest("SELECT id FROM domains ORDER BY id DESC LIMIT 1")["id"]

    # * Начальный индекс для парсинга
    offset = 0
    if args.offset:
        offset = int(args.offset)
    # * Количество процессов парсера
    coresNumber = 2
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

    while offset < lastId:
        # Только для инфы
        portionStartTime = time.time()
        startId = offset

        # Парсинг всех сайтов
        domains = DbConnector().makeDbRequest(f"SELECT * FROM domains WHERE id >= {offset} AND status=408 LIMIT {step}")
        offset = domains[-1]["id"]

        process = Process(target=runParser, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            infoString = f"С {startId - (step*(coresNumber-1))} по {offset} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime}  - {datetime.now()}"
            print(infoString)
            log("stats408.txt", infoString)
            
    print(f"Парсинг c {startIndex} по {domainsCount} закончился за {time.time() - globalStartTime}")


if __name__ == "__main__":
    main()