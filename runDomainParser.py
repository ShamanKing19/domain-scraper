import argparse
from multiprocessing import Process
import os
from pprint import pprint
import time

from dotenv import load_dotenv

from modules.dbConnector import DbConnector
from modules.domainParser import Parser


def putIn(filename, content):
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

    # Настройка аргументов при запуске через консоль
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--offset")
    argParser.add_argument("--cores")
    argParser.add_argument("--portion")
    args = argParser.parse_args()

    domainsCount = DbConnector().makeSingleDbRequest("SELECT count(*) FROM domains")["count(*)"]
    firstId = DbConnector().makeSingleDbRequest("SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]
    lastId = DbConnector().makeSingleDbRequest("SELECT id FROM domains ORDER BY id DESC LIMIT 1")["id"]

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

    while offset < lastId:
        portionStartTime = time.time()
        
        # Парсинг всех сайтов
        domains = DbConnector().makeDbRequest(f"SELECT * FROM domains WHERE id > {offset} LIMIT {step}")
        offset = domains[-1]["id"]

        process = Process(target=runParser, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {offset-(step*3)} по {offset+step} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime}")
            putIn("stats.txt", f"С {offset-(step*3)} по {offset+step} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime}")

    print(f"Парсинг c {startIndex} по {domainsCount} закончился за {time.time() - globalStartTime}")


if __name__ == "__main__":
    main()