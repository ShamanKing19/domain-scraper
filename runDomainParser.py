import argparse
from multiprocessing import Process
import os
from pprint import pprint
import time

from dotenv import load_dotenv
from tqdm import tqdm

from modules.dbConnector import DbConnector
from modules.domainParser import Parser


def LoadDotEnv():
    dotenvPath = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenvPath):
        load_dotenv(dotenvPath)


def RunParser(portion, offset, domains):
    parser = Parser(domains)
    parser.Run()


def main():
    LoadDotEnv()

    # Настройка аргументов при запуске через консоль
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--offset")
    argParser.add_argument("--cores")
    argParser.add_argument("--portion")
    args = argParser.parse_args()

    domainsCount = DbConnector().MakeSingleDbRequest(
        "SELECT count(*) FROM domains")["count(*)"]
    firstId = DbConnector().MakeSingleDbRequest("SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]

    # * Начальный индекс для парсинга
    offset = 0
    if args.offset:
        offset = int(args.offset)
    # * Количество процессов парсера
    coresNumber = 4
    if args.cores:
        coresNumber = int(args.cores)
    # * Одновременно обрабатываемая порция
    portion = 10000
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

    for offset in tqdm(range(startIndex, domainsCount + startIndex, step)):
        portionStartTime = time.time()
        
        # Парсинг всех сайтов
        domains = DbConnector().MakeDbRequest(f"SELECT * FROM domains WHERE id >= {offset} LIMIT {step}")

        process = Process(target=RunParser, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {offset-(step*3)} по {offset+step} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime}")

    print(f"Парсинг c {startIndex} по {domainsCount} закончился за {time.time() - globalStartTime}")


if __name__ == "__main__":
    main()