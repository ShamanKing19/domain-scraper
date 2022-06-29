import argparse
from multiprocessing import Process
import os
from pprint import pprint
import time

from dotenv import load_dotenv

from modules.db_connector import DbConnector
from modules.parser import Parser


def load_dot_env():
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)


def run_parser(portion, offset, domains):
    # logging.basicConfig(filename="logs.log", encoding="utf-8")
    parser = Parser(domains)
    parser.run()


def main():
    load_dot_env()

    # Настройка аргументов при запуске через консоль
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--offset")
    arg_parser.add_argument("--cores")
    arg_parser.add_argument("--portion")
    args = arg_parser.parse_args()

    domains_count = DbConnector().make_single_db_request(
        "SELECT count(*) FROM domains")["count(*)"]
    first_id = DbConnector().make_single_db_request(
        "SELECT id FROM domains ORDER BY id ASC LIMIT 1")["id"]

    # * Начальный индекс для парсинга
    offset = 0
    if args.offset:
        offset = int(args.offset)
    # * Количество процессов парсера
    cores_number = 4
    if args.cores:
        cores_number = int(args.cores)
    # * Одновременно обрабатываемая порция
    portion = 10000
    if args.portion:
        portion = int(args.portion)

    start_index = first_id + offset

    global_start_time = time.time()
    processes = []

    step = portion // cores_number + portion % cores_number

    # Для небольшого количества записей
    if step > domains_count:
        step = domains_count
        cores_number = 1

    for offset in range(start_index, domains_count + start_index, step):
        portion_start_time = time.time()
        
        # Парсинг всех сайтов
        domains = DbConnector().make_db_request(f"SELECT * FROM domains WHERE id >= {offset} LIMIT {step}")
        
        # Только сайты на 1С:Bitrix
        # bitrix_domains = DbConnector().make_db_request(f"""
        #     SELECT domain_id as id, real_domain as domain FROM domain_info
        #     INNER JOIN domains ON domain_info.domain_id = domains.id
        #     WHERE cms=\"Bitrix\" AND domain_id >= {offset} LIMIT {step}
        # """)

        process = Process(target=run_parser, args=(step, offset, domains))
        process.start()
        processes.append(process)
        if len(processes) == cores_number:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {offset-(step*3)} по {offset+step} за {time.time() - portion_start_time} - Общее время парсинга: {time.time() - global_start_time}")

    print(f"Парсинг c {start_index} по {domains_count} закончился за {time.time() - global_start_time}")


if __name__ == "__main__":
    main()