import time
from multiprocessing import Process
from modules.db_connector import DbConnector
from modules.inn_parser import InnInfoParser

def run_parser(portion, offset, inns):
    parser = InnInfoParser(inns)
    parser.run()


def main():
    inns_count = DbConnector().make_single_db_request("SELECT count(inn) FROM company_info")["count(inn)"]
    first_id = DbConnector().make_single_db_request("SELECT id FROM company_info ORDER BY id ASC LIMIT 1")["id"]

    offset = 0
    portion = 10000
    cores_number = 4
    start_index = first_id - 1 + offset

    global_start_time = time.time()
    processes = []

    step = portion // cores_number + portion % cores_number

    # Для небольшого количества записей
    if step > inns_count:
        step = inns_count
        cores_number = 1

    for offset in range(start_index, inns_count + start_index, step):
        portion_start_time = time.time()
        inns = DbConnector().make_db_request(f"SELECT * FROM company_info WHERE id >= {offset} LIMIT {step}")
        process = Process(target=run_parser, args=(step, offset, inns))
        process.start()
        processes.append(process)
        print(f"С {offset} по {offset+step}")
        if len(processes) == cores_number:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {offset-(step*3)} по {offset+step} за {time.time() - portion_start_time} - Общее время парсинга: {time.time() - global_start_time}")

    print(f"Парсинг c {start_index} по {inns_count} закончился за {time.time() - global_start_time}")

    
if __name__ == "__main__":
    main()
    