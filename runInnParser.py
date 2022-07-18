import os
import time
from multiprocessing import Process
from modules.dbConnector import DbConnector
from modules.innParser import InnParser


def log(filename, content):
    if not os.path.exists("logs"): os.mkdir("logs")
    file = open(filename, "a", encoding="utf-8")
    file.write(content + "\n")
    file.close()


def runParser(portion, offset, inns):
    parser = InnParser(inns)
    parser.run()


def main():
    db = DbConnector()
    innsCount = db.makeSingleDbRequest("SELECT count(inn) FROM inns")["count(inn)"]    
    firstID = db.getFirstInnID()
    lastID = db.getLastInnID()
    startID = firstID

    portion = 1000
    coresNumber = 1

    global_start_time = time.time()
    processes = []

    step = portion // coresNumber + portion % coresNumber

    # Для небольшого количества записей
    if step > innsCount:
        step = innsCount
        coresNumber = 1

    while startID < lastID:
        # Только для вывода
        portionStartTime = time.time()

        try:
            inns = db.getInnsPortion(startID, step)
            startID = inns[-1]["id"]
        except Exception as e:
            inns = []
            startID = startID
            log("innRequestError.txt", e)

        # Парсинг всех сайтов
        try:
            inns = db.getInnsPortion(startID=startID, limit=step)
            startID = inns[-1]["id"]
        except Exception as e:
            inns = []
            log("requestError.txt", e)
        

        process = Process(target=runParser, args=(step, startID, inns))
        process.start()
        processes.append(process)
        print(f"С {startID} по {startID+step}")
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            print(f"С {startID-(step*3)} по {startID+step} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - global_start_time}")
    print(f"Парсинг c {firstID} по {lastID} закончился за {time.time() - global_start_time}")

    
if __name__ == "__main__":
    main()
    