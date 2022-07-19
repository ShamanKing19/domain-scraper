from datetime import datetime
import os
import time
from multiprocessing import Process
from modules.dbClient import DbClient
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
    db = DbClient()
    innsCount = db.makeSingleDbRequest("SELECT count(inn) FROM inns")["count(inn)"]    
    firstID = db.getFirstInnID()
    lastID = db.getLastInnID()
    startID = firstID

    offset = 0
    portion = 1000
    coresNumber = 4

    globalStartTime = time.time()
    processes = []

    step = portion // coresNumber + portion % coresNumber
    startIndex = firstID + offset

    # Для небольшого количества записей
    if step > innsCount:
        step = innsCount
        coresNumber = 1

    while startIndex < lastID:
        # Только для вывода
        infoStartID = startIndex
        portionStartTime = time.time()

        try:
            inns = db.getInnsPortion(startIndex, step)
            startIndex = inns[-1]["id"]
        except Exception as e:
            inns = []
            startIndex = infoStartID
            log("innRequestError.txt", e)

        process = Process(target=runParser, args=(step, startIndex, inns))
        process.start()
        processes.append(process)
        if len(processes) == coresNumber:
            for process in processes:
                process.join()
            processes.clear()
            outputString = f"С {infoStartID - (step*(coresNumber-1))} по {startIndex} за {time.time() - portionStartTime} - Общее время парсинга: {time.time() - globalStartTime} - {datetime.now()}"
            log("logs/statsINN.txt", outputString)
    print(f"Парсинг c {firstID} по {lastID} закончился за {time.time() - globalStartTime}")

    
if __name__ == "__main__":
    main()
    