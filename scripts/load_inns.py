import csv
import time

from pymysql import IntegrityError
from db_connector import DbConnector


def load_rows():
    db = DbConnector()
    print("Загрузка началась...")
    start = time.time()

    with open("./data/INNS.csv", newline="") as file:
        reader = csv.DictReader(file)
        inns = set()

        for i, row in enumerate(reader):
            inns.add(row["inn"]) 
            if i % 100000 == 0 and i != 0: 
                print(i)
                
        for i, inn in enumerate(inns):
            try:
                db.make_db_request(f"INSERT INTO company_info (inn) VALUE ({inn})")
                if i % 100000 == 0: print(i)
            except IntegrityError as error:
                print(error)
    print(f"Загрузка закончилась за {time.time() - start}")


def main():
    load_rows()


if __name__ == "__main__":
    main()