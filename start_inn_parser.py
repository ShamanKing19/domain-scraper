
import asyncio
from modules.inn_parser import InnInfoParser


def main():
    inn_parser = InnInfoParser()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(inn_parser.parse_inns())
    
if __name__ == "__main__":
    main()
    