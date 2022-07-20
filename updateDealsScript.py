import asyncio
import json
from pprint import pprint

from tqdm import tqdm
from modules.crmClient import CrmClient
from modules.dbClient import DbClient


def main():    
    crm = CrmClient("logs/domains.json")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crm.run())


if __name__ == "__main__":
    main()
