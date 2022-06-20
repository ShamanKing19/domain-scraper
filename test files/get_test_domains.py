import asyncio
from urllib import response
from bs4 import BeautifulSoup
import aiohttp
import lxml

from db_connector import DbConnector


class DomainFinder:
    def __init__(self):
        self.timeout = 2

        self.user_agents = {
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "User-agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
            "User-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0"
        }



    async def getDomains(self):
        domains = []
        urls = [
            "https://arkhipsoft.ru/Rubrika/Cat?num=29",
            "https://arkhipsoft.ru/Rubrika/Cat?num=1",
            "https://arkhipsoft.ru/Rubrika/Cat?num=2",
        ]

        for url in urls:
            session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
            session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), timeout=session_timeout, trust_env=True)
            async with session.get(url) as response:
                bs4 = BeautifulSoup(await response.text(), "lxml")
                trs = bs4.find_all("tr")
                tags = [tr.find("a") for tr in trs]
                domains += [tag["href"] for tag in tags if "http" in tag["href"]]
            await session.close()

        return domains


    async def insertData(self):
        db = DbConnector()
        domains = await self.getDomains()
        for domain in domains:
            domain = domain.replace("http://", "")
            db.make_db_request(f"""
                INSERT IGNORE INTO domains (domain)
                VALUE ('{domain}')
            """)

if __name__ == "__main__":
    parser = DomainFinder()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    domains = loop.run_until_complete(parser.insertData())