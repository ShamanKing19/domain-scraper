from pprint import pprint
import time
from db_prod_connector import DbProdConnector


def main():
    print("Connecting...")
    db = DbProdConnector()
    print("Connected to db! Startring query...")

    start_time = time.time()
    domains = db.make_db_request("""
        SELECT DISTINCT d.real_domain, d.id, d_info.title, d_info.description, d_info.city, d_info.inn, d_info.cms, d_info.status, d_info.comment, c.name, sub_c.name, t.tag, d_info.status, GROUP_CONCAT(d_emails.email), GROUP_CONCAT(d_numbers.number)
            FROM
                domain_info d_info
            INNER JOIN
                domains d on d_info.domain_id = d.id
            LEFT JOIN
                tags t on d_info.tag_id = t.id
            LEFT JOIN 
                subcategory sub_c on t.subcategory_id = sub_c.id
            LEFT JOIN
                category c on sub_c.category_id = c.id
            LEFT JOIN
                domain_emails d_emails on d.id = d_emails.domain_id
            LEFT JOIN
                domain_phones d_numbers on d.id = d_numbers.domain_id
            WHERE
                d.status = 200 
            GROUP BY 
                d.real_domain
    """)

    # pprint(domains)
    print(time.time() - start_time)


main()