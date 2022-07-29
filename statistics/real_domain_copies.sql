SELECT real_domain, count(real_domain)
FROM domain_info 
GROUP BY real_domain 
HAVING count(real_domain) > 1 
ORDER BY count(real_domain) DESC;