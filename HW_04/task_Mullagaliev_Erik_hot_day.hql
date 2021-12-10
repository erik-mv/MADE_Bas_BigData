SELECT `date`, count(ip) as visit_n FROM logs
GROUP BY `date`
ORDER by visit_n DESC
LIMIT 10;
