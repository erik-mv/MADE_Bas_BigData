cqlsh $1 -e "CREATE KEYSPACE IF NOT EXISTS $2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}"
