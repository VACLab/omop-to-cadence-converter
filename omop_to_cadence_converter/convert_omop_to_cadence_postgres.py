import os
import psycopg2


def postgres_to_csv(conn, table, filename):
	cur = conn.cursor()
	f = open(filename, 'w')
	cur.copy_expert(f'COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER', f)
	f.close()
	cur.close()


def if_dir_not_exists_create(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)


if __name__ == '__main__':
	import generate_query_postgres
	import configparser

	config = configparser.ConfigParser()
	config.read('config/config.ini')

	conn = psycopg2.connect(config['POSTGRES']['CONNECTION_STRING'])
	cur = conn.cursor()
	cur.execute('CREATE SCHEMA IF NOT EXISTS cadence')
	cur.execute('DROP TABLE IF EXISTS cadence.attributes')
	cur.execute('DROP TABLE IF EXISTS cadence.events')

	cur.execute(generate_query_postgres.attributes())
	cur.execute(generate_query_postgres.events())
	conn.commit()
	cur.close()

	if config['POSTGRES']['EXPORT_AS_CSV']:
		if_dir_not_exists_create(config['POSTGRES']['EXPORT_PATH'])
		postgres_to_csv(conn, 'cadence.attributes', f"{config['POSTGRES']['EXPORT_PATH']}/attributes.csv")
		postgres_to_csv(conn, 'cadence.events', f"{config['POSTGRES']['EXPORT_PATH']}/events.csv")

	conn.close()
