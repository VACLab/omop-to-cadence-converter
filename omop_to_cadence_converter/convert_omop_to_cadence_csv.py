import os
import duckdb
from pyarrow import csv


def convert_omop_to_cadence(conn, query):
	return conn.execute(query)


def export_as_csv(conn, query, filename):
	csv.write_csv(convert_omop_to_cadence(conn, query).arrow(), filename)


def if_dir_not_exists_create(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)


if __name__ == '__main__':
	import generate_query_csv
	import configparser

	config = configparser.ConfigParser()
	config.read('config/config.ini')

	conn = duckdb.connect()

	if_dir_not_exists_create(config['CSV']['EXPORT_PATH'])
	export_as_csv(conn, generate_query_csv.attributes(), f"{config['CSV']['EXPORT_PATH']}/attributes.csv")
	export_as_csv(conn, generate_query_csv.events(), f"{config['CSV']['EXPORT_PATH']}/events.csv")
