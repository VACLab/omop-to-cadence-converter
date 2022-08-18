import duckdb
from pyarrow import csv

# TODO: move connections to separate file
DIR_CONCEPT = 'data/eunomia/CONCEPT.csv'
DIR_PERSON = 'data/eunomia/PERSON.csv'
DIR_CONDITION_OCCURRENCE = 'data/eunomia/CONDITION_OCCURRENCE.csv'


def set_source_connector(source):
	if source[-4:] == '.csv':
		return f"read_csv_auto('{source}')"
	else:
		# TODO: add support for other sources., e.g. postgres
		return ""

# TODO: move queries to separate file
query_attributes = f'''
	WITH stg_person AS (
		SELECT 
			PERSON_ID AS id,
			GENDER_CONCEPT_ID,
			date_part('year', current_date) - YEAR_OF_BIRTH AS age,
			-- RACE_CONCEPT_IDs are not in Eunomia's CONCEPT table
			RACE_SOURCE_VALUE AS race
		FROM {set_source_connector(DIR_PERSON)}
	),

	mapped_person AS (
		SELECT
			id,
			LOWER(CONCEPT_NAME) AS gender,
			age,
			race
		FROM stg_person
		LEFT JOIN {set_source_connector(DIR_CONCEPT)} AS concept
		ON stg_person.GENDER_CONCEPT_ID = concept.CONCEPT_ID
		ORDER BY id ASC
	),

	unpivoted AS (
		SELECT
			id,
			'gender' AS variable,
			gender AS value,
			'string' AS type
		FROM mapped_person
		UNION ALL
		SELECT
			id,
			'age' AS variable,
			age AS value,
			'int' AS type
		FROM mapped_person
		UNION ALL
		SELECT
			id,
			'race' AS variable,
			race AS value,
			'string' AS type
		FROM mapped_person
		ORDER BY id, variable
	)

	SELECT * FROM unpivoted
'''

query_events = f'''
	WITH stg_condition AS (
	SELECT 
		PERSON_ID AS id,
		CONDITION_CONCEPT_ID,
		-- CAST(CONDITION_START_DATE AS DATETIME) + INTERVAL 1 SECOND AS date
		CAST(CONDITION_START_DATE AS DATETIME) AS date
	FROM {set_source_connector(DIR_CONDITION_OCCURRENCE)}
	)

	SELECT
		stg_condition.id,
		stg_condition.date,
		concept.VOCABULARY_ID AS codeclass,
		concept.CONCEPT_CODE AS code
	FROM stg_condition
	LEFT JOIN {set_source_connector(DIR_CONCEPT)} AS concept
	ON stg_condition.CONDITION_CONCEPT_ID = concept.CONCEPT_ID
	ORDER BY id ASC, date ASC
'''

def convert_omop_to_cadence(query):
	conn = duckdb.connect()
	return conn.execute(query)

def export_as_csv(query, filename):
	csv.write_csv(convert_omop_to_cadence(query).arrow(), filename)

if __name__ == '__main__':
	export_as_csv(query_attributes, 'out/python/attributes.csv')
	export_as_csv(query_events, 'out/python/events.csv')

