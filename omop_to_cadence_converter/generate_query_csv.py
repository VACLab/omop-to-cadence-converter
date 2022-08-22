import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

def attributes(person_csv_path=config['CSV']['TABLE_PERSON'],
					 concept_csv_path=config['CSV']['TABLE_CONCEPT'],
					 year_for_age_calculation=config['DEFAULT']['YEAR_FOR_AGE_CALCULATION']):
	return f'''
		WITH stg_person AS (
			SELECT 
				CAST(PERSON_ID AS VARCHAR) AS id,
				GENDER_CONCEPT_ID,
				{year_for_age_calculation} - YEAR_OF_BIRTH AS age,
				-- RACE_CONCEPT_IDs are not in Eunomia's CONCEPT table
				RACE_SOURCE_VALUE AS race
			FROM read_csv_auto('{person_csv_path}')
		),

		mapped_person AS (
			SELECT
				id,
				LOWER(CONCEPT_NAME) AS gender,
				age,
				race
			FROM stg_person
			LEFT JOIN read_csv_auto('{concept_csv_path}') AS concept
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

def events(condition_occurrence_csv_path=config['CSV']['TABLE_CONDITION_OCCURRENCE'],
				 concept_csv_path=config['CSV']['TABLE_CONCEPT']):
	return f'''
		WITH stg_condition AS (
		SELECT 
			CAST(PERSON_ID AS VARCHAR) AS id,
			CONDITION_CONCEPT_ID,
			CAST(CONDITION_START_DATE AS DATETIME) AS date
		FROM read_csv_auto('{condition_occurrence_csv_path}')
		)

		SELECT
			stg_condition.id,
			stg_condition.date,
			concept.VOCABULARY_ID AS codeclass,
			concept.CONCEPT_CODE AS code
		FROM stg_condition
		LEFT JOIN read_csv_auto('{concept_csv_path}') AS concept
		ON stg_condition.CONDITION_CONCEPT_ID = concept.CONCEPT_ID
		ORDER BY id ASC, date ASC
	'''
