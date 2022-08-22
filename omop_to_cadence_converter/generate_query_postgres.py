import configparser

config = configparser.ConfigParser()
config.read('config/config.ini')

def attributes(person_table=config['POSTGRES']['TABLE_PERSON'],
					 concept_table=config['POSTGRES']['TABLE_CONCEPT'],
					 year_for_age_calculation=config['DEFAULT']['YEAR_FOR_AGE_CALCULATION']):
	return f'''
		CREATE TABLE cadence.attributes AS
		WITH stg_person AS (
			SELECT 
				PERSON_ID AS id,
				GENDER_CONCEPT_ID,
				{year_for_age_calculation} - YEAR_OF_BIRTH AS age,
				RACE_CONCEPT_ID
			FROM {person_table}
		),

		mapped_gender AS (
			SELECT
				id,
				CONCEPT_NAME AS gender,
				age,
				RACE_CONCEPT_ID
			FROM stg_person
			LEFT JOIN {concept_table} AS concept
			ON stg_person.GENDER_CONCEPT_ID = concept.CONCEPT_ID
			ORDER BY id ASC
		),

		mapped_race AS (
			SELECT
				id,
				gender,
				age,
				CONCEPT_NAME AS race
			FROM mapped_gender
			LEFT JOIN {concept_table} AS concept
			ON mapped_gender.RACE_CONCEPT_ID = concept.CONCEPT_ID
			ORDER BY id ASC
		),

		unpivoted AS (
			SELECT
				id,
				'gender' AS variable,
				gender AS value,
				'string' AS type
			FROM mapped_race
			UNION ALL
			SELECT
				id,
				'age' AS variable,
				CAST(age AS VARCHAR) AS value,
				'int' AS type
			FROM mapped_race
			UNION ALL
			SELECT
				id,
				'race' AS variable,
				race AS value,
				'string' AS type
			FROM mapped_race
			ORDER BY id, variable
		)

		SELECT * FROM unpivoted
	'''

def events(condition_occurrence_table=config['POSTGRES']['TABLE_CONDITION_OCCURRENCE'],
				 concept_table=config['POSTGRES']['TABLE_CONCEPT']):
	return f'''
		CREATE TABLE cadence.events AS 
		WITH stg_condition AS (
			SELECT 
				PERSON_ID AS id,
				CONDITION_CONCEPT_ID,
				CAST(CONDITION_START_DATE AS TIMESTAMP) AS date
			FROM {condition_occurrence_table}
		), 

		events AS (
			SELECT
				stg_condition.id,
				stg_condition.date,
				concept.VOCABULARY_ID AS codeclass,
				concept.CONCEPT_CODE AS code
			FROM stg_condition
			LEFT JOIN {concept_table} AS concept
			ON stg_condition.CONDITION_CONCEPT_ID = concept.CONCEPT_ID
			ORDER BY id ASC, date ASC
		)

		SELECT * FROM events
	'''