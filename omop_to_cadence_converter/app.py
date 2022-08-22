import streamlit as st
import pandas as pd
import duckdb
import configparser

import convert_omop_to_cadence_csv
import generate_query_csv

config = configparser.ConfigParser()
config.read('config/config.ini')

st.title('OMOP to CADENCE Converter')

with st.form("config"):
	### Config ###
	st.sidebar.header('Config')
	year_for_age_calculation = st.sidebar.number_input('Year for age calculation', 
		value=int(config['DEFAULT']['YEAR_FOR_AGE_CALCULATION']),
		min_value=2000, max_value=2030, step=1)

	st.sidebar.header('OMOP')
	person_table_csv_path = st.sidebar.text_input('CSV path for Person table:', config['CSV']['TABLE_PERSON'])
	condition_occurrence_table_csv_path = st.sidebar.text_input('CSV path for Condition Occurrence table:', config['CSV']['TABLE_CONDITION_OCCURRENCE'])
	concept_table_csv_path = st.sidebar.text_input('CSV path for Concept table:', config['CSV']['TABLE_CONCEPT'])

	st.sidebar.header('CADENCE')
	csv_export_path = st.sidebar.text_input('CSV export path:', config['CSV']['EXPORT_PATH'])

	submitted = st.form_submit_button('Run conversion')

	if submitted:
		### OMOP ###
		st.header('OMOP')


		# OMOP Person Table
		df_person = pd.read_csv(person_table_csv_path, nrows=20)

		st.subheader('Person Table (sample)')
		st.dataframe(df_person)

		# OMOP Condition Occurrence Table
		df_condition_occurrence = pd.read_csv(condition_occurrence_table_csv_path, nrows=20)

		st.subheader('Condition Occurrence Table (sample)')
		st.dataframe(df_condition_occurrence)

		# OMOP Concept Table
		df_concept = pd.read_csv(concept_table_csv_path, nrows=20)

		st.subheader('Concept Table (sample)')
		st.dataframe(df_concept)

		### CADENCE ###
		st.header('CADENCE')

		conn = duckdb.connect()

		# CADENCE Attributes Table
		st.subheader('Attributes Table (sample)')

		df_attributes = convert_omop_to_cadence_csv.convert_omop_to_cadence(
			conn, 
			generate_query_csv.attributes(
			person_csv_path=person_table_csv_path,
			concept_csv_path=concept_table_csv_path,
			year_for_age_calculation=year_for_age_calculation
		)).df()

		st.dataframe(df_attributes.head(20))

		st.text('Summary of Attributes Table')
		st.write(df_attributes.describe(include='all').fillna("").astype("str"))

		# CADENCE Events Table
		st.subheader('Events Table (sample)')
		df_events = convert_omop_to_cadence_csv.convert_omop_to_cadence(
			conn,
			generate_query_csv.events(
			condition_occurrence_csv_path=condition_occurrence_table_csv_path,
			concept_csv_path=concept_table_csv_path
		)).df()

		st.dataframe(df_events.head(20))

		st.text('Summary of Events Table')
		st.write(df_events.describe(include='all', datetime_is_numeric=True).fillna("").astype("str"))

		convert_omop_to_cadence_csv.if_dir_not_exists_create(csv_export_path)
		convert_omop_to_cadence_csv.export_as_csv(
			conn, 
			generate_query_csv.attributes(
				person_csv_path=person_table_csv_path, 
				concept_csv_path=concept_table_csv_path, 
				year_for_age_calculation=year_for_age_calculation), 
			f"{csv_export_path}/attributes.csv")

		convert_omop_to_cadence_csv.export_as_csv(
			conn,
			generate_query_csv.events(
				condition_occurrence_csv_path=condition_occurrence_table_csv_path,
				concept_csv_path=concept_table_csv_path),
			f"{csv_export_path}/events.csv")

		st.success(f'Conversion completed! Exported to {csv_export_path}')