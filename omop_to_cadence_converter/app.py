import streamlit as st
import pandas as pd
import duckdb
import psycopg2
import configparser
import altair as alt

import convert_omop_to_cadence_csv
import generate_query_csv

import convert_omop_to_cadence_postgres
import generate_query_postgres

config = configparser.ConfigParser()
config.read('config/config.ini')

st.sidebar.title('OMOP to CADENCE Converter')

with st.form("config"):

	st.sidebar.header('Data Source')
	datasource = st.sidebar.selectbox('Please data source format', ['CSV', 'PostgreSQL'])
	if datasource == 'PostgreSQL':
		postgres_connection_string = st.sidebar.text_input(
		    'PostgreSQL connection string:', config['POSTGRES']['CONNECTION_STRING']
		)

	### Config ###
	st.sidebar.header('Config')
	year_for_age_calculation = st.sidebar.number_input(
	    'Year for age calculation',
	    value=int(config['DEFAULT']['YEAR_FOR_AGE_CALCULATION']),
	    min_value=2000,
	    max_value=2030,
	    step=1
	)

	st.sidebar.header('OMOP')

	# CSV
	if datasource == 'CSV':
		person_table_csv_path = st.sidebar.text_input('CSV path for Person table:', config['CSV']['TABLE_PERSON'])
		condition_occurrence_table_csv_path = st.sidebar.text_input(
		    'CSV path for Condition Occurrence table:', config['CSV']['TABLE_CONDITION_OCCURRENCE']
		)
		concept_table_csv_path = st.sidebar.text_input('CSV path for Concept table:', config['CSV']['TABLE_CONCEPT'])

	# PostgreSQL
	if datasource == 'PostgreSQL':
		person_table_postgres = st.sidebar.text_input(
		    'Schema.Database for Person table:', config['POSTGRES']['TABLE_PERSON']
		)
		condition_occurrence_table_postgres = st.sidebar.text_input(
		    'Schema.Database for Condition Occurrence table:', config['POSTGRES']['TABLE_CONDITION_OCCURRENCE']
		)
		concept_table_postgres = st.sidebar.text_input(
		    'Schema.Database for Concept table:', config['POSTGRES']['TABLE_CONCEPT']
		)

	st.sidebar.header('CADENCE')
	if datasource == 'CSV':
		csv_export_path = st.sidebar.text_input('CSV export path:', config['CSV']['EXPORT_PATH'])

	if datasource == 'PostgreSQL':
		st.sidebar.write('Output will be exported to `cadence.attributes` and `cadence.events` tables')

	submitted = st.form_submit_button('Run conversion')

	if submitted:
		if datasource == 'CSV':
			conn = duckdb.connect()
		if datasource == 'PostgreSQL':
			conn = psycopg2.connect(postgres_connection_string)
			cur = conn.cursor()

		### OMOP CDM ###
		st.header('OMOP CDM')

		with st.expander("See sample data"):
			# OMOP Person Table
			if datasource == 'CSV':
				df_person = pd.read_csv(person_table_csv_path, nrows=20)
			if datasource == 'PostgreSQL':
				cur.execute(f'SELECT * FROM {person_table_postgres} LIMIT 20')
				df_person = pd.DataFrame(cur.fetchall())
				df_person.columns = [desc[0] for desc in cur.description]

			st.subheader('Person Table (sample)')
			st.dataframe(df_person)

			# OMOP Condition Occurrence Table
			if datasource == 'CSV':
				df_condition_occurrence = pd.read_csv(condition_occurrence_table_csv_path, nrows=20)
			if datasource == 'PostgreSQL':
				cur.execute(f'SELECT * FROM {condition_occurrence_table_postgres} LIMIT 20')
				df_condition_occurrence = pd.DataFrame(cur.fetchall())
				df_condition_occurrence.columns = [desc[0] for desc in cur.description]

			st.subheader('Condition Occurrence Table (sample)')
			st.dataframe(df_condition_occurrence)

			# OMOP Concept Table
			if datasource == 'CSV':
				df_concept = pd.read_csv(concept_table_csv_path, nrows=20)
			if datasource == 'PostgreSQL':
				cur.execute(f'SELECT * FROM {concept_table_postgres} LIMIT 20')
				df_concept = pd.DataFrame(cur.fetchall())
				df_concept.columns = [desc[0] for desc in cur.description]

			st.subheader('Concept Table (sample)')
			st.dataframe(df_concept)

		st.markdown('----------------------------------------------------')
		### CADENCE ###
		st.header('CADENCE')

		# CADENCE Attributes Table
		st.subheader('Attributes Table (sample)')

		if datasource == 'CSV':
			df_attributes = convert_omop_to_cadence_csv.convert_omop_to_cadence(
			    conn,
			    generate_query_csv.attributes(
			        person_csv_path=person_table_csv_path,
			        concept_csv_path=concept_table_csv_path,
			        year_for_age_calculation=year_for_age_calculation
			    )
			).df()
		if datasource == 'PostgreSQL':
			cur.execute('CREATE SCHEMA IF NOT EXISTS cadence')
			cur.execute('DROP TABLE IF EXISTS cadence.attributes')
			cur.execute('DROP TABLE IF EXISTS cadence.events')
			cur.execute(
			    generate_query_postgres.attributes(
			        person_table=person_table_postgres,
			        concept_table=concept_table_postgres,
			        year_for_age_calculation=year_for_age_calculation
			    )
			)
			conn.commit()
			cur.execute('SELECT * FROM cadence.attributes')
			df_attributes = pd.DataFrame(cur.fetchall())
			df_attributes.columns = [desc[0] for desc in cur.description]

		st.dataframe(df_attributes.head(20))

		st.text('Summary of Attributes Table')
		st.write(df_attributes.describe(include='all').fillna("").astype("str"))

		st.altair_chart(
		    alt.Chart(df_attributes[df_attributes['variable'] == 'age'], title='Age distribution').mark_bar().encode(
		        alt.X('value:Q', bin=True, title='Age'),
		        y='count()',
		        tooltip=[
		            alt.Tooltip('value:Q', bin=True, title='Age'),
		            alt.Tooltip('count()', title='Count'),
		        ]
		    ).interactive(),
		    use_container_width=True
		)

		st.altair_chart(
		    alt.Chart(df_attributes[df_attributes['variable'] == 'gender'],
		              title='Gender distribution').mark_bar().encode(
		                  alt.X(
		                      'value:N',
		                      title='Gender',
		                      sort=alt.EncodingSortField(field="value", op="count", order='descending'),
		                      axis=alt.Axis(labelAngle=0)
		                  ),
		                  y='count()',
		                  tooltip=[
		                      alt.Tooltip('value:N', title='Gender'),
		                      alt.Tooltip('count()', title='Count'),
		                  ],
		                  color=alt.Color('value:N', legend=None)
		              ).interactive(),
		    use_container_width=True
		)

		st.altair_chart(
		    alt.Chart(df_attributes[df_attributes['variable'] == 'race'], title='Race distribution').mark_bar().encode(
		        alt.X(
		            'value:N',
		            title='Race',
		            sort=alt.EncodingSortField(field="value", op="count", order='descending'),
		            axis=alt.Axis(labelAngle=0)
		        ),
		        y='count()',
		        tooltip=[
		            alt.Tooltip('value:N', title='Race'),
		            alt.Tooltip('count()', title='Count'),
		        ],
		        color=alt.Color('value:N', legend=None)
		    ),
		    use_container_width=True
		)

		# CADENCE Events Table
		st.subheader('Events Table (sample)')
		if datasource == 'CSV':
			df_events = convert_omop_to_cadence_csv.convert_omop_to_cadence(
			    conn,
			    generate_query_csv.events(
			        condition_occurrence_csv_path=condition_occurrence_table_csv_path,
			        concept_csv_path=concept_table_csv_path
			    )
			).df()
		if datasource == 'PostgreSQL':
			cur.execute('DROP TABLE IF EXISTS cadence.events')
			cur.execute(
			    generate_query_postgres.events(
			        condition_occurrence_table=condition_occurrence_table_postgres,
			        concept_table=concept_table_postgres
			    )
			)
			conn.commit()
			cur.execute('SELECT * FROM cadence.events')
			df_events = pd.DataFrame(cur.fetchall())
			df_events.columns = [desc[0] for desc in cur.description]

		st.dataframe(df_events.head(20))

		st.text('Summary of Events Table')
		st.write(df_events.describe(include='all', datetime_is_numeric=True).fillna("").astype("str"))

		st.altair_chart(
		    alt.Chart(df_events, title='Year distribution').mark_bar().encode(
		        alt.X('year(date):T', title='Year'),
		        y='count()',
		        tooltip=[alt.Tooltip('year(date):T', title='Year'),
		                 alt.Tooltip('count()', title='Count')]
		    ).interactive(),
		    use_container_width=True
		)

		st.altair_chart(
		    alt.Chart(df_events, title='Code Class distribution').mark_bar().encode(
		        alt.X('codeclass:N', title='Code Class', axis=alt.Axis(labelAngle=0)),
		        y='count()',
		        tooltip=[alt.Tooltip('codeclass:N', title='Code Class'),
		                 alt.Tooltip('count()', title='Count')],
		        color=alt.Color('codeclass:N', legend=None)
		    ).interactive(),
		    use_container_width=True
		)

		st.altair_chart(
		    alt.Chart(df_events, title='Code distribution').mark_bar().encode(
		        alt.Y(
		            'code:N',
		            title='Code',
		            sort=alt.EncodingSortField(field="code", op="count", order='descending'),
		            axis=alt.Axis(labelAngle=0)
		        ),
		        x='count()',
		        tooltip=[alt.Tooltip('code:N', title='Code'),
		                 alt.Tooltip('count()', title='Count')],
		        color=alt.Color('code:N', legend=None)
		    ).interactive(),
		    use_container_width=True
		)

		if datasource == 'CSV':
			convert_omop_to_cadence_csv.if_dir_not_exists_create(csv_export_path)
			convert_omop_to_cadence_csv.export_as_csv(
			    conn,
			    generate_query_csv.attributes(
			        person_csv_path=person_table_csv_path,
			        concept_csv_path=concept_table_csv_path,
			        year_for_age_calculation=year_for_age_calculation
			    ), f"{csv_export_path}/attributes.csv"
			)

			convert_omop_to_cadence_csv.export_as_csv(
			    conn,
			    generate_query_csv.events(
			        condition_occurrence_csv_path=condition_occurrence_table_csv_path,
			        concept_csv_path=concept_table_csv_path
			    ), f"{csv_export_path}/events.csv"
			)

			st.success(f'Conversion completed! Exported to {csv_export_path}')

		if datasource == 'PostgreSQL':
			cur.close()
			conn.close()

			st.success(f'Conversion completed! Exported to `cadence.attributes` and `cadence.events`.')