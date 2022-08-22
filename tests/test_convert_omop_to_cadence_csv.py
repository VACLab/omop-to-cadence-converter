from omop_to_cadence_converter import convert_omop_to_cadence_csv, generate_query_csv
from pyarrow import types


def test_attributes_datatypes():
	table_attributes = convert_omop_to_cadence_csv.convert_omop_to_cadence(generate_query_csv.attributes()).arrow()
	assert types.is_string(table_attributes['variable'].type)
	assert types.is_string(table_attributes['value'].type)
	assert types.is_string(table_attributes['type'].type)
	
def test_events_datatypes():
	table_events = convert_omop_to_cadence_csv.convert_omop_to_cadence(generate_query_csv.events()).arrow()
	assert types.is_timestamp(table_events['date'].type)
	assert types.is_string(table_events['codeclass'].type)
	assert types.is_string(table_events['code'].type)