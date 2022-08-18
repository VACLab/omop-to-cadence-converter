import omop_to_cadence_converter.convert_omop_to_cadence
from pyarrow import types

def test_set_source_connector_csv():
	source = 'data/eunomia/CONCEPT.csv'
	assert omop_to_cadence_converter.convert_omop_to_cadence.set_source_connector(source) == "read_csv_auto('data/eunomia/CONCEPT.csv')"

def test_set_source_connector_other():
	source = 'other'
	assert omop_to_cadence_converter.convert_omop_to_cadence.set_source_connector(source) == ""

def test_attributes_datatypes():
	table_attributes = omop_to_cadence_converter.convert_omop_to_cadence.convert_omop_to_cadence(omop_to_cadence_converter.convert_omop_to_cadence.query_attributes).arrow()
	assert types.is_string(table_attributes['variable'].type)
	assert types.is_string(table_attributes['value'].type)
	assert types.is_string(table_attributes['type'].type)
	
def test_events_datatypes():
	table_events = omop_to_cadence_converter.convert_omop_to_cadence.convert_omop_to_cadence(omop_to_cadence_converter.convert_omop_to_cadence.query_events).arrow()
	assert types.is_timestamp(table_events['date'].type)
	assert types.is_string(table_events['codeclass'].type)
	assert types.is_string(table_events['code'].type)