import omop_to_cadence_converter.convert_omop_to_cadence

def test_set_source_connector_csv():
	source = 'data/eunomia/CONCEPT.csv'
	assert omop_to_cadence_converter.convert_omop_to_cadence.set_source_connector(source) == "read_csv_auto('data/eunomia/CONCEPT.csv')"

def test_set_source_connector_other():
	source = 'other'
	assert omop_to_cadence_converter.convert_omop_to_cadence.set_source_connector(source) == ""
	