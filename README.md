# OMOP to Cadence Converter

[![.github/workflows/poetry-pytest.yml](https://github.com/na399/omop-to-cadence-converter/actions/workflows/poetry-pytest.yml/badge.svg)](https://github.com/na399/omop-to-cadence-converter/actions/workflows/poetry-pytest.yml)

## Requirements

- Python v3.9+
- [Poetry](https://python-poetry.org/docs/#installation)

## Instructions

Install packages:

```{bash}
poetry install
```

Run app:

```{bash}
poetry run streamlit run omop_to_cadence_converter/app.py
```

Run CLI:

```{bash}
poetry run python run omop_to_cadence_converter/convert_omop_to_cadence_csv.py
```

or

```{bash}
poetry run python run omop_to_cadence_converter/convert_omop_to_cadence_postgres.py
```

Run tests:

```{bash}
poetry run pytest
```
