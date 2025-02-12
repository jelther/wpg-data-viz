import requests
import pandas as pd
import duckdb

api_url = "https://data.winnipeg.ca/resource/cam2-ii3u.json"

# Add duckdb database file
con = duckdb.connect('../wpgdata.db')

# Drop table if exists
con.execute("DROP TABLE IF EXISTS address")

# Columns
base_columns = [
    'street_number',
    'full_address',
    'street_address',
    'street_number_suffix',
    'street_name',
    'street_type',
    'street_direction',
    'unit_type',
    'unit_number',
    'neighbourhood',
    'ward',
    'school_division',
    'school_division_ward',
    'location',
    'point',
    'ward_as_of_september_17'
]

# Create table
sql_statement = """
CREATE TABLE address (
    street_number INTEGER,
    full_address VARCHAR,
    street_address VARCHAR,
    street_number_suffix VARCHAR,
    street_name VARCHAR,
    street_type VARCHAR,
    street_direction VARCHAR,
    unit_type VARCHAR,
    unit_number VARCHAR,
    neighbourhood VARCHAR,
    ward VARCHAR,
    school_division VARCHAR,
    school_division_ward VARCHAR,
    location VARCHAR,
    point VARCHAR,
    ward_as_of_september_17 VARCHAR
)
"""
con.execute(sql_statement)

offset = 0
limit = 1000

all_dataframes = []

chunk = 0
while True:
    response = requests.get(f"{api_url}?$limit={limit}&$offset={offset}")
    response.raise_for_status()
    data = response.json()
    if not data:
        break

    tmp_df = pd.DataFrame(data)
    # Remove columns with computed name on it
    for column in tmp_df.columns:
        if column.startswith(":@"):
            del tmp_df[column]

    for column in base_columns:
        if column not in tmp_df.columns:
            tmp_df[column] = None

    # Fill missing values
    tmp_df = tmp_df.fillna({
        'street_number': 0,
        'full_address': '',
        'street_address': '',
        'street_number_suffix': '',
        'street_name': '',
        'street_type': '',
        'street_direction': '',
        'unit_type': '',
        'unit_number': '',
        'neighbourhood': '',
        'ward': '',
        'school_division': '',
        'school_division_ward': '',
        'location': '',
        'point': '',
        'ward_as_of_september_17': ''
    })

    # Save to duckdb
    con.register("tmp_address", tmp_df)
    con.execute("INSERT INTO address SELECT * FROM tmp_address")

    offset += limit
    print(f"Downloaded {len(tmp_df.index)} records")

    chunk += 1