"""Upload a bunch of random data to rds

1. create .env based on .env.example
2. install requirements
3. run ingest.py

"""

from os import environ
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd


def create_connection():
    return create_engine(
        f'postgresql+psycopg2://{environ["PGUID"]}:{environ["PGPASS"]}@{environ["PGHOST"]}/testdata'
    )


def find_csv_files():
    paths = Path(".").glob(pattern="**/*.csv")
    for path in paths:
        yield path


def path_to_dataframe(path):
    df = pd.read_csv(path)
    df.name = path.parts[0]
    return df


def create_or_append_to_table(df: pd.DataFrame, conn):
    df.to_sql(df.name, conn, if_exists="append", index=False, method="multi")


def main():
    connection = create_connection()
    for file in find_csv_files():
        if file.parts[0] != "index.csv":
            try:
                path_to_dataframe(file).pipe(create_or_append_to_table, connection)
            except Exception as e:
                print(f"Problem with {file}")
                print(repr(e))


if __name__ == "__main__":
    main()
