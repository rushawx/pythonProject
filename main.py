import os
import time
from functools import wraps

import dotenv
import pandas as pd
import psycopg2
import requests
from sqlalchemy import Engine, create_engine

dotenv.load_dotenv()

SOURCE_URL = os.getenv("SOURCE_URL")
FILE_NAME_PARQUET = os.getenv("FILE_NAME_PARQUET")
FILE_NAME_CSV = os.getenv("FILE_NAME_CSV")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_TABLE = os.getenv("PG_TABLE")
PG_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
DDL_FILE_NAME = os.getenv("DDL_FILE_NAME")


def log_time(func, file="log.txt"):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        record = ",".join([func.__doc__, func.__name__, str(round(end - start, 3))])
        with open(file, "a") as f:
            f.write(record + "\n")
        print(record)
        return result

    return wrapper


@log_time
def get_data_from_web(url: str, file: str) -> None:
    """requests"""
    response = requests.get(url)
    if response.status_code == 200:
        with open(file, "wb") as f:
            f.write(response.content)
    return None


@log_time
def init_pg_engine(dsn: str) -> Engine:
    """psycopg"""
    engine = create_engine(dsn)
    return engine


@log_time
def create_pg_table(engine: Engine, file: str) -> None:
    """psycopg"""
    connection = engine.raw_connection()
    with connection.cursor() as cursor:
        with open(file, "r") as f:
            cursor.execute(f.read())
    connection.commit()
    connection.close()
    return None


@log_time
def read_parquet(file: str) -> pd.DataFrame:
    """pandas"""
    df = pd.read_parquet(file)
    return df.iloc[:1000]


@log_time
def save_data_to_csv(df: pd.DataFrame, csv_name: str) -> str:
    """pandas"""
    with open(csv_name, "wb") as file:
        df.to_csv(file, header=False, sep=";", index=True)
    return csv_name


@log_time
def load_data_to_postgres_by_psycopg2_execute(df: pd.DataFrame, dsn: str, table: str) -> None:
    """psycopg2"""
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            query = (
                f"insert into {table} values ({','.join(['%s' for x in range(df.shape[1]+1)])})"
            )
            for row in df.reset_index().values.tolist():
                cursor.execute(query, row)
        conn.commit()


@log_time
def load_data_to_postgres_by_psycopg2_execute_many(df: pd.DataFrame, dsn: str, table: str) -> None:
    """psycopg2"""
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            query = (
                f"insert into {table} values ({','.join(['%s' for x in range(df.shape[1]+1)])})"
            )
            cursor.executemany(query, df.reset_index().values.tolist())
        conn.commit()


@log_time
def load_data_to_postgres_by_pandas_to_sql(df: pd.DataFrame, engine: Engine, table: str) -> None:
    """pandas"""
    df.to_sql(table, engine, if_exists="append", index=True)
    return None


@log_time
def load_data_to_postgres_by_psycopg_copy_expert(dsn: str, table: str, csv_name: str) -> None:
    """psycopg2"""
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            with open(csv_name, "r") as f:
                cursor.copy_expert(
                    f"copy {table} from stdin with (delimiter ';', header, null '')", f
                )
        conn.commit()


@log_time
def load_data_to_postgres_by_psycopg_copy_from(dsn: str, table: str, csv_name: str) -> None:
    """psycopg2"""
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            with open(csv_name, "r") as file:
                cursor.copy_from(file, table, sep=";")


def main():
    if not os.path.exists(FILE_NAME_PARQUET):
        get_data_from_web(SOURCE_URL, FILE_NAME_PARQUET)
    engine = init_pg_engine(PG_DSN)
    create_pg_table(engine, DDL_FILE_NAME)
    df = read_parquet(FILE_NAME_PARQUET)
    save_data_to_csv(df, FILE_NAME_CSV)

    load_data_to_postgres_by_psycopg2_execute(df, PG_DSN, PG_TABLE)

    create_pg_table(engine, DDL_FILE_NAME)
    load_data_to_postgres_by_psycopg2_execute_many(df, PG_DSN, PG_TABLE)

    create_pg_table(engine, DDL_FILE_NAME)
    load_data_to_postgres_by_pandas_to_sql(df, engine, PG_TABLE)

    create_pg_table(engine, DDL_FILE_NAME)
    load_data_to_postgres_by_psycopg_copy_expert(PG_DSN, PG_TABLE, FILE_NAME_CSV)

    create_pg_table(engine, DDL_FILE_NAME)
    load_data_to_postgres_by_psycopg_copy_from(PG_DSN, PG_TABLE, FILE_NAME_CSV)

    print("done!")


if __name__ == "__main__":
    main()
