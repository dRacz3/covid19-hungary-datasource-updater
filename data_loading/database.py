import sqlite3
import time

import sqlalchemy
from typing import Dict, Any
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from data_loading.korona_gov_update import get_new_values
import pandas as pd

import os


class TableProps:
    name = 'COVID_HUNGARY'
    api_fertozott_pest = 'api-fertozott-pest'
    api_fertozott_videk = 'api-fertozott-videk'
    api_gyogyult_pest = 'api-gyogyult-pest'
    api_gyogyult_videk = 'api-gyogyult-videk'
    api_elhunyt_pest = 'api-elhunyt-pest'
    api_elhunyt_videk = 'api-elhunyt-videk'
    api_karantenban = 'api-karantenban'
    api_mintavetel = 'api-mintavetel'
    api_elhunyt_global = 'api-elhunyt-global'
    api_fertozott_global = 'api-fertozott-global'
    api_gyogyult_global = 'api-gyogyult-global'
    timestamp = 'timestamp'


def drop_table(db):
    db.execute(f"""
    IF EXISTS(SELECT *
          FROM   {TableProps.name})
  DROP TABLE {TableProps.name}""")


def create_table(db):
    tables: sqlalchemy.engine.result.ResultProxy = db.execute(f"""
CREATE TABLE {TableProps.name} (
{TableProps.api_fertozott_pest.replace('-', '_')} int,
{TableProps.api_fertozott_videk.replace('-', '_')} int,
{TableProps.api_gyogyult_pest.replace('-', '_')} int,
{TableProps.api_gyogyult_videk.replace('-', '_')} int,
{TableProps.api_elhunyt_pest.replace('-', '_')} int,
{TableProps.api_elhunyt_videk.replace('-', '_')} int,
{TableProps.api_karantenban.replace('-', '_')} int,
{TableProps.api_mintavetel.replace('-', '_')} int,
{TableProps.api_elhunyt_global.replace('-', '_')} int,
{TableProps.api_fertozott_global.replace('-', '_')} int,
{TableProps.api_gyogyult_global.replace('-', '_')} int,
{TableProps.timestamp.replace('-', '_')} varchar(255)
)""")
    print(tables)


def create_connection() -> sqlalchemy.engine.base.Engine:
    try:
        DATABASE_URL = os.environ['DATABASE_URL']
        db = create_engine(DATABASE_URL, connect_args={'sslmode': 'require'}, echo=True).connect()
    except Exception:
        print("Falling back to sqlite.db")
        db_file = 'sqlite.db'
        try:
            db = create_engine(f"sqlite:////{db_file}").connect()
        except Exception as e:
            print(e)
    return db


def clear_table(db: sqlalchemy.engine.base.Engine):
    db.execute(f"DROP TABLE {TableProps.name}")


if __name__ == '__main__':
    db = create_connection()
    try:
        clear_table(db)
    except:
        pass
    try:
        create_table(db)
    except:
        pass
    database_connection = create_connection()

    while True:
        try:
            data = pd.read_sql_table(TableProps.name, database_connection)
            data.append(get_new_values())
        except Exception as e:
            data = pd.DataFrame.from_records([get_new_values()])

        data.to_sql(TableProps.name, database_connection, if_exists='append')

        time.sleep(5)
    database_connection.close()

    print(f"Current data :{data}")

    print(f"New values: {get_new_values()}")
