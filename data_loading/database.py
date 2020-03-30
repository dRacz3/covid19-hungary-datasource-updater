import time

import sqlalchemy
from typing import Dict, Any
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from data_loading.korona_gov_update import get_new_values
import pandas as pd

import os


class TableProps:
    name = 'covid_hungary_data'
    fertozott = "Fertztt"
    gyogyult = "Gygyult"
    elhunyt = "Elhunyt"
    karantenban = "Karantnban"
    mintavetel = "Mintavtel"
    timestamp = "time"


def drop_table(db):
    db.execute(f"""
    IF EXISTS(SELECT *
          FROM   {TableProps.name})
  DROP TABLE {TableProps.name}""")


def create_table(db):
    tables: sqlalchemy.engine.result.ResultProxy = db.execute(f"""
    CREATE TABLE {TableProps.name} (
      index int,
      Fertztt int,
      Gygyult int,
      Elhunyt int,
      Karantnban int,
      Mintavtel int,
      time varchar(255)
    )""")


def add_new_row_to_table(db, values: Dict[str, Any]):
    db.execute(f"""
    INSERT INTO {TableProps.name}
    VALUES ({values['Fertztt']},
            {values['Gygyult']},
            {values['Elhunyt']},
            {values['Karantnban']},
            {values['Mintavtel']},
            {values['time']}    
); """)


def create_connection() -> sqlalchemy.engine.base.Engine:
    DATABASE_URL = os.environ['DATABASE_URL']
    db = create_engine(DATABASE_URL, connect_args={'sslmode': 'require'}, echo=True).connect()
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
        pass
        #create_table(db)
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
