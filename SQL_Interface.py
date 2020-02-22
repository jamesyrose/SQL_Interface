#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL object for inserts and queries

OHLC Tables Structures

Tables                  Columns
Main                    TickerSymbol, SecurityType, Sector
{TickerSymbol}_{Year}   Datetime, Open, Close, High, Low, Volume

Tables are split by year to increase query speed. Total OHLC DB size is ~1.2TB (raw text)
with over 12K symbols.

"""

import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine
from config import sql_user, sql_pass

class SQL(object):
    def __init__(self, database):
        self.engine = create_engine("mysql+pymysql://{}:{}@localhost:3306/{}".format(sql_user, 
                                                                                     sql_passw, 
                                                                                     database
                                                                                    )
                                   )

    @property
    def show_tables(self):
        """
        Shows existing Tables
        SQL: 'show tables;'

        :return: list
        """
        tables = pd.read_sql(con=self.engine, sql="show tables;")
        return tables[tables.columns[0]].to_list()

    def create_table_ohlc(self, table_name):
        """
        Creates Table for OHLCV Data

        Creates table as
        Column Name         DType
        Datetime            TIMESTAMP
        Open                INT
        Close               INT
        High                INT
        Low                 INT
        Datetime            INT

        All OHLCV should be inserted as Integers,
        All OHLC values should be multiplied by 10**4
            Some data is down to 100th of a cent, so this will maintain resolution

        :param table_name: name of the table
        :return: None
        """
        sql_str = "CREATE TABLE {} " \
                  "(Datetime TIMESTAMP," \
                  " Open INT," \
                  " Close INT," \
                  " High INT," \
                  " Low INT," \
                  " Volume INT);".format(table_name)
        self.engine.execute(sql_str)

    def query_sql(self, sql_str: str):
        """
        queries SQL using SQL commands

        :param sql_str: SQL command (sql syntax required)
        :return: pd.DataFrame (sql query)
        """
        return pd.read_sql(con=self.engine, sql=sql_str)

    def pandas_upload(self, data: pd.DataFrame, table_name: str):
        """
        Uploads OHLCV Data from pandas dataframe

        if table does not exists:
            Creates table with proper dytpes
        else:
            query table and remove duplicates

        Append only new data to SQL tables

        :param data: pd.DataFrame
        :param table_name: Name of table to be uploaded too
        :return: None
        """
        if table_name not in self.show_tables:
            self.create_table_ohlc(table_name=table_name)
        else:
            datetime_max = data.Datetime.max()  # query for only data within time range
            datetime_min = data.Datetime.min()
            query_str = "SELECT distinct Datetime, Open, Close, High, Low, Volume " \
                        "FROM {} " \
                        "WHERE  Datetime BETWEEN '{}' AND '{}'; ".format(table_name,
                                                                         datetime_min,
                                                                         datetime_max
                                                                         )
            existing_data = self.query_sql(sql_str=query_str)
            data.Datetime = pd.to_datetime(data.Datetime)
            # appends two copies of existing data to new data and drops all values that occur more than once
            # appending two instances of the existing data guarantees duplicates of existing data, thus will
            # always be dropped
            data = pd.concat([data, existing_data, existing_data],
                             sort=False
                             ).drop_duplicates(keep=False)
        # insert data
        data.to_sql(name=table_name,
                    con=self.engine,
                    if_exists='append',
                    index=False
                    )

    def insert_data(self, ticker_symbol: str, data: pd.DataFrame):
        """
        Inserts data to sql

        Multiplies OHLC by 10**4 and converts to integer
        Groups data by year
        Inserts data under tablename {ticker_symbol}_{year}

        :param ticker_symbol: Symbol being inserted
        :param data: data for that symbol
        :return: none
        """
        data.set_index("Datetime",
                       drop=False,
                       inplace=True
                       )
        data.index = pd.to_datetime(data.index)
        for column in ["Open", "Low", "Close", "High"]:
            data[column] = (data[column] * 10000).apply(int)  # some data recorded to 100th of a cent
        data.Volume = data.Volume.apply(int)
        grouped_data = data.groupby(pd.Grouper(freq="Y"))
        for year, df in grouped_data:
            table_name = "{}_{}".format(ticker_symbol,
                                        year.year
                                        )
            self.pandas_upload(data=df.reset_index(drop=True),
                               table_name=table_name)



