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
import logging
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine
from datetime import datetime
_script_path = os.path.realpath(__file__)
_script_dir = os.path.dirname(_script_path)
_log_path = os.path.join(_script_dir, ".logs", __file__.replace(".py", ".log"))


def getLogger(log_file, level=logging.INFO):
    """
    Stream Logger + File Log

    :param log_file:  Path to log file
    :param level: logging level
    :return:  logger
    """
    name = "new_logger"
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(stream)
    return logger


class SQL(object):
    def __init__(self, database):
        self.engine = create_engine("mysql+pymysql://{}:{}@localhost:3306/{}".format(sql_user, 
                                                                                     sql_passw, 
                                                                                     database
                                                                                    )
                                   )
        self.logger = getLogger(log_file=_log_path)
    @property
    def show_tables(self):
        """
        Shows existing Tables
        SQL: 'show tables;'

        :return: list
        """
        tables = pd.read_sql(con=self.engine, sql="show tables;")
        return tables[tables.columns[0]].to_list()

    @property
    def symbols_avaliable(self):
        """
        Shows the symbols that are available

        converts show_tables to dict, select keys, convert to dict
        :return:  list
        """
        return list(dict.fromkeys([symbol.split("_")[0] for symbol in self.show_tables]))


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
    def query_one_symbol(self, symbol=str, start_date=[datetime, str], end_date=[datetime, str], datetime_format=None
                         ) -> pd.DataFrame:
        """
        Queries SQL for Data

        Takes user input of ticker symbol and date range and queries database.
        Query is split up by years to  minimize the datetime search. 
        Converts the OHLC values back to original (decimal) format

        EX SQL Command:
        SELECT CAST(Open AS DECIMAL) / 10000 AS Open, Close
        FROM (SELECT * FROM {symbol}_2018
              UNION
              SELECT * FROM {symbol}_2019 WHERE Datetime BETWEEN '2018-01-01' and '2019-05-05') x
        ORDER BY x.Datetime

        :param symbol: Ticker Symbol
        :param start_date: Begining of Date Range
        :param end_date:  End of Data Range
        :param datetime_format: datetime format if string dates are passed
        :return:
        """
        # Converting string to datetime if needed
        if isinstance(start_date, str) and datetime_format is not None:
            try:
                start_date = datetime.strptime(start_date, datetime_format)
            except ValueError:
                raise(self.logger.error('Start Date Not Formatted Properly'))
        elif isinstance(start_date, str) and datetime_format is None:
            raise(self.logger.error('Datetime format not provided'))
        if isinstance(end_date, str) and datetime_format is not None:
            try:
                end_date = datetime.strptime(end_date, datetime_format)
            except ValueError:
                raise(self.logger.error('End Date Not Formatted Properly'))
        elif isinstance(end_date, str) and datetime_format is None:
            raise(self.logger.error('Datetime format not provided'))
        # building an SQL command
        start_year = start_date.year
        end_year = end_date.year
        # Splitting by dates to minimize the datetime between search
        # Starting Year
        sql_str = "SELECT Datetime, Open, Close, High, Low, Volume " \
                  "FROM {}_{} " \
                  "WHERE Datetime BETWEEN '{}' AND'{}'".format(symbol,
                                                               start_year,
                                                               start_date,
                                                               end_date
                                                               )
        # Inbetween years (we know we will use the whole year
        for full_year in range(start_year+1, end_year):
            sql_str = "{} UNION " \
                      "SELECT Datetime, Open, Close, High, Low, Volume " \
                      "FROM {}_{}".format(sql_str,
                                          symbol,
                                          full_year
                                          )
        # Ending Year
        sql_str = "{} UNION " \
                  "SELECT Datetime, Open, Close, High, Low, Volume " \
                  "FROM {}_{} " \
                  "WHERE Datetime BETWEEN '{}' AND '{}'".format(sql_str,
                                                                symbol,
                                                                end_year,
                                                                start_date,
                                                                end_date
                                                                )
        # Converting OHLC back to decimals
        final_sql_str = "SELECT " \
                        "    Datetime, "\
                        "    CAST(Open AS DECIMAL) / 10000 as Open, " \
                        "    CAST(Close AS DECIMAL) / 10000 as Close, " \
                        "    CAST(High AS DECIMAL) / 10000 as High, " \
                        "    CAST(Low AS DECIMAL) / 10000 as Low, " \
                        "    Volume " \
                        "FROM ({}) X " \
                        "ORDER BY X.Datetime".format(sql_str)
        return self.query_sql(sql_str=final_sql_str)


