from __future__ import annotations
from typing import Type, Any, Dict
from pathlib import Path
from dataclasses import dataclass
from json import loads

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql.types import IntegerType

import vis.descriptor as dsc
from vis.configuration import Configuration, ConfigurationForProcessing


def add_filter(df: DF, column: str, value: Any[str, int]) -> DF:
    '''Function filters dataframe by value in column
    (drop all rows without this value in this column)'''
    return df.filter(col(column) == value)


def get_date(df: DF, column: str, date_format: str) -> DF:
    '''Function changes type in column from string to DateType() by date_format'''
    return df.withColumn(column, to_date(column, date_format))


def get_int(df: DF, column: str) -> DF:
    '''Function changes type in column to IntegerType()'''
    return df.withColumn(column, col(column).cast(IntegerType()))


def get_boolean(df: DF, column: str, value: Any[str, int]) -> DF:
    '''Function replaces data in column to boolean (True/False) type
    by the value (value -> False, other -> True)'''
    return df.withColumn(column, when(col(column) == value, False).otherwise(True))


def get_new_col(df: DF, column: str, config_value: str) -> DF:
    '''Function adds new_column by the data from existing column'''
    config_value_to_dict = loads(config_value)
    new_column = list(config_value_to_dict.keys())[0]
    dictionary = list(config_value_to_dict.values())[0]
    iterator = iter(dictionary.items())
    first_key, first_value = next(iterator)
    result = when(col(column) == first_key, first_value)
    for key, value in dictionary.items():
        result = result.when(col(column) == key, value)
    return df.withColumn(new_column, result)


def replace_from_txttable(main_df: DF, txt_df: DF, replaced_column: str) -> DF:
    '''Function adds vehicle type information
    from the txttable to the another preliminary temporary table
    if the type code from both tables the same'''
    return (
        main_df.join(txt_df, main_df[replaced_column] == txt_df[dsc.TXTTABLE.table.TXTCode.fin_name], 'left')
        .drop(replaced_column, dsc.TXTTABLE.table.TXTCode.fin_name)
        .withColumnRenamed(dsc.TXTTABLE.table.TXTTextLong.fin_name, replaced_column)
    )


def rewrite_column(mode: str, column: str, df: DF, txt_df: DF, config_value: Dict) -> DF:
    if mode == 'date':
        df = get_date(df=df, column=column, date_format=config_value[column])
    if mode == 'integer':
        df = get_int(df=df, column=column)
    if mode == 'boolean':
        df = get_boolean(df=df, column=column, value=config_value[column])
    if mode == 'new_column':
        df = get_new_col(df=df, column=column, config_value=config_value[column])
    if mode == 'from_txttable':
        df = replace_from_txttable(main_df=df, txt_df=txt_df, replaced_column=column)
    return df


@dataclass
class Tables:
    tables: Dict[str, DF]
    configuration: Configuration

    @classmethod
    def obtain_tables(cls: Type, spark: SparkSession, configuration: Configuration) -> Tables:
        '''Function obtains dataframes from hive tables and saves all tables as dictionary of tables'''

        tables = dsc.TABLES
        config_tables = ConfigurationForProcessing.from_file(
            Path(Path.cwd(), configuration.source_folder, 'config_tables.ini')
        )
        df_list = {}

        for tbl in tables:
            df_list[tbl.name] = (
                spark.table(f'{configuration.db_name}_{configuration.mode.value}.{tbl.name}')
                .filter(
                    (col('data_date_part') == f'{configuration.current_date}')
                    & (col('data_timestamp_part') == f'{configuration.current_timestamp}')
                )
                .select(
                    [col(f'{column.old_name}').alias(f'{column.fin_name}') for column in tbl.table.__dict__.values()]
                )
                .dropDuplicates()
            )

            if tbl.add_filter:
                df_list[tbl.name] = add_filter(
                    df=df_list[tbl.name], column=tbl.add_filter, value=config_tables.__dict__[tbl.name][tbl.add_filter]
                )

            for column in tbl.table.__dict__.values():
                if column.rewrite:
                    df_list[tbl.name] = rewrite_column(
                        mode=column.rewrite.value,
                        column=column.fin_name,
                        df=df_list[tbl.name],
                        txt_df=df_list['txttable'],
                        config_value=config_tables.__dict__[tbl.name],
                    )
        return cls(tables=df_list, configuration=configuration)
