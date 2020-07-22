from __future__ import annotations
from typing import Type, Any, Dict
from pathlib import Path
from dataclasses import dataclass
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql.types import IntegerType

import vis.descriptor as dsc
from vis.configuration import Configuration, ConfigurationForProcessing


def add_filter(df: DF, column: str, value: Any[str, int]) -> DF:
    return df.filter(col(column) == value)


def get_date(df: DF, column: str, date_format: str) -> DF:
    return df.withColumn(column, to_date(column, date_format))


def get_int(df: DF, column: str) -> DF:
    return df.withColumn(column, col(column).cast(IntegerType()))


def get_boolean(df: DF, column: str, value: Any[str, int]) -> DF:
    return df.withColumn(column, when(col(column) == value, False).otherwise(True))


def get_new_col(df: DF, column: str, new_column: str, dictionary: Dict[int, str]) -> DF:
    iterator = iter(dictionary.items())
    first_key, first_value = next(iterator)
    result = when(col(column) == first_key, first_value)
    for key, value in dictionary.items():
        result = result.when(col(column) == key, value)
    return df.withColumn(new_column, result)


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
            print(tbl.name)
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
                    df=df_list[tbl.name], column=tbl.add_filter, value=config_tables.__dict__[tbl.add_filter]
                )

            for column in tbl.table.__dict__.values():
                if column.replace_type:
                    if column.replace_type.startswith('date'):
                        df_list[tbl.name] = get_date(
                            df=df_list[tbl.name],
                            column=column.fin_name,
                            date_format=re.findall(r'date:(\w+)$', column.replace_type)[0],
                        )
                    if column.replace_type == 'integer':
                        df_list[tbl.name] = get_int(df=df_list[tbl.name], column=column.fin_name)
                    if column.replace_type == 'boolean':
                        df_list[tbl.name] = get_boolean(
                            df=df_list[tbl.name], column=column.fin_name, value=config_tables.__dict__[column.fin_name]
                        )
                    if column.replace_type.startswith('new_column'):
                        new_column = re.findall(r'new_column:(\w+)', column.replace_type)[0]
                        df_list[tbl.name] = get_new_col(
                            df=df_list[tbl.name],
                            column=column.fin_name,
                            new_column=new_column,
                            dictionary=config_tables.__dict__[new_column],
                        )

            df_list[tbl.name].show()
            df_list[tbl.name].printSchema()
        return cls(tables=df_list, configuration=configuration)
