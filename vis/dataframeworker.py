from __future__ import annotations
import logging
from pathlib import Path
from typing import Type, List
from dataclasses import dataclass
from json import loads
import re

from pyspark.sql import SparkSession, dataframe

from .configuration import Configuration
from .ddl_processing import DDL


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def type_replace(
    main_df: dataframe.DataFrame, type_df: dataframe.DataFrame, replaced_column: str
) -> dataframe.DataFrame:
    '''Functions adds vehicle type information
    from the type table to the preliminary temporary table
    if the type code from both tables the same.'''
    return (
        main_df.join(type_df, main_df[replaced_column] == type_df['TXTCode'], 'left')
        .drop(replaced_column, "TXTCode")
        .withColumnRenamed('TXTTextLong', replaced_column)
    )


@dataclass
class DataFrameWorker:
    '''class to create dataframe from source data,
    write it into files and database'''

    table: dataframe.DataFrame
    configuration: Configuration
    result_table_folder: Path = Path(Path.cwd(), 'result')

    @classmethod
    def create_short_tmp_table(cls: Type, spark: SparkSession, configuration: Configuration) -> DataFrameWorker:
        '''
        The function gets required information from several hive-tables,
         aggregates it and saves like pyspark dataframe object.
         '''

        ddl_tmp_table = DDL(configuration, 'short_tmp_table_select_ddl.txt')
        ddl_tmp_table.update_ddl()
        ddl_txttable = DDL(configuration, 'txttable_select_ddl.txt')
        ddl_txttable.update_ddl()

        tmp_table = ddl_tmp_table.run_ddl(spark)
        type_table = ddl_txttable.run_ddl(spark)

        for replaced_column in [
            'bodyType',
            'driveType',
            'transmissionType',
        ]:
            tmp_table = type_replace(tmp_table, type_table, replaced_column)

        tmp_table = tmp_table.drop_duplicates()
        return cls(table=tmp_table, configuration=configuration)

    def write_to_file(self) -> None:
        '''The function creates folder with one json
        file with all source table rows via one partition'''
        self.result_table_folder.mkdir(parents=True, exist_ok=True)
        self.table.coalesce(1).write.json(
            str(
                Path(
                    self.result_table_folder,
                    f'{self.configuration.current_date}' f'_{self.configuration.current_timestamp}',
                )
            )
        )

    def read_from_file(self) -> List:
        '''The function reads json files with results per entry
        and returns list of dictionaries'''
        for path in Path(
            self.result_table_folder, f'{self.configuration.current_date}' f'_{self.configuration.current_timestamp}',
        ).iterdir():
            if re.match(r'.*(.json)$', str(path)):
                with Path(path).open('r') as file_result:
                    result = [loads(row) for row in file_result]
        return result
