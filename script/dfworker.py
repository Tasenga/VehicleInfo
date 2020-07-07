from __future__ import annotations
import logging
from pathlib import Path
from typing import Type, List
from dataclasses import dataclass
from json import loads
import re

from .configuration import Configuration

from pyspark.sql import SparkSession, dataframe


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


@dataclass
class DfWorker:
    '''class to create dataframe from source data,
    write it into files and database'''

    table: dataframe.DataFrame
    configuration: Configuration
    tmp_table_folder: Path

    @classmethod
    def create_tmp_table(
        cls: Type, spark: SparkSession, configuration
    ) -> DfWorker:
        '''
        The function gets required information from several hive-tables,
         aggregates it and saves like pyspark dataframe object.
         '''

        tmp_table = spark.sql(
            f'''SELECT
            TYPNatCode AS schwackeCode,
            STRUCT(MODNatCode AS schwackeCode,
            MODName AS name,
            MODName2 AS name2) AS model,
            STRUCT(MAKNatCode AS schwackeCode,
            MAKName AS name) AS make,
            TYPName AS name,
            TYPDoor AS doors,
            TYPSeat AS seats,
            TYPTXTBodyCo1Cd2 AS bodyType,
            TYPTXTDriveTypeCd2 AS driveType,
            TYPTXTTransTypeCd2 AS transmissionType
            FROM
                (SELECT * FROM {configuration.db_name}.type
                WHERE data_date_part='{configuration.current_date}' AND
                    data_timestamp_part='{configuration.current_timestamp}')
                AS TYPE
                LEFT JOIN
                (SELECT * FROM {configuration.db_name}.model
                WHERE data_date_part='{configuration.current_date}' AND
                    data_timestamp_part='{configuration.current_timestamp}')
                AS model
                ON
                type.TYPVehType = model.MODVehType AND
                type.TYPModCd = model.MODNatCode
                LEFT JOIN
                (SELECT * FROM {configuration.db_name}.make
                WHERE data_date_part='{configuration.current_date}' AND
                    data_timestamp_part='{configuration.current_timestamp}')
                AS make
                ON
                type.TYPVehType = make.MAKVehType AND
                type.TYPMakCd = make.MAKNatCode;'''
        )

        type_table = spark.sql(
            f'''SELECT
                TXTCode,
                TXTTextLong
            FROM {configuration.db_name}.txttable
            WHERE data_date_part='{configuration.current_date}' AND
            data_timestamp_part='{configuration.current_timestamp}';'''
        )

        def type_replace(
            main_df: dataframe.DataFrame,
            type_df: dataframe.DataFrame,
            replaced_column: str,
        ) -> dataframe.DataFrame:
            '''Functions adds vehicle type information
            from the type table to the preliminary temporary table
            if the type code from both tables the same.'''
            return (
                main_df.join(
                    type_df,
                    main_df[replaced_column] == type_df['TXTCode'],
                    'left',
                )
                .drop(replaced_column, "TXTCode")
                .withColumnRenamed('TXTTextLong', replaced_column)
            )

        for replaced_column in [
            'bodyType',
            'driveType',
            'transmissionType',
        ]:
            tmp_table = type_replace(tmp_table, type_table, replaced_column)

        tmp_table = tmp_table.drop_duplicates()
        return cls(
            table=tmp_table,
            configuration=configuration,
            tmp_table_folder=Path(
                Path.cwd(), configuration.mode_folder.value, 'result'
            ),
        )

    def write_to_file(self) -> None:
        '''The function creates folder with one json
        file with all source table rows via one partition'''
        self.tmp_table_folder.mkdir(parents=True, exist_ok=True)
        self.table.coalesce(1).write.json(
            str(
                Path(
                    self.tmp_table_folder,
                    f'{self.configuration.current_date}'
                    f'_{self.configuration.current_timestamp}',
                )
            )
        )

    def read_from_file(self) -> List:
        '''The function reads json files with results per entry
        and returns list of dictionaries'''
        for path in Path(
            self.tmp_table_folder,
            f'{self.configuration.current_date}'
            f'_{self.configuration.current_timestamp}',
        ).iterdir():
            if re.match(r'.*(.json)$', str(path)):
                with Path(path).open('r') as file_result:
                    result = [loads(row) for row in file_result]
        return result
