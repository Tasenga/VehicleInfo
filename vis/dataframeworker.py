from __future__ import annotations
import logging
from pathlib import Path
from typing import Type, List
from dataclasses import dataclass
from json import loads
import re

from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import struct, col, when

from .configuration import Configuration
from .ddl_processing import DDL


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def type_replace(
    main_df: dataframe.DataFrame, type_df: dataframe.DataFrame, replaced_column: str
) -> dataframe.DataFrame:
    '''Function adds vehicle type information
    from the type table to the preliminary temporary table
    if the type code from both tables the same.'''
    return (
        main_df.join(type_df, main_df[replaced_column] == type_df['TXTCode'], 'left')
        .drop(replaced_column, "TXTCode")
        .withColumnRenamed('TXTTextLong', replaced_column)
    )


def map_wheel_block(
    jwheel_table: dataframe.DataFrame, tyres_table: dataframe.DataFrame, rims_table: dataframe.DataFrame
) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'wheel' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true) -> will drop after fin_aggregation
         |-- NatCode: string (nullable = true) -> will drop after fin_aggregation
         |-- rimWidth: string (nullable = true)
         |-- diameter: string (nullable = true)
         |-- tyre: struct (nullable = false)
         |    |-- width: string (nullable = true)
         |    |-- aspectRatio: string (nullable = true)
         |    |-- construction: string (nullable = true)
    '''
    return (
        jwheel_table.join(tyres_table, ['VehType', 'JWHTYRTyreFCd', 'JWHTYRTyreRCd'], 'left')
        .join(rims_table, ['VehType', 'JWHRIMRimFCd', 'JWHRIMRimRCd'], 'left')
        .select(
            'VehType', 'NatCode', 'rimWidth', 'diameter', struct('width', 'aspectRatio', 'construction').alias("tyre")
        )
    )


def map_model_block(model_table: dataframe.DataFrame, make_table: dataframe.DataFrame) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'model' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true) -> will rename as 'vehicle class' and replace data after fin_aggregation
         |-- TYPModCd: integer (nullable = true) -> will rename as 'schwackeCode' after fin_aggregation
         |-- name: string (nullable = true)
         |-- name2: string (nullable = true)
         |-- serialCode: string (nullable = true)
         |-- yearBegin: integer (nullable = true)
         |-- yearEnd: integer (nullable = true)
         |-- productionBegin: date (nullable = true)
         |-- productionEnd: date (nullable = true)
         |-- make: struct (nullable = false)
         |    |-- schwackeCode: integer (nullable = true)
         |    |-- name: string (nullable = true)
    '''
    return (
        model_table.join(make_table, ['VehType', 'schwackeCode'], 'left')
        .select('model_table.*', struct('model_table.schwackeCode', 'make_table.name').alias("make"))
        .drop('schwackeCode')
    )


def map_esaco_block(esajoin_table: dataframe.DataFrame, esaco_table: dataframe.DataFrame) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'esaco' during map_feature_block) with schema:
         |-- code: integer (nullable = true) -> will rename as 'name' during map_feature_block
         |-- mainGroup: string (nullable = true)
         |-- subGroup: string (nullable = true)
    '''
    return esajoin_table.join(esaco_table, 'name', 'left').select('code', 'mainGroup', 'subGroup')


def map_color_block(
    typecol_table: dataframe.DataFrame, manucol_table: dataframe.DataFrame, eurocol_table: dataframe.DataFrame
) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'color' during map_feature_block) with schema:
         |-- id: integer (nullable = true) -> will drop during map_feature_block
         |-- orderCode: string (nullable = true)
         |-- basicColorName: string (nullable = true)
         |-- basicColorCode: short (nullable = true)
         |-- manufacturerColorName: string (nullable = true)
         |-- manufacturerColorType: short (nullable = true)
    '''
    return (
        typecol_table.join(manucol_table, 'TCLMCLColCd', 'left')
        .join(eurocol_table, 'basicColorCode', 'left')
        .select(
            'id', 'orderCode', 'basicColorName', 'basicColorCode', 'manufacturerColorName', 'manufacturerColorType'
        )
    )


def map_feature_block(
    addition_table: dataframe.DataFrame,
    color_table: dataframe.DataFrame,
    manufactor_table: dataframe.DataFrame,
    esaco_table: dataframe.DataFrame,
) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'feature' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true) -> will drop after fin_aggregation
         |-- NatCode: string (nullable = true) -> will drop after fin_aggregation
         |-- id: integer (nullable = true)
         |-- code: integer (nullable = true)
         |-- priceNet: decimal(10,2) (nullable = true)
         |-- priceGross: decimal(10,2) (nullable = true)
         |-- beginDate: date (nullable = true)
         |-- endDate: date (nullable = true)
         |-- isOptional: boolean (nullable = false)
         |-- manufacturerCode: string (nullable = true)
         |-- flagPack: short (nullable = true)
         |-- targetGroup: short (nullable = true)
         |-- taxRate: decimal(4,2) (nullable = true)
         |-- currency: string (nullable = true)
         |-- color: struct (nullable = false)
         |    |-- orderCode: string (nullable = true)
         |    |-- basicColorName: string (nullable = true)
         |    |-- basicColorCode: short (nullable = true)
         |    |-- manufacturerColorName: string (nullable = true)
         |    |-- manufacturerColorType: short (nullable = true)
         |-- esaco: struct (nullable = false)
         |    |-- name: integer (nullable = true) -> will replace data from txttable after fin_aggregation
         |    |-- mainGroup: string (nullable = true) -> will replace data from txttable after fin_aggregation
         |    |-- subGroup: string (nullable = true) -> will replace data from txttable after fin_aggregation
    '''
    return (
        addition_table.join(color_table, 'id', 'left')
        .join(manufactor_table, 'code', 'left')
        .join(esaco_table, 'code', 'left')
        .select(
            'VehType',
            'NatCode',
            'id',
            'code',
            'priceNet',
            'priceGross',
            'beginDate',
            'endDate',
            'isOptional',
            'manufacturerCode',
            'flagPack',
            'targetGroup',
            'taxRate',
            'currency',
            struct(
                'orderCode', 'basicColorName', 'basicColorCode', 'manufacturerColorName', 'manufacturerColorType'
            ).alias('color'),
            struct(col('code').alias('name'), 'mainGroup', 'subGroup').alias('esaco'),
        )
        .withColumn('isOptional', when(col('isOptional') == 0, False).otherwise(True))
    )


def map_engine_block(
    type_table: dataframe.DataFrame,
    consumer_table: dataframe.DataFrame,
    typ_envkv_table: dataframe.DataFrame,
    technic_table: dataframe.DataFrame,
) -> dataframe.DataFrame:
    '''
    Function returns dataframe (will add as 'engine' during fin_aggregation) with schema:
        |-- VehType: short (nullable = true) -> will drop after fin_aggregation
         |-- NatCode: string (nullable = true) -> will drop after fin_aggregation
         |-- engineType: string (nullable = true)
         |-- fuelType: string (nullable = true)
         |-- cylinders: short (nullable = true)
         |-- displacement: decimal(7,2) (nullable = true)
         |-- co2Emission: short (nullable = true)
         |-- co2Emission2: short (nullable = true)
         |-- emissionStandard: string (nullable = true)
         |-- energyEfficiencyClass: string (nullable = true)
         |-- power: struct (nullable = false)
         |    |-- ps: decimal(6,2) (nullable = true)
         |    |-- kw: decimal(6,2) (nullable = true)
         |-- fuelConsumption: struct (nullable = false)
         |    |-- urban: decimal(3,1) (nullable = true)
         |    |-- extraUrban: decimal(3,1) (nullable = true)
         |    |-- combined: decimal(3,1) (nullable = true)
         |    |-- urban2: decimal(3,1) (nullable = true)
         |    |-- extraUrban2: decimal(3,1) (nullable = true)
         |    |-- combined2: decimal(3,1) (nullable = true)
         |    |-- gasUrban: decimal(3,1) (nullable = true)
         |    |-- gasExtraUrban: decimal(3,1) (nullable = true)
         |    |-- gasCombined: decimal(3,1) (nullable = true)
         |    |-- gasUnit: string (nullable = true)
         |    |-- power: decimal(4,1) (nullable = true)
         |    |-- batteryCapacity: short (nullable = true)
    '''
    key = ['VehType', 'NatCode']
    m = 'left'
    return (
        type_table.join(technic_table, key, m)
        .join(typ_envkv_table, key, m)
        .join(consumer_table, key, m)
        .select(
            'VehType',
            'NatCode',
            'engineType',
            'fuelType',
            'cylinders',
            'displacement',
            'co2Emission',
            'co2Emission2',
            'emissionStandard',
            'energyEfficiencyClass',
            struct('ps', 'kw').alias('power'),
            struct(
                'urban',
                'extraUrban',
                'combined',
                'urban2',
                'extraUrban2',
                'combined2',
                'gasUrban',
                'gasExtraUrban',
                'gasCombined',
                'gasUnit',
                'power',
                'batteryCapacity',
            ).alias('fuelConsumption'),
        )
    )


def fin_aggregation(variant: dataframe.DataFrame, pricehistory: dataframe.DataFrame) -> dataframe.DataFrame:
    '''

    :param variant:
    :param pricehistory:
    :return:
    '''
    key = ['VehType', 'NatCode']
    m = 'left'
    return variant.join(pricehistory, key, m).withColumn('price', struct('pricehistory.*')).drop(pricehistory.columns)


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
         aggregates it according to short mode and saves like pyspark dataframe object.
         '''

        tables = ["short_tmp_table", "txttable"]

        df_list = {}
        for table in tables:
            ddl = DDL(configuration, f'{table}_select_ddl.txt')
            ddl.update_ddl()
            df_list[table] = ddl.run_ddl(spark)

        tmp_table = df_list["short_tmp_table"]

        for replaced_column in [
            'bodyType',
            'driveType',
            'transmissionType',
        ]:
            tmp_table = type_replace(tmp_table, df_list["txttable"], replaced_column)

        tmp_table = tmp_table.drop_duplicates()
        return cls(table=tmp_table, configuration=configuration)

    @classmethod
    def create_full_tmp_table(cls: Type, spark: SparkSession, configuration: Configuration) -> DataFrameWorker:
        '''
        The function gets required information from several hive-tables,
         aggregates it according to short mode and saves like pyspark dataframe object.
         '''

        tables = [
            "jwheel",
            "rims",
            "tyres",
            "consumer",
            "typ_envkv",
            "make",
            "model",
            "type",
            "pricehistory",
            "technic",
            "tcert",
            "typecol",
            "manucol",
            "eurocol",
            "addition",
            "manufactor",
            "esajoin",
            "esaco",
            "txttable",
        ]

        df_list = {}
        for table in tables:
            ddl = DDL(configuration, f'{table}_select_ddl.txt')
            ddl.update_ddl()
            df_list[table] = ddl.run_ddl(spark)

        # wheel_block = map_wheel_block(df_list["jwheel"], df_list["tyres"], df_list["rims"])
        #
        # model_block = map_model_block(df_list["model"], df_list["make"])

        tmp_table = df_list["short_tmp_table"]

        for replaced_column in [
            'bodyType',
            'driveType',
            'transmissionType',
        ]:
            tmp_table = type_replace(tmp_table, df_list["txttable"], replaced_column)

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
                    f'{self.configuration.db_name}_{self.configuration.current_date}'
                    f'_{self.configuration.current_timestamp}',
                )
            )
        )

    def read_from_file(self) -> List:
        '''The function reads json files with results per entry
        and returns list of dictionaries'''
        for path in Path(
            self.result_table_folder,
            f'{self.configuration.db_name}_{self.configuration.current_date}'
            f'_{self.configuration.current_timestamp}',
        ).iterdir():
            if re.match(r'.*(.json)$', str(path)):
                with Path(path).open('r') as file_result:
                    result = [loads(row) for row in file_result]
        return result
