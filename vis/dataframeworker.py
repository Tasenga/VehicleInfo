from __future__ import annotations
import logging
from pathlib import Path
from typing import Type, List
from dataclasses import dataclass
from json import loads
import re

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql.functions import struct, col, when, collect_set

from .configuration import Configuration
from .ddl_processing import DDL


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def type_replace(main_df: DF, type_df: DF, replaced_column: str) -> DF:
    '''Function adds vehicle type information
    from the type table to the preliminary temporary table
    if the type code from both tables the same.'''
    return (
        main_df.join(type_df, main_df[replaced_column] == type_df['TXTCode'], 'left')
        .drop(replaced_column, "TXTCode")
        .withColumnRenamed('TXTTextLong', replaced_column)
    )


def map_wheel_block(jwheel: DF, tyres: DF, rims: DF) -> DF:
    '''
    Function returns dataframe (will add as 'wheels' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true)
         |-- NatCode: string (nullable = true)
         |-- wheels: struct (nullable = false)
         |    |-- front: struct (nullable = false)
         |    |    |-- rimWidth: string (nullable = true)
         |    |    |-- diameter: string (nullable = true)
         |    |    |-- tyre: struct (nullable = false)
         |    |    |    |-- width: string (nullable = true)
         |    |    |    |-- aspectRatio: string (nullable = true)
         |    |    |    |-- construction: string (nullable = true)
         |    |-- rear: struct (nullable = false)
         |    |    |-- rimWidth: string (nullable = true)
         |    |    |-- diameter: string (nullable = true)
         |    |    |-- tyre: struct (nullable = false)
         |    |    |    |-- width: string (nullable = true)
         |    |    |    |-- aspectRatio: string (nullable = true)
         |    |    |    |-- construction: string (nullable = true)
    '''
    wheels = (
        jwheel.join(tyres, ['VehType', 'JWHTYRTyreFCd', 'JWHTYRTyreRCd'], 'left')
        .join(rims, ['VehType', 'JWHRIMRimFCd', 'JWHRIMRimRCd'], 'left')
        .select(
            'VehType',
            'NatCode',
            struct(
                struct('rimWidth', 'diameter', struct('width', 'aspectRatio', 'construction').alias('tyre')).alias(
                    'front'
                ),
                struct('rimWidth', 'diameter', struct('width', 'aspectRatio', 'construction').alias('tyre')).alias(
                    'rear'
                ),
            ).alias('wheels'),
        )
    )
    return wheels


def map_model_block(model: DF, make: DF) -> DF:
    '''
    Function returns dataframe (will add as 'model' during fin_aggregation) with schema:
        |-- VehType: short (nullable = true)
        |-- TYPModCd: integer (nullable = true)
        |-- model: struct (nullable = false)
        |    |-- schwackeCode: integer (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- name2: string (nullable = true)
        |    |-- serialCode: string (nullable = true)
        |    |-- yearBegin: integer (nullable = true)
        |    |-- yearEnd: integer (nullable = true)
        |    |-- productionBegin: date (nullable = true)
        |    |-- productionEnd: date (nullable = true)
        |    |-- vehicleClass: string (nullable = true)
        |    |-- make: struct (nullable = false)
        |    |    |-- schwackeCode: integer (nullable = true)
        |    |    |-- name: string (nullable = true)

    '''
    model = model.alias('model').withColumn(
        'vehicleClass',
        when(col('TYPModCd') == 10, "Personenwagen")
        .when(col('TYPModCd') == 20, "Transporter")
        .when(col('TYPModCd') == 30, "Zweirad")
        .when(col('TYPModCd') == 40, "Gelandewagen")
        .otherwise(None),
    )
    model = model.join(make.alias('make'), ['VehType', 'schwackeCode'], 'left')
    model = model.select(
        'VehType',
        'TYPModCd',
        struct(
            col('TYPModCd').alias('schwackeCode'),
            'model.name',
            'name2',
            'serialCode',
            'yearBegin',
            'yearEnd',
            'productionBegin',
            'productionEnd',
            'vehicleClass',
            struct('model.schwackeCode', 'make.name').alias('make'),
        ).alias('model'),
    ).drop('schwackeCode')

    return model


def map_esaco_block(esajoin: DF, esaco: DF, txttable: DF) -> DF:
    '''
    Function returns dataframe (will add as 'esacos' during map_feature_block) with schema:
         |-- code: integer (nullable = true)
         |-- esaco: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- name: string (nullable = true)
         |    |    |-- mainGroup: string (nullable = true)
         |    |    |-- subGroup: string (nullable = true)
    '''
    esaco = esaco.select('name', 'mainGroup', 'subGroup', col("name").alias("name2"))
    replaced_columns = ['name2', 'mainGroup', 'subGroup']
    for column in replaced_columns:
        esaco = type_replace(esaco, txttable, column)

    esacos = (
        esajoin.join(esaco, 'name', 'left')
        .select('code', struct(col('name2').alias('name'), 'mainGroup', 'subGroup').alias('esacos'))
        .groupBy('code')
        .agg(collect_set('esacos').alias('esacos'))
    )
    return esacos


def map_color_block(typecol: DF, manucol: DF, eurocol: DF) -> DF:
    '''
    Function returns dataframe (will add as 'colors' during map_feature_block) with schema:
         |-- id: integer (nullable = true)
         |-- color: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- id: integer (nullable = true)
         |    |    |-- orderCode: string (nullable = true)
         |    |    |-- basicColorName: string (nullable = true)
         |    |    |-- basicColorCode: short (nullable = true)
         |    |    |-- manufacturerColorName: string (nullable = true)
         |    |    |-- manufacturerColorType: short (nullable = true)
    '''
    colors = (
        typecol.join(manucol, 'TCLMCLColCd', 'left')
        .join(eurocol, 'basicColorCode', 'left')
        .select(
            'id',
            struct(
                'id', 'orderCode', 'basicColorName', 'basicColorCode', 'manufacturerColorName', 'manufacturerColorType'
            ).alias('colors'),
        )
        .groupBy('id')
        .agg(collect_set('colors').alias('colors'))
    )

    return colors


def map_feature_block(addition: DF, color_block: DF, manufactor: DF, esaco_block: DF,) -> DF:
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

    features = (
        addition.join(manufactor, 'code', 'left')
        .join(color_block, 'id', 'left')
        .join(esaco_block, 'code', 'left')
        .select(
            'VehType',
            'NatCode',
            struct(
                'id',
                'code',
                'priceNet',
                'priceGross',
                'beginDate',
                'endDate',
                when(col('isOptional') == 0, False).otherwise(True),
                'manufacturerCode',
                'flagPack',
                'targetGroup',
                'taxRate',
                'currency',
                'colors',
                'esacos',
            ).alias('features'),
        )
    )
    return features


def map_engine_block(type: DF, consumer: DF, typ_envkv: DF, technic: DF, txttable: DF) -> DF:
    '''
    Function returns dataframe (will add as 'engine' during fin_aggregation) with schema:
        |-- VehType: short (nullable = true)
         |-- NatCode: string (nullable = true)
         |-- engine: struct (nullable = false)
         |    |-- engineType: string (nullable = true)
         |    |-- fuelType: string (nullable = true)
         |    |-- cylinders: short (nullable = true)
         |    |-- displacement: decimal(7,2) (nullable = true)
         |    |-- co2Emission: short (nullable = true)
         |    |-- co2Emission2: short (nullable = true)
         |    |-- emissionStandard: string (nullable = true)
         |    |-- energyEfficiencyClass: string (nullable = true)
         |    |-- power: struct (nullable = false)
         |    |    |-- ps: decimal(6,2) (nullable = true)
         |    |    |-- kw: decimal(6,2) (nullable = true)
         |    |-- fuelConsumption: struct (nullable = false)
         |    |    |-- urban: decimal(3,1) (nullable = true)
         |    |    |-- extraUrban: decimal(3,1) (nullable = true)
         |    |    |-- combined: decimal(3,1) (nullable = true)
         |    |    |-- urban2: decimal(3,1) (nullable = true)
         |    |    |-- extraUrban2: decimal(3,1) (nullable = true)
         |    |    |-- combined2: decimal(3,1) (nullable = true)
         |    |    |-- gasUrban: decimal(3,1) (nullable = true)
         |    |    |-- gasExtraUrban: decimal(3,1) (nullable = true)
         |    |    |-- gasCombined: decimal(3,1) (nullable = true)
         |    |    |-- gasUnit: string (nullable = true)
         |    |    |-- power: decimal(4,1) (nullable = true)
         |    |    |-- batteryCapacity: short (nullable = true)

    '''
    for column in ['fuelType', 'emissionStandard']:
        type = type_replace(type, txttable, column)

    technic = type_replace(technic, txttable, 'engineType')
    typ_envkv = type_replace(typ_envkv, txttable, 'energyEfficiencyClass')
    consumer = type_replace(consumer, txttable, 'gasUnit')

    key = ['VehType', 'NatCode']
    m = 'left'

    engine = (
        type.join(technic, key, m)
        .join(typ_envkv, key, m)
        .join(consumer, key, m)
        .select(
            'VehType',
            'NatCode',
            struct(
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
            ).alias('engine'),
        )
    )
    return engine


def fin_aggregation(
    type: DF,
    prices: DF,
    tcert: DF,
    txttable: DF,
    wheel_block: DF,
    model_block: DF,
    feature_block: DF,
    engine_block: DF,
) -> DF:
    '''
    Function returns resulting dataframe with schema:
         |-- schwackeCode: string (nullable = true)
         |-- name: string (nullable = true)
         |-- name2: string (nullable = true)
         |-- bodyType: string (nullable = true)
         |-- driveType: string (nullable = true)
         |-- transmissionType: string (nullable = true)
         |-- productionBegin: date (nullable = true)
         |-- productionEnd: date (nullable = true)
         |-- doors: short (nullable = true)
         |-- seats: short (nullable = true)
         |-- weight: integer (nullable = true)
         |-- dimentions: struct (nullable = false)
         |    |-- lenght: integer (nullable = true)
         |    |-- width: short (nullable = true)
         |-- model: struct (nullable = true)
         |    |-- schwackeCode: integer (nullable = true)
         |    |-- name: string (nullable = true)
         |    |-- name2: string (nullable = true)
         |    |-- serialCode: string (nullable = true)
         |    |-- yearBegin: integer (nullable = true)
         |    |-- yearEnd: integer (nullable = true)
         |    |-- productionBegin: date (nullable = true)
         |    |-- productionEnd: date (nullable = true)
         |    |-- vehicleClass: string (nullable = true)
         |    |-- make: struct (nullable = false)
         |    |    |-- schwackeCode: integer (nullable = true)
         |    |    |-- name: string (nullable = true)
         |-- wheels: struct (nullable = true)
         |    |-- front: struct (nullable = false)
         |    |    |-- rimWidth: string (nullable = true)
         |    |    |-- diameter: string (nullable = true)
         |    |    |-- tyre: struct (nullable = false)
         |    |    |    |-- width: string (nullable = true)
         |    |    |    |-- aspectRatio: string (nullable = true)
         |    |    |    |-- construction: string (nullable = true)
         |    |-- rear: struct (nullable = false)
         |    |    |-- rimWidth: string (nullable = true)
         |    |    |-- diameter: string (nullable = true)
         |    |    |-- tyre: struct (nullable = false)
         |    |    |    |-- width: string (nullable = true)
         |    |    |    |-- aspectRatio: string (nullable = true)
         |    |    |    |-- construction: string (nullable = true)
         |-- engine: struct (nullable = true)
         |    |-- engineType: string (nullable = true)
         |    |-- fuelType: string (nullable = true)
         |    |-- cylinders: short (nullable = true)
         |    |-- displacement: decimal(7,2) (nullable = true)
         |    |-- co2Emission: short (nullable = true)
         |    |-- co2Emission2: short (nullable = true)
         |    |-- emissionStandard: string (nullable = true)
         |    |-- energyEfficiencyClass: string (nullable = true)
         |    |-- power: struct (nullable = false)
         |    |    |-- ps: decimal(6,2) (nullable = true)
         |    |    |-- kw: decimal(6,2) (nullable = true)
         |    |-- fuelConsumption: struct (nullable = false)
         |    |    |-- urban: decimal(3,1) (nullable = true)
         |    |    |-- extraUrban: decimal(3,1) (nullable = true)
         |    |    |-- combined: decimal(3,1) (nullable = true)
         |    |    |-- urban2: decimal(3,1) (nullable = true)
         |    |    |-- extraUrban2: decimal(3,1) (nullable = true)
         |    |    |-- combined2: decimal(3,1) (nullable = true)
         |    |    |-- gasUrban: decimal(3,1) (nullable = true)
         |    |    |-- gasExtraUrban: decimal(3,1) (nullable = true)
         |    |    |-- gasCombined: decimal(3,1) (nullable = true)
         |    |    |-- gasUnit: string (nullable = true)
         |    |    |-- power: decimal(4,1) (nullable = true)
         |    |    |-- batteryCapacity: short (nullable = true)
         |-- features: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- id: integer (nullable = true)
         |    |    |-- code: integer (nullable = true)
         |    |    |-- priceNet: decimal(10,2) (nullable = true)
         |    |    |-- priceGross: decimal(10,2) (nullable = true)
         |    |    |-- beginDate: date (nullable = true)
         |    |    |-- endDate: date (nullable = true)
         |    |    |-- col7: boolean (nullable = false)
         |    |    |-- manufacturerCode: string (nullable = true)
         |    |    |-- flagPack: short (nullable = true)
         |    |    |-- targetGroup: short (nullable = true)
         |    |    |-- taxRate: decimal(4,2) (nullable = true)
         |    |    |-- currency: string (nullable = true)
         |    |    |-- colors: array (nullable = true)
         |    |    |    |-- element: struct (containsNull = false)
         |    |    |    |    |-- id: integer (nullable = true)
         |    |    |    |    |-- orderCode: string (nullable = true)
         |    |    |    |    |-- basicColorName: string (nullable = true)
         |    |    |    |    |-- basicColorCode: short (nullable = true)
         |    |    |    |    |-- manufacturerColorName: string (nullable = true)
         |    |    |    |    |-- manufacturerColorType: short (nullable = true)
         |    |    |-- esacos: array (nullable = true)
         |    |    |    |-- element: struct (containsNull = false)
         |    |    |    |    |-- name: string (nullable = true)
         |    |    |    |    |-- mainGroup: string (nullable = true)
         |    |    |    |    |-- subGroup: string (nullable = true)
         |-- prices: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- currency: string (nullable = true)
         |    |    |-- net: decimal(13,2) (nullable = true)
         |    |    |-- gross: decimal(13,2) (nullable = true)
         |    |    |-- taxRate: decimal(4,2) (nullable = true)
         |    |    |-- beginDate: date (nullable = true)
         |    |    |-- endDate: date (nullable = true)
         |-- certifications: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- hsn: string (nullable = true)
         |    |    |-- tsn: string (nullable = true)
    '''

    prices = prices.select(
        'VehType', 'NatCode', struct('currency', 'net', 'gross', 'taxRate', 'beginDate', 'endDate').alias('prices')
    )
    tcert = tcert.alias('tcert').select('VehType', 'NatCode', struct('hsn', 'tsn').alias('certifications'))

    variants = type.select(
        'VehType',
        'NatCode',
        'TYPModCd',
        'name',
        'name2',
        'bodyType',
        'driveType',
        'transmissionType',
        'productionBegin',
        'productionEnd',
        'doors',
        'seats',
        'weight',
        struct('lenght', 'width').alias('dimentions'),
    )
    for column in ['bodyType', 'driveType', 'transmissionType']:
        variants = type_replace(variants, txttable, column)

    unit_blocks = {"wheel": wheel_block, 'engine': engine_block}
    multiple_blocks = {'features': feature_block, "prices": prices, 'certifications': tcert}

    key = ['VehType', 'NatCode']
    m = 'left'

    final_table = variants.join(model_block, ['VehType', 'TYPModCd'], m)

    for name, df in unit_blocks.items():
        final_table = final_table.join(df, key, m)

    for name, df in multiple_blocks.items():
        df = df.groupBy(key).agg(collect_set(name).alias(name))
        final_table = final_table.join(df, key, m)

    final_table = final_table.withColumnRenamed('NatCode', 'schwackeCode').drop('VehType', 'TYPModCd')
    return final_table


@dataclass
class DataFrameWorker:
    '''class to create dataframe from source data,
    write it into files and database'''

    table: DF
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
         aggregates it according to full mode and saves like pyspark dataframe object.
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

        wheel_block = map_wheel_block(jwheel=df_list["jwheel"], tyres=df_list["tyres"], rims=df_list["rims"])

        model_block = map_model_block(model=df_list["model"], make=df_list["make"])

        esaco_block = map_esaco_block(esajoin=df_list["esajoin"], esaco=df_list["esaco"], txttable=df_list["txttable"])

        color_block = map_color_block(
            typecol=df_list["typecol"], manucol=df_list["manucol"], eurocol=df_list["eurocol"]
        )

        feature_block = map_feature_block(
            addition=df_list["addition"],
            color_block=color_block,
            manufactor=df_list['manufactor'],
            esaco_block=esaco_block,
        )

        engine_block = map_engine_block(
            type=df_list["type"],
            consumer=df_list["consumer"],
            typ_envkv=df_list["typ_envkv"],
            technic=df_list["technic"],
            txttable=df_list["txttable"],
        )

        tmp_table = fin_aggregation(
            type=df_list['type'],
            prices=df_list['pricehistory'],
            tcert=df_list['tcert'],
            txttable=df_list["txttable"],
            wheel_block=wheel_block,
            model_block=model_block,
            feature_block=feature_block,
            engine_block=engine_block,
        )

        return cls(table=tmp_table, configuration=configuration)

    def write_to_file(self) -> None:
        '''The function creates folder with one json
        file with all source table rows via one partition'''
        self.result_table_folder.mkdir(parents=True, exist_ok=True)
        self.table.coalesce(1).write.json(
            str(
                Path(
                    self.result_table_folder,
                    f'{self.configuration.db_name}_{self.configuration.mode.value}_'
                    f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
                )
            )
        )

    def read_from_file(self) -> List:
        '''The function reads json files with results per entry
        and returns list of dictionaries'''
        for path in Path(
            self.result_table_folder,
            f'{self.configuration.db_name}_{self.configuration.mode.value}_'
            f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
        ).iterdir():
            if re.match(r'.*(.json)$', str(path)):
                with Path(path).open('r') as file_result:
                    result = [loads(row) for row in file_result]
        return result
