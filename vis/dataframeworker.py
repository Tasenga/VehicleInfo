from __future__ import annotations
import logging
from pathlib import Path
from typing import Type, List, Any, Callable, Dict, Union
from dataclasses import dataclass
from json import loads
import re

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as DF
from pyspark.sql.functions import struct, col, collect_set

import vis.descriptor as dsc
from vis.configuration import Configuration
from vis.sql_processing import SQL
from vis.tablespreparator import replace_from_txttable


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

_T = dsc.TABLES
_JWHEEL = dsc.JWHEEL.table
_TYRES = dsc.TYRES.table
_RIMS = dsc.RIMS.table
_MAKE = dsc.MAKE.table
_MODEL = dsc.MODEL.table
_ESACO = dsc.ESACO.table
_ESAJOIN = dsc.ESAJOIN.table
_TYPECOL = dsc.TYPECOL.table
_ADDITION = dsc.ADDITION.table
_MANUFACTOR = dsc.MANUFACTOR.table
_MANUCOL = dsc.MANUCOL.table
_EUROCOL = dsc.EUROCOL.table
_TYPE = dsc.TYPE.table
_CONSUMER = dsc.CONSUMER.table
_TYP_ENVKV = dsc.TYP_ENVKV.table
_TECHNIC = dsc.TECHNIC.table
_PRICE = dsc.PRICEHISTORY.table
_TCERT = dsc.TCERT.table


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

    tyres_key = [_TYRES.TYRVehType.fin_name, _TYRES.JWHTYRTyreFCd.fin_name, _TYRES.JWHTYRTyreRCd.fin_name]
    rims_key = [_RIMS.RIMVehType.fin_name, _RIMS.JWHRIMRimFCd.fin_name, _RIMS.JWHRIMRimRCd.fin_name]

    tyres_col = [_TYRES.TYRWidth.fin_name, _TYRES.TYRCrossSec.fin_name, _TYRES.TYRDesign.fin_name]
    rims_col = [_RIMS.RIMWidth.fin_name, _RIMS.RIMDiameter.fin_name, struct(tyres_col).alias('tyre')]

    wheels = (
        jwheel.join(tyres, tyres_key, 'left')
        .join(rims, rims_key, 'left')
        .select(
            _JWHEEL.JWHVehType.fin_name,
            _JWHEEL.JWHNatCode.fin_name,
            struct(struct(rims_col).alias('front'), struct(rims_col).alias('rear'),).alias('wheels'),
        )
    )
    return wheels


def map_model_block(model: DF, make: DF) -> DF:
    '''
    Function returns dataframe (will be added as 'model' during fin_aggregation) with schema:
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

    make_key = [_MAKE.MAKVehType.fin_name, _MAKE.MAKNatCode.fin_name]
    model = model.join(make, make_key, 'left')

    model = model.select(
        _MODEL.MODVehType.fin_name,
        _MODEL.MODNatCode.fin_name,
        struct(
            model[_MODEL.MODNatCode.fin_name].alias('schwackeCode'),
            model[_MODEL.MODName.fin_name].alias('name'),
            _MODEL.MODName2.fin_name,
            _MODEL.MODModelSerCode.fin_name,
            _MODEL.MODBegin.fin_name,
            _MODEL.MODEnd.fin_name,
            _MODEL.MODImpBegin.fin_name,
            _MODEL.MODImpEnd.fin_name,
            'vehicleClass',
            struct(model[_MAKE.MAKNatCode.fin_name], make[_MAKE.MAKName.fin_name]).alias('make'),
        ).alias('model'),
    )
    return model


def map_esaco_block(esajoin: DF, esaco: DF) -> DF:
    '''
    Function returns dataframe (will be grouped and added as 'esacos' during map_feature_block) with schema:
         |-- code: integer (nullable = true)
         |-- esacos: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- name: string (nullable = true)
         |    |    |-- mainGroup: string (nullable = true)
         |    |    |-- subGroup: string (nullable = true)
    '''

    esacos = (
        esajoin.join(esaco, _ESACO.ESGTXTCodeCd2.fin_name, 'left')
        .select(
            [
                _ESAJOIN.ESJEQTEQCodeCd.fin_name,
                struct(
                    _ESACO.ESGTXTCodeCd2_.fin_name, _ESACO.ESGTXTMainGrpCd2.fin_name, _ESACO.ESGTXTSubGrpCd2.fin_name,
                ).alias('esacos'),
            ]
        )
        .groupBy(_ESAJOIN.ESJEQTEQCodeCd.fin_name)
        .agg(collect_set('esacos').alias('esacos'))
    )
    return esacos


def map_color_block(typecol: DF, manucol: DF, eurocol: DF) -> DF:
    '''
    Function returns dataframe (be grouped and added as 'colors' during map_feature_block) with schema:
         |-- id: integer (nullable = true)
         |-- colors: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- orderCode: string (nullable = true)
         |    |    |-- basicColorName: string (nullable = true)
         |    |    |-- basicColorCode: short (nullable = true)
         |    |    |-- manufacturerColorName: string (nullable = true)
         |    |    |-- manufacturerColorType: short (nullable = true)
    '''

    colors = (
        typecol.join(manucol, _MANUCOL.MCLManColCode.fin_name, 'left')
        .join(eurocol, _EUROCOL.ECLColID.fin_name, 'left')
        .select(
            _TYPECOL.TCLTypEqtCode.fin_name,
            struct(
                _TYPECOL.TCLOrdCd.fin_name,
                _EUROCOL.ECLColName.fin_name,
                _EUROCOL.ECLColID.fin_name,
                _MANUCOL.MCLColName.fin_name,
                _MANUCOL.MCLPaintTrimFlag.fin_name,
            ).alias('colors'),
        )
        .groupBy(_TYPECOL.TCLTypEqtCode.fin_name)
        .agg(collect_set('colors').alias('colors'))
    )
    return colors


def map_feature_block(addition: DF, color_block: DF, manufactor: DF, esaco_block: DF,) -> DF:
    '''
    Function returns dataframe (be grouped and added as 'features' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true)
         |-- NatCode: string (nullable = true)
         |-- features: struct (nullable = false)
         |    |-- id: integer (nullable = true)
         |    |-- code: integer (nullable = true)
         |    |-- priceNet: decimal(10,2) (nullable = true)
         |    |-- priceGross: decimal(10,2) (nullable = true)
         |    |-- beginDate: date (nullable = true)
         |    |-- endDate: date (nullable = true)
         |    |-- isOptional: boolean (nullable = false)
         |    |-- manufacturerCode: string (nullable = true)
         |    |-- flagPack: short (nullable = true)
         |    |-- targetGroup: short (nullable = true)
         |    |-- taxRate: decimal(4,2) (nullable = true)
         |    |-- currency: string (nullable = true)
         |    |-- colors: array (nullable = true)
         |    |    |-- element: struct (containsNull = false)
         |    |    |    |-- orderCode: string (nullable = true)
         |    |    |    |-- basicColorName: string (nullable = true)
         |    |    |    |-- basicColorCode: short (nullable = true)
         |    |    |    |-- manufacturerColorName: string (nullable = true)
         |    |    |    |-- manufacturerColorType: short (nullable = true)
         |    |-- esacos: array (nullable = true)
         |    |    |-- element: struct (containsNull = false)
         |    |    |    |-- name: string (nullable = true)
         |    |    |    |-- mainGroup: string (nullable = true)
         |    |    |    |-- subGroup: string (nullable = true)
    '''

    features = (
        addition.join(manufactor, _MANUFACTOR.MANEQTEQCodeCd.fin_name, 'left')
        .join(color_block, _TYPECOL.TCLTypEqtCode.fin_name, 'left')
        .join(esaco_block, _ESAJOIN.ESJEQTEQCodeCd.fin_name, 'left')
        .select(
            _ADDITION.ADDVehType.fin_name,
            _ADDITION.ADDNatCode.fin_name,
            struct(
                _TYPECOL.TCLTypEqtCode.fin_name,
                _MANUFACTOR.MANEQTEQCodeCd.fin_name,
                _ADDITION.ADDPrice2.fin_name,
                _ADDITION.ADDPrice1.fin_name,
                _ADDITION.ADDVal.fin_name,
                _ADDITION.ADDValUntil.fin_name,
                _ADDITION.ADDFlag.fin_name,
                _MANUFACTOR.MANMCode.fin_name,
                _ADDITION.ADDFlagPack.fin_name,
                _ADDITION.ADDTargetGrp.fin_name,
                _ADDITION.ADDTaxRt.fin_name,
                _ADDITION.ADDCurrency.fin_name,
                'colors',
                'esacos',
            ).alias('features'),
        )
    )
    return features


def map_engine_block(type: DF, consumer: DF, typ_envkv: DF, technic: DF) -> DF:
    '''
    Function returns dataframe (will be added as 'engine' during fin_aggregation) with schema:
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

    key = [_TYPE.TYPVehType.fin_name, _TYPE.TYPNatCode.fin_name]
    m = 'left'

    engine = (
        type.join(technic, key, m)
        .join(typ_envkv, key, m)
        .join(consumer, key, m)
        .select(
            _TYPE.TYPVehType.fin_name,
            _TYPE.TYPNatCode.fin_name,
            struct(
                _TECHNIC.TECTXTEngTypeCd2.fin_name,
                _TYPE.TYPTXTFuelTypeCd2.fin_name,
                _TYPE.TYPCylinder.fin_name,
                _TYPE.TYPCapTech.fin_name,
                _CONSUMER.TCOCo2Emi.fin_name,
                _CONSUMER.TCOCo2EmiV2.fin_name,
                _TYPE.TYPTXTPollNormCd2.fin_name,
                _TYP_ENVKV.TENCo2EffClassCd2.fin_name,
                struct(_TYPE.TYPHP.fin_name, _TYPE.TYPKW.fin_name).alias('power'),
                struct(
                    _CONSUMER.TCOConsUrb.fin_name,
                    _CONSUMER.TCOConsLand.fin_name,
                    _CONSUMER.TCOConsTot.fin_name,
                    _CONSUMER.TCOConsUrbV2.fin_name,
                    _CONSUMER.TCOConsLandV2.fin_name,
                    _CONSUMER.TCOConsTotV2.fin_name,
                    _CONSUMER.TCOConsGasUrb.fin_name,
                    _CONSUMER.TCOConsGasLand.fin_name,
                    _CONSUMER.TCOConsGasTot.fin_name,
                    _CONSUMER.TCOTXTConsGasUnitCd.fin_name,
                    _CONSUMER.TCOConsPow.fin_name,
                    _CONSUMER.TCOBatCap.fin_name,
                ).alias('fuelConsumption'),
            ).alias('engine'),
        )
    )
    return engine


def map_price_block(price: DF) -> DF:
    '''
    Function returns dataframe (will be grouped and added as 'prices' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true)
         |-- NatCode: string (nullable = true)
         |-- prices: struct (nullable = false)
         |    |-- currency: string (nullable = true)
         |    |-- net: decimal(13,2) (nullable = true)
         |    |-- gross: decimal(13,2) (nullable = true)
         |    |-- taxRate: decimal(4,2) (nullable = true)
         |    |-- beginDate: date (nullable = true)
         |    |-- endDate: date (nullable = true)
    '''
    prices = price.select(
        _PRICE.PRHVehType.fin_name,
        _PRICE.PRHNatCode.fin_name,
        struct(
            _PRICE.PRHCurrency.fin_name,
            _PRICE.PRHNP2.fin_name,
            _PRICE.PRHNP1.fin_name,
            _PRICE.PRHTaxRt.fin_name,
            _PRICE.PRHVal.fin_name,
            _PRICE.PRHValUntil.fin_name,
        ).alias('prices'),
    )
    return prices


def map_tcert_block(tcert: DF) -> DF:
    '''
    Function returns dataframe (will be grouped and added as 'certifications' during fin_aggregation) with schema:
         |-- VehType: short (nullable = true)
         |-- NatCode: string (nullable = true)
         |-- certifications: struct (nullable = false)
         |    |-- hsn: string (nullable = true)
         |    |-- tsn: string (nullable = true)
    '''
    tcerts = tcert.select(
        _TCERT.TCEVehType.fin_name,
        _TCERT.TCENatCode.fin_name,
        struct(_TCERT.TCENum.fin_name, _TCERT.TCENum2.fin_name).alias('certifications'),
    )
    return tcerts


def fin_aggregation(
    type: DF, price_block: DF, tcert_block: DF, wheel_block: DF, model_block: DF, feature_block: DF, engine_block: DF,
) -> DF:
    '''
    Function returns resulting dataframe with schema:
         |-- schwackeCode: string (nullable = true) -> renamed from TYPE.TYPNatCode after joining
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
         |-- name: string (nullable = true)
         |-- name2: string (nullable = true)
         |-- bodyType: string (nullable = true)
         |-- driveType: string (nullable = true)
         |-- transmissionType: string (nullable = true)
         |-- productionBegin: date (nullable = true)
         |-- productionEnd: date (nullable = true)
         |-- prices: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- currency: string (nullable = true)
         |    |    |-- net: decimal(13,2) (nullable = true)
         |    |    |-- gross: decimal(13,2) (nullable = true)
         |    |    |-- taxRate: decimal(4,2) (nullable = true)
         |    |    |-- beginDate: date (nullable = true)
         |    |    |-- endDate: date (nullable = true)
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
         |-- doors: short (nullable = true)
         |-- seats: short (nullable = true)
         |-- weight: integer (nullable = true)
         |-- dimensions: struct (nullable = false)
         |    |-- length: integer (nullable = true)
         |    |-- width: short (nullable = true)
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
         |-- certifications: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- hsn: string (nullable = true)
         |    |    |-- tsn: string (nullable = true)
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
    '''

    unit_blocks = {'wheel': wheel_block, 'engine': engine_block}
    multiple_blocks = {'features': feature_block, 'prices': price_block, 'certifications': tcert_block}

    key = [_TYPE.TYPVehType.fin_name, _TYPE.TYPNatCode.fin_name]
    model_block_key = [_TYPE.TYPVehType.fin_name, _TYPE.TYPModCd.fin_name]
    m = 'left'

    final_table = type.join(model_block, model_block_key, m)

    for name, df in unit_blocks.items():
        final_table = final_table.join(df, key, m)

    for name, df in multiple_blocks.items():
        df = df.groupBy(key).agg(collect_set(name).alias(name))
        final_table = final_table.join(df, key, m)

    final_table = final_table.select(
        col(_TYPE.TYPNatCode.fin_name).alias('schwackeCode'),
        'model',
        _TYPE.TYPName.fin_name,
        _TYPE.TYPName2.fin_name,
        _TYPE.TYPTXTBodyCo1Cd2.fin_name,
        _TYPE.TYPTXTDriveTypeCd2.fin_name,
        _TYPE.TYPTXTTransTypeCd2.fin_name,
        _TYPE.TYPImpBegin.fin_name,
        _TYPE.TYPImpEnd.fin_name,
        'prices',
        'wheels',
        _TYPE.TYPDoor.fin_name,
        _TYPE.TYPSeat.fin_name,
        _TYPE.TYPTotWgt.fin_name,
        struct(_TYPE.TYPLength.fin_name, _TYPE.TYPWidth.fin_name).alias('dimensions'),
        'engine',
        'certifications',
        'features',
    )
    return final_table


def check_resulting_file(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(self: DataFrameWorker, *args: Union[Any]) -> Any:
        '''
        The function checks directory where resulting data json files were stored
        and chooses only json files to further processing
        '''
        for path in Path(
            Path.cwd(),
            'result',
            f'{self.configuration.db_name}_{self.configuration.mode.value}_'
            f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
        ).iterdir():
            if re.match(r'.*(.json)$', str(path)) and path.stat().st_size != 0:
                with Path(path).open('r') as file_result:
                    result = func(self, file_result, *args)
        return result

    return wrapper


@dataclass
class DataFrameWorker:
    '''class to create dataframe from source data,
    write it into files and database'''

    table: DF
    configuration: Configuration

    @classmethod
    def create_short_tmp_table(cls: Type, spark: SparkSession, configuration: Configuration) -> DataFrameWorker:
        '''
        The function gets required information from several hive-tables,
        aggregates it according to short mode and saves like pyspark dataframe object.
        '''

        tables = ["short_tmp_table", "txttable"]

        df_list = {}
        for table in tables:
            sql = SQL(configuration, f'{table}_select.txt')
            sql.update_sql()
            df_list[table] = sql.run_sql(spark)

        tmp_table = df_list["short_tmp_table"]

        for replaced_column in [
            'bodyType',
            'driveType',
            'transmissionType',
        ]:
            tmp_table = replace_from_txttable(tmp_table, df_list["txttable"], replaced_column)

        tmp_table = tmp_table.drop_duplicates()
        return cls(table=tmp_table, configuration=configuration)

    @classmethod
    def create_full_tmp_table(cls: Type, df_list: Dict[str, DF], configuration: Configuration) -> DataFrameWorker:
        '''
        The function gets list of required dataframe,
         aggregates it according to full mode and saves like pyspark dataframe object.
         '''

        wheel_block = map_wheel_block(
            jwheel=df_list[dsc.JWHEEL.name], tyres=df_list[dsc.TYRES.name], rims=df_list[dsc.RIMS.name]
        )

        model_block = map_model_block(model=df_list[dsc.MODEL.name], make=df_list[dsc.MAKE.name])

        esaco_block = map_esaco_block(esajoin=df_list[dsc.ESAJOIN.name], esaco=df_list[dsc.ESACO.name])

        color_block = map_color_block(
            typecol=df_list[dsc.TYPECOL.name], manucol=df_list[dsc.MANUCOL.name], eurocol=df_list[dsc.EUROCOL.name]
        )

        feature_block = map_feature_block(
            addition=df_list[dsc.ADDITION.name],
            color_block=color_block,
            manufactor=df_list[dsc.MANUFACTOR.name],
            esaco_block=esaco_block,
        )

        engine_block = map_engine_block(
            type=df_list[dsc.TYPE.name],
            consumer=df_list[dsc.CONSUMER.name],
            typ_envkv=df_list[dsc.TYP_ENVKV.name],
            technic=df_list[dsc.TECHNIC.name],
        )

        price_block = map_price_block(price=df_list[dsc.PRICEHISTORY.name])

        tcert_block = map_tcert_block(tcert=df_list[dsc.TCERT.name])

        tmp_table = fin_aggregation(
            type=df_list[dsc.TYPE.name],
            price_block=price_block,
            tcert_block=tcert_block,
            wheel_block=wheel_block,
            model_block=model_block,
            feature_block=feature_block,
            engine_block=engine_block,
        )

        return cls(table=tmp_table, configuration=configuration)

    def write_to_file(self) -> None:
        '''The function creates folder with one json
        file with all rows from source table via one partition'''
        result_table_folder = Path(Path.cwd(), 'result')
        result_table_folder.mkdir(parents=True, exist_ok=True)
        self.table.write.json(
            str(
                Path(
                    result_table_folder,
                    f'{self.configuration.db_name}_{self.configuration.mode.value}_'
                    f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
                )
            )
        )

    @check_resulting_file
    def read_from_file(self, file_result: List[Union[str, bytes, bytearray]], result: List[Dict]) -> List[Dict]:
        '''
        The function reads json files with results per entry
        and returns list of dictionaries
        '''
        for row in file_result:
            result.append(loads(row))
        return result
