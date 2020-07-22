# from __future__ import annotations
# import logging
# from pathlib import Path
# from typing import Type, List, Any, Callable, Dict, Union
# from dataclasses import dataclass
# from json import loads
# import re
#
# from pyspark.sql import SparkSession
# from pyspark.sql.dataframe import DataFrame as DF
# from pyspark.sql.functions import struct, col, when, collect_set, to_date
# from pyspark.sql.types import IntegerType
#
# import vis.descriptor as dsc
# from vis.configuration import Configuration
# from vis.sql_processing import SQL
# from vis.tablespreparator import Tables
#
#
# _LOGGER = logging.getLogger(__name__)
# _LOGGER.setLevel(logging.DEBUG)
#
#
# def type_replace(main_df: DF, type_df: DF, replaced_column: str) -> DF:
#     '''Function adds vehicle type information
#     from the type table to the preliminary temporary table
#     if the type code from both tables the same.'''
#     return (
#         main_df.join(type_df, main_df[replaced_column] == type_df[dsc.TXTTABLE.TXTCode.fin_name], 'left')
#         .drop(replaced_column, dsc.TXTTABLE.TXTCode.fin_name)
#         .withColumnRenamed(dsc.TXTTABLE.TXTTextLong.fin_name, replaced_column)
#     )
#
#
# def map_wheel_block(jwheel: DF, tyres: DF, rims: DF) -> DF:
#     '''
#     Function returns dataframe (will add as 'wheels' during fin_aggregation) with schema:
#          |-- VehType: short (nullable = true)
#          |-- NatCode: string (nullable = true)
#          |-- wheels: struct (nullable = false)
#          |    |-- front: struct (nullable = false)
#          |    |    |-- rimWidth: string (nullable = true)
#          |    |    |-- diameter: string (nullable = true)
#          |    |    |-- tyre: struct (nullable = false)
#          |    |    |    |-- width: string (nullable = true)
#          |    |    |    |-- aspectRatio: string (nullable = true)
#          |    |    |    |-- construction: string (nullable = true)
#          |    |-- rear: struct (nullable = false)
#          |    |    |-- rimWidth: string (nullable = true)
#          |    |    |-- diameter: string (nullable = true)
#          |    |    |-- tyre: struct (nullable = false)
#          |    |    |    |-- width: string (nullable = true)
#          |    |    |    |-- aspectRatio: string (nullable = true)
#          |    |    |    |-- construction: string (nullable = true)
#     '''
#
#     tyres_key = [dsc.TYRES.TYRVehType.fin_name, dsc.TYRES.JWHTYRTyreFCd.fin_name, dsc.TYRES.JWHTYRTyreRCd.fin_name]
#     rims_key = [dsc.RIMS.RIMVehType.fin_name, dsc.RIMS.JWHRIMRimFCd.fin_name, dsc.RIMS.JWHRIMRimRCd.fin_name]
#
#     tyres_col = [dsc.TYRES.TYRWidth.fin_name, dsc.TYRES.TYRCrossSec.fin_name, dsc.TYRES.TYRDesign.fin_name]
#     rims_col = [dsc.RIMS.RIMWidth.fin_name, dsc.RIMS.RIMDiameter.fin_name, struct(tyres_col).alias('tyre')]
#
#     wheels = (
#         jwheel.join(tyres, tyres_key, 'left')
#         .join(rims, rims_key, 'left')
#         .select(
#             dsc.JWHEEL.JWHVehType.fin_name,
#             dsc.JWHEEL.JWHNatCode.fin_name,
#             struct(struct(rims_col).alias('front'), struct(rims_col).alias('rear'),).alias('wheels'),
#         )
#     )
#     return wheels
#
#
# def map_model_block(model: DF, make: DF) -> DF:
#     '''
#     Function returns dataframe (will add as 'model' during fin_aggregation) with schema:
#         |-- VehType: short (nullable = true)
#         |-- TYPModCd: integer (nullable = true)
#         |-- model: struct (nullable = false)
#         |    |-- schwackeCode: integer (nullable = true) -> renamed from MODEL.MODNatCode after joining
#         |    |-- name: string (nullable = true)
#         |    |-- name2: string (nullable = true)
#         |    |-- serialCode: string (nullable = true)
#         |    |-- yearBegin: integer (nullable = true)
#         |    |-- yearEnd: integer (nullable = true)
#         |    |-- productionBegin: date (nullable = true) -> cast to_date(MODEL.MODImpBegin, 'yyyyMM')
#         |    |-- productionEnd: date (nullable = true) -> cast to_date(MODEL.MODImpEnd, 'yyyyMM')
#         |    |-- vehicleClass: string (nullable = true) -> new column from MODEL.MODVehType by dict
#                                                             {10: "Personenwagen", 20: "Transporter", etc.}
#         |    |-- make: struct (nullable = false)
#         |    |    |-- schwackeCode: integer (nullable = true)
#         |    |    |-- name: string (nullable = true)
#     '''
#
#     model = (
#         model.withColumn(dsc.MODEL.MODImpBegin.fin_name, to_date(dsc.MODEL.MODImpBegin.fin_name, 'yyyyMM'))
#         .withColumn(dsc.MODEL.MODImpEnd.fin_name, to_date(dsc.MODEL.MODImpEnd.fin_name, 'yyyyMM'))
#         .withColumn(dsc.MODEL.MODBegin.fin_name, model[dsc.MODEL.MODBegin.fin_name].cast(IntegerType()))
#         .withColumn(dsc.MODEL.MODEnd.fin_name, model[dsc.MODEL.MODEnd.fin_name].cast(IntegerType()))
#         .withColumn(
#             'vehicleClass',
#             when(model[dsc.MODEL.MODVehType.fin_name] == 10, "Personenwagen")
#             .when(model[dsc.MODEL.MODVehType.fin_name] == 20, "Transporter")
#             .when(model[dsc.MODEL.MODVehType.fin_name] == 30, "Zweirad")
#             .when(model[dsc.MODEL.MODVehType.fin_name] == 40, "Gelandewagen")
#             .otherwise(None),
#         )
#     )
#
#     make_key = [dsc.MAKE.MAKVehType.fin_name, dsc.MAKE.MAKNatCode.fin_name]
#     model = model.join(make, make_key, 'left')
#
#     model = model.select(
#         dsc.MODEL.MODVehType.fin_name,
#         dsc.MODEL.MODNatCode.fin_name,
#         struct(
#             model[dsc.MODEL.MODNatCode.fin_name].alias('schwackeCode'),
#             model[dsc.MODEL.MODName.fin_name].alias('name'),
#             dsc.MODEL.MODName2.fin_name,
#             dsc.MODEL.MODModelSerCode.fin_name,
#             dsc.MODEL.MODBegin.fin_name,
#             dsc.MODEL.MODEnd.fin_name,
#             dsc.MODEL.MODImpBegin.fin_name,
#             dsc.MODEL.MODImpEnd.fin_name,
#             'vehicleClass',
#             struct(model[dsc.MAKE.MAKNatCode.fin_name], make[dsc.MAKE.MAKName.fin_name]).alias('make'),
#         ).alias('model'),
#     ).drop('schwackeCode')
#     return model
#
#
# def map_esaco_block(esajoin: DF, esaco: DF, txttable: DF) -> DF:
#     '''
#     Function returns dataframe (will add as 'esacos' during map_feature_block) with schema:
#          |-- code: integer (nullable = true)
#          |-- esacos: array (nullable = true)
#          |    |-- element: struct (containsNull = false)
#          |    |    |-- name: string (nullable = true) -> new column by renamed from ESACO.ESGTXTCodeCd2,
#                                                          replaced with data from txttable with type_replace()
#          |    |    |-- mainGroup: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |    |    |-- subGroup: string (nullable = true) -> replaced with data from txttable with type_replace()
#     '''
#
#     esaco_col = [
#         'name',
#         dsc.ESACO.ESGTXTMainGrpCd2.fin_name,
#         dsc.ESACO.ESGTXTSubGrpCd2.fin_name,
#     ]
#     esaco = esaco.select(
#         dsc.ESACO.ESGTXTCodeCd2.fin_name,
#         esaco[dsc.ESACO.ESGTXTCodeCd2.fin_name].alias('name'),
#         dsc.ESACO.ESGTXTMainGrpCd2.fin_name,
#         dsc.ESACO.ESGTXTSubGrpCd2.fin_name,
#     )
#
#     for column in esaco_col:
#         esaco = type_replace(esaco, txttable, column)
#
#     esacos = (
#         esajoin.join(esaco, dsc.ESACO.ESGTXTCodeCd2.fin_name, 'left')
#         .select([dsc.ESAJOIN.ESJEQTEQCodeCd.fin_name, struct(esaco_col).alias('esacos')])
#         .groupBy(dsc.ESAJOIN.ESJEQTEQCodeCd.fin_name)
#         .agg(collect_set('esacos').alias('esacos'))
#     )
#     return esacos
#
#
# def map_color_block(typecol: DF, manucol: DF, eurocol: DF) -> DF:
#     '''
#     Function returns dataframe (will add as 'colors' during map_feature_block) with schema:
#          |-- id: integer (nullable = true)
#          |-- colors: array (nullable = true)
#          |    |-- element: struct (containsNull = false)
#          |    |    |-- orderCode: string (nullable = true)
#          |    |    |-- basicColorName: string (nullable = true)
#          |    |    |-- basicColorCode: short (nullable = true)
#          |    |    |-- manufacturerColorName: string (nullable = true)
#          |    |    |-- manufacturerColorType: short (nullable = true)
#     '''
#
#     colors = (
#         typecol.join(manucol, dsc.MANUCOL.MCLManColCode.fin_name, 'left')
#         .join(eurocol, dsc.EUROCOL.ECLColID.fin_name, 'left')
#         .select(
#             dsc.TYPECOL.TCLTypEqtCode.fin_name,
#             struct(
#                 dsc.TYPECOL.TCLOrdCd.fin_name,
#                 dsc.EUROCOL.ECLColName.fin_name,
#                 dsc.EUROCOL.ECLColID.fin_name,
#                 dsc.MANUCOL.MCLColName.fin_name,
#                 dsc.MANUCOL.MCLPaintTrimFlag.fin_name,
#             ).alias('colors'),
#         )
#         .groupBy(dsc.TYPECOL.TCLTypEqtCode.fin_name)
#         .agg(collect_set('colors').alias('colors'))
#     )
#     return colors
#
#
# def map_feature_block(addition: DF, color_block: DF, manufactor: DF, esaco_block: DF,) -> DF:
#     '''
#     Function returns dataframe (will add as 'features' during fin_aggregation) with schema:
#          |-- VehType: short (nullable = true)
#          |-- NatCode: string (nullable = true)
#          |-- features: struct (nullable = false)
#          |    |-- id: integer (nullable = true)
#          |    |-- code: integer (nullable = true)
#          |    |-- priceNet: decimal(10,2) (nullable = true)
#          |    |-- priceGross: decimal(10,2) (nullable = true)
#          |    |-- beginDate: date (nullable = true) -> cast to_date(ADDITION.ADDVal, 'yyyyMMdd')
#          |    |-- endDate: date (nullable = true) -> cast to_date(ADDITION.ADDValUntil, 'yyyyMMdd')
#          |    |-- isOptional: boolean (nullable = false) -> replaced data to boolean {'0': False, '1 or more': true}
#          |    |-- manufacturerCode: string (nullable = true)
#          |    |-- flagPack: short (nullable = true)
#          |    |-- targetGroup: short (nullable = true)
#          |    |-- taxRate: decimal(4,2) (nullable = true)
#          |    |-- currency: string (nullable = true)
#          |    |-- colors: array (nullable = true)
#          |    |    |-- element: struct (containsNull = false)
#          |    |    |    |-- orderCode: string (nullable = true)
#          |    |    |    |-- basicColorName: string (nullable = true)
#          |    |    |    |-- basicColorCode: short (nullable = true)
#          |    |    |    |-- manufacturerColorName: string (nullable = true)
#          |    |    |    |-- manufacturerColorType: short (nullable = true)
#          |    |-- esacos: array (nullable = true)
#          |    |    |-- element: struct (containsNull = false)
#          |    |    |    |-- name: string (nullable = true)
#          |    |    |    |-- mainGroup: string (nullable = true)
#          |    |    |    |-- subGroup: string (nullable = true)
#     '''
#
#     addition = addition.withColumn(
#         dsc.ADDITION.ADDVal.fin_name, to_date(addition[dsc.ADDITION.ADDVal.fin_name], 'yyyyMMdd')
#     ).withColumn(dsc.ADDITION.ADDValUntil.fin_name, to_date(addition[dsc.ADDITION.ADDValUntil.fin_name], 'yyyyMMdd'))
#
#     features = (
#         addition.join(manufactor, dsc.MANUFACTOR.MANEQTEQCodeCd.fin_name, 'left')
#         .join(color_block, dsc.TYPECOL.TCLTypEqtCode.fin_name, 'left')
#         .join(esaco_block, dsc.ESAJOIN.ESJEQTEQCodeCd.fin_name, 'left')
#         .select(
#             dsc.ADDITION.ADDVehType.fin_name,
#             dsc.ADDITION.ADDNatCode.fin_name,
#             struct(
#                 dsc.TYPECOL.TCLTypEqtCode.fin_name,
#                 dsc.MANUFACTOR.MANEQTEQCodeCd.fin_name,
#                 dsc.ADDITION.ADDPrice2.fin_name,
#                 dsc.ADDITION.ADDPrice1.fin_name,
#                 dsc.ADDITION.ADDVal.fin_name,
#                 dsc.ADDITION.ADDValUntil.fin_name,
#                 when(col(dsc.ADDITION.ADDFlag.fin_name) == 0, False)
#                 .otherwise(True)
#                 .alias(dsc.ADDITION.ADDFlag.fin_name),
#                 dsc.MANUFACTOR.MANMCode.fin_name,
#                 dsc.ADDITION.ADDFlagPack.fin_name,
#                 dsc.ADDITION.ADDTargetGrp.fin_name,
#                 dsc.ADDITION.ADDTaxRt.fin_name,
#                 dsc.ADDITION.ADDCurrency.fin_name,
#                 'colors',
#                 'esacos',
#             ).alias('features'),
#         )
#     )
#     return features
#
#
# def map_engine_block(type: DF, consumer: DF, typ_envkv: DF, technic: DF, txttable: DF) -> DF:
#     '''
#     Function returns dataframe (will add as 'engine' during fin_aggregation) with schema:
#          |-- VehType: short (nullable = true)
#          |-- NatCode: string (nullable = true)
#          |-- engine: struct (nullable = false)
#          |    |-- engineType: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |    |-- fuelType: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |    |-- cylinders: short (nullable = true)
#          |    |-- displacement: decimal(7,2) (nullable = true)
#          |    |-- co2Emission: short (nullable = true)
#          |    |-- co2Emission2: short (nullable = true)
#          |    |-- emissionStandard: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |    |-- energyEfficiencyClass: string (nullable = true)->replaced with data
#          from txttable with type_replace()
#          |    |-- power: struct (nullable = false)
#          |    |    |-- ps: decimal(6,2) (nullable = true)
#          |    |    |-- kw: decimal(6,2) (nullable = true)
#          |    |-- fuelConsumption: struct (nullable = false)
#          |    |    |-- urban: decimal(3,1) (nullable = true)
#          |    |    |-- extraUrban: decimal(3,1) (nullable = true)
#          |    |    |-- combined: decimal(3,1) (nullable = true)
#          |    |    |-- urban2: decimal(3,1) (nullable = true)
#          |    |    |-- extraUrban2: decimal(3,1) (nullable = true)
#          |    |    |-- combined2: decimal(3,1) (nullable = true)
#          |    |    |-- gasUrban: decimal(3,1) (nullable = true)
#          |    |    |-- gasExtraUrban: decimal(3,1) (nullable = true)
#          |    |    |-- gasCombined: decimal(3,1) (nullable = true)
#          |    |    |-- gasUnit: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |    |    |-- power: decimal(4,1) (nullable = true)
#          |    |    |-- batteryCapacity: short (nullable = true)
#     '''
#
#     for column in [dsc.TYPE.TYPTXTFuelTypeCd2.fin_name, dsc.TYPE.TYPTXTPollNormCd2.fin_name]:
#         type = type_replace(type, txttable, column)
#
#     technic = type_replace(technic, txttable, dsc.TECHNIC.TECTXTEngTypeCd2.fin_name)
#     typ_envkv = type_replace(typ_envkv, txttable, dsc.TYP_ENVKV.TENCo2EffClassCd2.fin_name)
#     consumer = type_replace(consumer, txttable, dsc.CONSUMER.TCOTXTConsGasUnitCd.fin_name)
#
#     key = [dsc.TYPE.TYPVehType.fin_name, dsc.TYPE.TYPNatCode.fin_name]
#     m = 'left'
#
#     engine = (
#         type.join(technic, key, m)
#         .join(typ_envkv, key, m)
#         .join(consumer, key, m)
#         .select(
#             dsc.TYPE.TYPVehType.fin_name,
#             dsc.TYPE.TYPNatCode.fin_name,
#             struct(
#                 dsc.TECHNIC.TECTXTEngTypeCd2.fin_name,
#                 dsc.TYPE.TYPTXTFuelTypeCd2.fin_name,
#                 dsc.TYPE.TYPCylinder.fin_name,
#                 dsc.TYPE.TYPCapTech.fin_name,
#                 dsc.CONSUMER.TCOCo2Emi.fin_name,
#                 dsc.CONSUMER.TCOCo2EmiV2.fin_name,
#                 dsc.TYPE.TYPTXTPollNormCd2.fin_name,
#                 dsc.TYP_ENVKV.TENCo2EffClassCd2.fin_name,
#                 struct(dsc.TYPE.TYPHP.fin_name, dsc.TYPE.TYPKW.fin_name).alias('power'),
#                 struct(
#                     dsc.CONSUMER.TCOConsUrb.fin_name,
#                     dsc.CONSUMER.TCOConsLand.fin_name,
#                     dsc.CONSUMER.TCOConsTot.fin_name,
#                     dsc.CONSUMER.TCOConsUrbV2.fin_name,
#                     dsc.CONSUMER.TCOConsLandV2.fin_name,
#                     dsc.CONSUMER.TCOConsTotV2.fin_name,
#                     dsc.CONSUMER.TCOConsGasUrb.fin_name,
#                     dsc.CONSUMER.TCOConsGasLand.fin_name,
#                     dsc.CONSUMER.TCOConsGasTot.fin_name,
#                     dsc.CONSUMER.TCOTXTConsGasUnitCd.fin_name,
#                     dsc.CONSUMER.TCOConsPow.fin_name,
#                     dsc.CONSUMER.TCOBatCap.fin_name,
#                 ).alias('fuelConsumption'),
#             ).alias('engine'),
#         )
#     )
#     return engine
#
#
# def fin_aggregation(type: DF,
#     prices: DF,
#     tcert: DF,
#     txttable: DF,
#     wheel_block: DF,
#     model_block: DF,
#     feature_block: DF,
#     engine_block: DF,
# ) -> DF:
#     '''
#     Function returns resulting dataframe with schema:
#          |-- schwackeCode: string (nullable = true) -> renamed from TYPE.TYPNatCode after joining
#          |-- model: struct (nullable = true)
#          |    |-- schwackeCode: integer (nullable = true)
#          |    |-- name: string (nullable = true)
#          |    |-- name2: string (nullable = true)
#          |    |-- serialCode: string (nullable = true)
#          |    |-- yearBegin: integer (nullable = true)
#          |    |-- yearEnd: integer (nullable = true)
#          |    |-- productionBegin: date (nullable = true)
#          |    |-- productionEnd: date (nullable = true)
#          |    |-- vehicleClass: string (nullable = true)
#          |    |-- make: struct (nullable = false)
#          |    |    |-- schwackeCode: integer (nullable = true)
#          |    |    |-- name: string (nullable = true)
#          |-- name: string (nullable = true)
#          |-- name2: string (nullable = true)
#          |-- bodyType: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |-- driveType: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |-- transmissionType: string (nullable = true) -> replaced with data from txttable with type_replace()
#          |-- productionBegin: date (nullable = true) -> cast to_date(TYPE.TYPImpBegin, 'yyyyMM')
#          |-- productionEnd: date (nullable = true) -> cast to_date(TYPE.TYPImpEnd, 'yyyyMM')
#          |-- prices: array (nullable = true)
#          |    |-- element: struct (containsNull = false)
#          |    |    |-- currency: string (nullable = true)
#          |    |    |-- net: decimal(13,2) (nullable = true)
#          |    |    |-- gross: decimal(13,2) (nullable = true)
#          |    |    |-- taxRate: decimal(4,2) (nullable = true)
#          |    |    |-- beginDate: date (nullable = true) -> cast to_date(PRICEHISTORY.PRHVal, 'yyyyMMdd')
#          |    |    |-- endDate: date (nullable = true) -> cast to_date(PRICEHISTORY.PRHVal, 'yyyyMMdd')
#          |-- wheels: struct (nullable = true)
#          |    |-- front: struct (nullable = false)
#          |    |    |-- rimWidth: string (nullable = true)
#          |    |    |-- diameter: string (nullable = true)
#          |    |    |-- tyre: struct (nullable = false)
#          |    |    |    |-- width: string (nullable = true)
#          |    |    |    |-- aspectRatio: string (nullable = true)
#          |    |    |    |-- construction: string (nullable = true)
#          |    |-- rear: struct (nullable = false)
#          |    |    |-- rimWidth: string (nullable = true)
#          |    |    |-- diameter: string (nullable = true)
#          |    |    |-- tyre: struct (nullable = false)
#          |    |    |    |-- width: string (nullable = true)
#          |    |    |    |-- aspectRatio: string (nullable = true)
#          |    |    |    |-- construction: string (nullable = true)
#          |-- doors: short (nullable = true)
#          |-- seats: short (nullable = true)
#          |-- weight: integer (nullable = true)
#          |-- dimensions: struct (nullable = false)
#          |    |-- length: integer (nullable = true)
#          |    |-- width: short (nullable = true)
#          |-- engine: struct (nullable = true)
#          |    |-- engineType: string (nullable = true)
#          |    |-- fuelType: string (nullable = true)
#          |    |-- cylinders: short (nullable = true)
#          |    |-- displacement: decimal(7,2) (nullable = true)
#          |    |-- co2Emission: short (nullable = true)
#          |    |-- co2Emission2: short (nullable = true)
#          |    |-- emissionStandard: string (nullable = true)
#          |    |-- energyEfficiencyClass: string (nullable = true)
#          |    |-- power: struct (nullable = false)
#          |    |    |-- ps: decimal(6,2) (nullable = true)
#          |    |    |-- kw: decimal(6,2) (nullable = true)
#          |    |-- fuelConsumption: struct (nullable = false)
#          |    |    |-- urban: decimal(3,1) (nullable = true)
#          |    |    |-- extraUrban: decimal(3,1) (nullable = true)
#          |    |    |-- combined: decimal(3,1) (nullable = true)
#          |    |    |-- urban2: decimal(3,1) (nullable = true)
#          |    |    |-- extraUrban2: decimal(3,1) (nullable = true)
#          |    |    |-- combined2: decimal(3,1) (nullable = true)
#          |    |    |-- gasUrban: decimal(3,1) (nullable = true)
#          |    |    |-- gasExtraUrban: decimal(3,1) (nullable = true)
#          |    |    |-- gasCombined: decimal(3,1) (nullable = true)
#          |    |    |-- gasUnit: string (nullable = true)
#          |    |    |-- power: decimal(4,1) (nullable = true)
#          |    |    |-- batteryCapacity: short (nullable = true)
#          |-- certifications: array (nullable = true)
#          |    |-- element: struct (containsNull = false)
#          |    |    |-- hsn: string (nullable = true)
#          |    |    |-- tsn: string (nullable = true)
#          |-- features: array (nullable = true)
#          |    |-- element: struct (containsNull = false)
#          |    |    |-- id: integer (nullable = true)
#          |    |    |-- code: integer (nullable = true)
#          |    |    |-- priceNet: decimal(10,2) (nullable = true)
#          |    |    |-- priceGross: decimal(10,2) (nullable = true)
#          |    |    |-- beginDate: date (nullable = true)
#          |    |    |-- endDate: date (nullable = true)
#          |    |    |-- col7: boolean (nullable = false)
#          |    |    |-- manufacturerCode: string (nullable = true)
#          |    |    |-- flagPack: short (nullable = true)
#          |    |    |-- targetGroup: short (nullable = true)
#          |    |    |-- taxRate: decimal(4,2) (nullable = true)
#          |    |    |-- currency: string (nullable = true)
#          |    |    |-- colors: array (nullable = true)
#          |    |    |    |-- element: struct (containsNull = false)
#          |    |    |    |    |-- orderCode: string (nullable = true)
#          |    |    |    |    |-- basicColorName: string (nullable = true)
#          |    |    |    |    |-- basicColorCode: short (nullable = true)
#          |    |    |    |    |-- manufacturerColorName: string (nullable = true)
#          |    |    |    |    |-- manufacturerColorType: short (nullable = true)
#          |    |    |-- esacos: array (nullable = true)
#          |    |    |    |-- element: struct (containsNull = false)
#          |    |    |    |    |-- name: string (nullable = true)
#          |    |    |    |    |-- mainGroup: string (nullable = true)
#          |    |    |    |    |-- subGroup: string (nullable = true)
#     '''
#
#     prices = prices.select(
#         dsc.PRICEHISTORY.PRHVehType.fin_name,
#         dsc.PRICEHISTORY.PRHNatCode.fin_name,
#         struct(
#             dsc.PRICEHISTORY.PRHCurrency.fin_name,
#             dsc.PRICEHISTORY.PRHNP2.fin_name,
#             dsc.PRICEHISTORY.PRHNP1.fin_name,
#             dsc.PRICEHISTORY.PRHTaxRt.fin_name,
#             to_date(prices[dsc.PRICEHISTORY.PRHVal.fin_name], 'yyyyMMdd').alias(dsc.PRICEHISTORY.PRHVal.fin_name),
#             to_date(prices[dsc.PRICEHISTORY.PRHValUntil.fin_name], 'yyyyMMdd').alias(
#                 dsc.PRICEHISTORY.PRHValUntil.fin_name
#             ),
#         ).alias('prices'),
#     )
#
#     tcert = tcert.select(
#         dsc.TCERT.TCEVehType.fin_name,
#         dsc.TCERT.TCENatCode.fin_name,
#         struct(dsc.TCERT.TCENum.fin_name, dsc.TCERT.TCENum2.fin_name).alias('certifications'),
#     )
#
#     variants = type.select(
#         dsc.TYPE.TYPVehType.fin_name,
#         dsc.TYPE.TYPNatCode.fin_name,
#         dsc.TYPE.TYPModCd.fin_name,
#         dsc.TYPE.TYPName.fin_name,
#         dsc.TYPE.TYPName2.fin_name,
#         dsc.TYPE.TYPTXTBodyCo1Cd2.fin_name,
#         dsc.TYPE.TYPTXTDriveTypeCd2.fin_name,
#         dsc.TYPE.TYPTXTTransTypeCd2.fin_name,
#         to_date(type[dsc.TYPE.TYPImpBegin.fin_name], 'yyyyMM').alias(dsc.TYPE.TYPImpBegin.fin_name),
#         to_date(type[dsc.TYPE.TYPImpEnd.fin_name], 'yyyyMM').alias(dsc.TYPE.TYPImpEnd.fin_name),
#         dsc.TYPE.TYPDoor.fin_name,
#         dsc.TYPE.TYPSeat.fin_name,
#         dsc.TYPE.TYPTotWgt.fin_name,
#         struct(dsc.TYPE.TYPLength.fin_name, dsc.TYPE.TYPWidth.fin_name).alias('dimensions'),
#     )
#     for column in [
#         dsc.TYPE.TYPTXTBodyCo1Cd2.fin_name,
#         dsc.TYPE.TYPTXTDriveTypeCd2.fin_name,
#         dsc.TYPE.TYPTXTTransTypeCd2.fin_name,
#     ]:
#         variants = type_replace(variants, txttable, column)
#
#     unit_blocks = {"wheel": wheel_block, 'engine': engine_block}
#     multiple_blocks = {'features': feature_block, "prices": prices, 'certifications': tcert}
#
#     key = [dsc.TYPE.TYPVehType.fin_name, dsc.TYPE.TYPNatCode.fin_name]
#     model_block_key = [dsc.TYPE.TYPVehType.fin_name, dsc.TYPE.TYPModCd.fin_name]
#     m = 'left'
#
#     final_table = variants.join(model_block, model_block_key, m)
#
#     for name, df in unit_blocks.items():
#         final_table = final_table.join(df, key, m)
#
#     for name, df in multiple_blocks.items():
#         df = df.groupBy(key).agg(collect_set(name).alias(name))
#         final_table = final_table.join(df, key, m)
#
#     final_table = final_table.withColumnRenamed(dsc.TYPE.TYPNatCode.fin_name, 'schwackeCode').drop(
#         dsc.TYPE.TYPVehType.fin_name, dsc.TYPE.TYPModCd.fin_name
#     )
#
#     final_table = final_table.select(
#         'schwackeCode',
#         'model',
#         dsc.TYPE.TYPName.fin_name,
#         dsc.TYPE.TYPName2.fin_name,
#         dsc.TYPE.TYPTXTBodyCo1Cd2.fin_name,
#         dsc.TYPE.TYPTXTDriveTypeCd2.fin_name,
#         dsc.TYPE.TYPTXTTransTypeCd2.fin_name,
#         dsc.TYPE.TYPImpBegin.fin_name,
#         dsc.TYPE.TYPImpEnd.fin_name,
#         'prices',
#         'wheels',
#         dsc.TYPE.TYPDoor.fin_name,
#         dsc.TYPE.TYPSeat.fin_name,
#         dsc.TYPE.TYPTotWgt.fin_name,
#         'dimensions',
#         'engine',
#         'certifications',
#         'features',
#     )
#     return final_table
#
#
# def check_resulting_file(func: Callable[..., Any]) -> Callable[..., Any]:
#     def wrapper(self: DataFrameWorker, *args: Union[Any]) -> Any:
#         '''
#         The function checks directory where resulting data json files were stored
#         and chooses only json files to further processing
#         '''
#         for path in Path(
#             Path.cwd(),
#             'result',
#             f'{self.configuration.db_name}_{self.configuration.mode.value}_'
#             f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
#         ).iterdir():
#             if re.match(r'.*(.json)$', str(path)) and path.stat().st_size != 0:
#                 with Path(path).open('r') as file_result:
#                     result = func(self, file_result, *args)
#         return result
#
#     return wrapper
#
#
# @dataclass
# class DataFrameWorker:
#     '''class to create dataframe from source data,
#     write it into files and database'''
#
#     table: DF
#     configuration: Configuration
#
#     @classmethod
#     def create_short_tmp_table(cls: Type, spark: SparkSession, configuration: Configuration) -> DataFrameWorker:
#         '''
#         The function gets required information from several hive-tables,
#         aggregates it according to short mode and saves like pyspark dataframe object.
#         '''
#
#         tables = ["short_tmp_table", "txttable"]
#
#         df_list = {}
#         for table in tables:
#             sql = SQL(configuration, f'{table}_select.txt')
#             sql.update_sql()
#             df_list[table] = sql.run_sql(spark)
#
#         tmp_table = df_list["short_tmp_table"]
#
#         for replaced_column in [
#             'bodyType',
#             'driveType',
#             'transmissionType',
#         ]:
#             tmp_table = type_replace(tmp_table, df_list["txttable"], replaced_column)
#
#         tmp_table = tmp_table.drop_duplicates()
#         return cls(table=tmp_table, configuration=configuration)
#
#     @classmethod
#     def create_full_tmp_table(cls: Type, df_list: Dict[str, DF], configuration: Configuration) -> DataFrameWorker:
#         '''
#         The function gets required information from several hive-tables,
#          aggregates it according to full mode and saves like pyspark dataframe object.
#          '''
#
#         wheel_block = map_wheel_block(jwheel=df_list["jwheel"], tyres=df_list["tyres"], rims=df_list["rims"])
#
#         model_block = map_model_block(model=df_list["model"], make=df_list["make"])
#
#         esaco_block = map_esaco_block(esajoin=df_list["esajoin"],
#         esaco=df_list["esaco"], txttable=df_list["txttable"])
#
#         color_block = map_color_block(
#             typecol=df_list["typecol"], manucol=df_list["manucol"], eurocol=df_list["eurocol"]
#         )
#
#         feature_block = map_feature_block(
#             addition=df_list["addition"],
#             color_block=color_block,
#             manufactor=df_list['manufactor'],
#             esaco_block=esaco_block,
#         )
#
#         engine_block = map_engine_block(
#             type=df_list["type"],
#             consumer=df_list["consumer"],
#             typ_envkv=df_list["typ_envkv"],
#             technic=df_list["technic"],
#             txttable=df_list["txttable"],
#         )
#
#         tmp_table = fin_aggregation(
#             type=df_list['type'],
#             prices=df_list['pricehistory'],
#             tcert=df_list['tcert'],
#             txttable=df_list["txttable"],
#             wheel_block=wheel_block,
#             model_block=model_block,
#             feature_block=feature_block,
#             engine_block=engine_block,
#         )
#
#         return cls(table=tmp_table, configuration=configuration)
#
#     def write_to_file(self) -> None:
#         '''The function creates folder with one json
#         file with all rows from source table via one partition'''
#         result_table_folder = Path(Path.cwd(), 'result')
#         result_table_folder.mkdir(parents=True, exist_ok=True)
#         self.table.write.json(
#             str(
#                 Path(
#                     result_table_folder,
#                     f'{self.configuration.db_name}_{self.configuration.mode.value}_'
#                     f'{self.configuration.current_date}_{self.configuration.current_timestamp}',
#                 )
#             )
#         )
#
#     @check_resulting_file
#     def read_from_file(self, file_result: List[Union[str, bytes, bytearray]], result: List[Dict]) -> List[Dict]:
#         '''
#         The function reads json files with results per entry
#         and returns list of dictionaries
#         '''
#         for row in file_result:
#             result.append(loads(row))
#         return result
