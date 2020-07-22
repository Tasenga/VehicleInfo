from __future__ import annotations
from typing import Optional, Any
from dataclasses import dataclass
from enum import Enum


@dataclass
class ChangeType(Enum):
    date = 'date'
    integer = 'integer'
    boolean = 'boolean'
    new_column = 'new_column'


@dataclass
class Table:
    '''
    add_filter should be string with name of column from hive table
    (in the configuration for processing should be the same name)
    example 'JWHisStd'
    '''

    name: str
    table: Any[dataclass()]
    add_filter: Optional[str] = None


@dataclass
class Column:
    old_name: str
    fin_name: str
    replace_type: Optional[ChangeType] = None


@dataclass
class Addition:
    ADDVehType: Column = Column('ADDVehType', 'VehType')
    ADDNatCode: Column = Column('ADDNatCode', 'NatCode')
    ADDID: Column = Column('ADDID', 'id')
    ADDEQCode: Column = Column('ADDEQCode', 'code')
    ADDVal: Column = Column('ADDVal', 'beginDate', ChangeType.date)  # TO_DATE(ADDVal, 'yyyyMMdd')
    ADDValUntil: Column = Column('ADDValUntil', 'endDate', ChangeType.date)  # TO_DATE(ADDValUntil, 'yyyyMMdd')
    ADDPrice1: Column = Column('ADDPrice1', 'priceGross')
    ADDPrice2: Column = Column('ADDPrice2', 'priceNet')
    ADDFlag: Column = Column('ADDFlag', 'isOptional', ChangeType.boolean)
    ADDFlagPack: Column = Column('ADDFlagPack', 'flagPack')
    ADDTargetGrp: Column = Column('ADDTargetGrp', 'targetGroup')
    ADDTaxRt: Column = Column('ADDTaxRt', 'taxRate')
    ADDCurrency: Column = Column('ADDCurrency', 'currency')


@dataclass
class Consumer:
    TCOVehType: Column = Column('TCOVehType', 'VehType')
    TCONatCode: Column = Column('TCONatCode', 'NatCode')
    TCOCo2Emi: Column = Column('TCOCo2Emi', 'co2Emission')
    TCOCo2EmiV2: Column = Column('TCOCo2EmiV2', 'co2Emission2')
    TCOConsUrb: Column = Column('TCOConsUrb', 'urban')
    TCOConsLand: Column = Column('TCOConsLand', 'extraUrban')
    TCOConsTot: Column = Column('TCOConsTot', 'combined')
    TCOConsUrbV2: Column = Column('TCOConsUrbV2', 'urban2')
    TCOConsLandV2: Column = Column('TCOConsLandV2', 'extraUrban2')
    TCOConsTotV2: Column = Column('TCOConsTotV2', 'combined2')
    TCOConsGasUrb: Column = Column('TCOConsGasUrb', 'gasUrban')
    TCOConsGasLand: Column = Column('TCOConsGasLand', 'gasExtraUrban')
    TCOConsGasTot: Column = Column('TCOConsGasTot', 'gasCombined')
    TCOTXTConsGasUnitCd: Column = Column('TCOTXTConsGasUnitCd', 'gasUnit')
    TCOConsPow: Column = Column('TCOConsPow', 'power')
    TCOBatCap: Column = Column('TCOBatCap', 'batteryCapacity')


@dataclass
class Esaco:
    ESGTXTCodeCd2: Column = Column('ESGTXTCodeCd2', 'id')
    ESGTXTMainGrpCd2: Column = Column('ESGTXTMainGrpCd2', 'mainGroup')
    ESGTXTSubGrpCd2: Column = Column('ESGTXTSubGrpCd2', 'subGroup')


@dataclass
class Esajoin:
    ESJEQTEQCodeCd: Column = Column('ESJEQTEQCodeCd', 'code')
    ESJTXTESACOCd2: Column = Column('ESJTXTESACOCd2', 'id')


@dataclass
class Eurocol:
    ECLColID: Column = Column('ECLColID', 'basicColorCode')
    ECLColName: Column = Column('ECLColName', 'basicColorName')


@dataclass
class Jwheel:
    JWHVehType: Column = Column('JWHVehType', 'VehType')
    JWHNatCode: Column = Column('JWHNatCode', 'NatCode')
    JWHTYRTyreFCd: Column = Column('JWHTYRTyreFCd', 'JWHTYRTyreFCd')
    JWHTYRTyreRCd: Column = Column('JWHTYRTyreRCd', 'JWHTYRTyreRCd')
    JWHRIMRimFCd: Column = Column('JWHRIMRimFCd', 'JWHRIMRimFCd')
    JWHRIMRimRCd: Column = Column('JWHRIMRimRCd', 'JWHRIMRimRCd')


@dataclass
class Make:
    MAKVehType: Column = Column('MAKVehType', 'VehType')
    MAKNatCode: Column = Column('MAKNatCode', 'schwackeCode')
    MAKName: Column = Column('MAKName', 'name')


@dataclass
class Manucol:
    MCLManColCode: Column = Column('MCLManColCode', 'TCLMCLColCd')
    MCLECLColCd: Column = Column('MCLECLColCd', 'basicColorCode')
    MCLColName: Column = Column('MCLColName', 'manufacturerColorName')
    MCLPaintTrimFlag: Column = Column('MCLPaintTrimFlag', 'manufacturerColorType')


@dataclass
class Manufactor:
    MANEQTEQCodeCd: Column = Column('MANEQTEQCodeCd', 'code')
    MANMCode: Column = Column('MANMCode', 'manufacturerCode')


@dataclass
class Model:
    MODVehType: Column = Column('MODVehType', 'VehType', ChangeType.new_column)
    MODNatCode: Column = Column('MODNatCode', 'TYPModCd')
    MODMakCD: Column = Column('MODMakCD', 'schwackeCode')
    MODName: Column = Column('MODName', 'model_name')
    MODName2: Column = Column('MODName2', 'name2')
    MODModelSerCode: Column = Column('MODModelSerCode', 'serialCode')
    MODBegin: Column = Column('MODBegin', 'yearBegin', ChangeType.integer)  # CAST(MODBegin, as INT)
    MODEnd: Column = Column('MODEnd', 'yearEnd', ChangeType.integer)  # CAST(MODEnd, as INT)
    MODImpBegin: Column = Column('MODImpBegin', 'productionBegin', ChangeType.date)  # TO_DATE(MODImpBegin, 'yyyyMM')
    MODImpEnd: Column = Column('MODImpEnd', 'productionEnd', ChangeType.date)  # TO_DATE(MODImpEnd, 'yyyyMM')


@dataclass
class Pricehistory:
    PRHVehType: Column = Column('PRHVehType', 'VehType')
    PRHNatCode: Column = Column('PRHNatCode', 'NatCode')
    PRHCurrency: Column = Column('PRHCurrency', 'currency')
    PRHNP1: Column = Column('PRHNP1', 'gross')
    PRHNP2: Column = Column('PRHNP2', 'net')
    PRHTaxRt: Column = Column('PRHTaxRt', 'taxRate')
    PRHVal: Column = Column('PRHVal', 'beginDate', ChangeType.date)  # TO_DATE(PRHVal, 'yyyyMMdd')
    PRHValUntil: Column = Column('PRHValUntil', 'endDate', ChangeType.date)  # TO_DATE(PRHValUntil, 'yyyyMMdd')


@dataclass
class Rims:
    RIMVehType: Column = Column('RIMVehType', 'VehType')
    JWHRIMRimFCd: Column = Column('RIMID', 'JWHRIMRimFCd')
    JWHRIMRimRCd: Column = Column('RIMID', 'JWHRIMRimRCd')
    RIMWidth: Column = Column('RIMWidth', 'rimWidth')
    RIMDiameter: Column = Column('RIMDiameter', 'diameter')


@dataclass
class Tcert:
    TCEVehType: Column = Column('TCEVehType', 'VehType')
    TCENatCode: Column = Column('TCENatCode', 'NatCode')
    TCENum: Column = Column('TCENum', 'hsn')
    TCENum2: Column = Column('TCENum2', 'tsn')


@dataclass
class Technic:
    TECVehType: Column = Column('TECVehType', 'VehType')
    TECNatCode: Column = Column('TECNatCode', 'NatCode')
    TECTXTEngTypeCd2: Column = Column('TECTXTEngTypeCd2', 'engineType')


@dataclass
class Txttable:
    TXTCode: Column = Column('TXTCode', 'TXTCode')
    TXTTextLong: Column = Column('TXTTextLong', 'TXTTextLong')


@dataclass
class Typ_envkv:
    TENVehType: Column = Column('TENVehType', 'VehType')
    TENNatCode: Column = Column('TENNatCode', 'NatCode')
    TENCo2EffClassCd2: Column = Column('TENCo2EffClassCd2', 'energyEfficiencyClass')


@dataclass
class Type:
    TYPVehType: Column = Column('TYPVehType', 'VehType')
    TYPNatCode: Column = Column('TYPNatCode', 'NatCode')
    TYPModCd: Column = Column('TYPModCd', 'TYPModCd')
    TYPName: Column = Column('TYPName', 'name')
    TYPName2: Column = Column('TYPName2', 'name2')
    TYPTXTFuelTypeCd2: Column = Column('TYPTXTFuelTypeCd2', 'fuelType')
    TYPTXTDriveTypeCd2: Column = Column('TYPTXTDriveTypeCd2', 'driveType')
    TYPTXTTransTypeCd2: Column = Column('TYPTXTTransTypeCd2', 'transmissionType')
    TYPTXTBodyCo1Cd2: Column = Column('TYPTXTBodyCo1Cd2', 'bodyType')
    TYPDoor: Column = Column('TYPDoor', 'doors')
    TYPSeat: Column = Column('TYPSeat', 'seats')
    TYPImpBegin: Column = Column('TYPImpBegin', 'productionBegin', ChangeType.date)  # TO_DATE(TYPImpBegin, 'yyyyMM')
    TYPImpEnd: Column = Column('TYPImpEnd', 'productionEnd', ChangeType.date)  # TO_DATE(TYPImpEnd, 'yyyyMM')
    TYPKW: Column = Column('TYPKW', 'kw')
    TYPHP: Column = Column('TYPHP', 'ps')
    TYPCapTech: Column = Column('TYPCapTech', 'displacement')
    TYPCylinder: Column = Column('TYPCylinder', 'cylinders')
    TYPTXTPollNormCd2: Column = Column('TYPTXTPollNormCd2', 'emissionStandard')
    TYPTotWgt: Column = Column('TYPTotWgt', 'weight')
    TYPLength: Column = Column('TYPLength', 'length')
    TYPWidth: Column = Column('TYPWidth', 'width')


@dataclass
class Typecol:
    TCLTypEqtCode: Column = Column('TCLTypEqtCode', 'id')
    TCLMCLColCd: Column = Column('TCLMCLColCd', 'TCLMCLColCd')
    TCLOrdCd: Column = Column('TCLOrdCd', 'orderCode')


@dataclass
class Tyres:
    TYRVehType: Column = Column('TYRVehType', 'VehType')
    JWHTYRTyreFCd: Column = Column('TYRID', 'JWHTYRTyreFCd')
    JWHTYRTyreRCd: Column = Column('TYRID', 'JWHTYRTyreRCd')
    TYRWidth: Column = Column('TYRWidth', 'width')
    TYRCrossSec: Column = Column('TYRCrossSec', 'aspectRatio')
    TYRDesign: Column = Column('TYRDesign', 'construction')


ADDITION = Table('addition', Addition())
CONSUMER = Table('consumer', Consumer())
ESACO = Table('esaco', Esaco())
ESAJOIN = Table('esajoin', Esajoin())
EUROCOL = Table('eurocol', Eurocol())
JWHEEL = Table('jwheel', Jwheel(), 'JWHisStd')
MAKE = Table('make', Make())
MANUCOL = Table('manucol', Manucol())
MANUFACTOR = Table('manufactor', Manufactor())
MODEL = Table('model', Model())
PRICEHISTORY = Table('pricehistory', Pricehistory())
RIMS = Table('rims', Rims())
TCERT = Table('tcert', Tcert())
TECHNIC = Table('technic', Technic())
TXTTABLE = Table('txttable', Txttable())
TYP_ENVKV = Table('typ_envkv', Typ_envkv(), 'TENINFOTYPE')
TYPE = Table('type', Type())
TYPECOL = Table('typecol', Typecol())
TYRES = Table('tyres', Tyres())

TABLES = [
    ADDITION,
    CONSUMER,
    ESACO,
    ESAJOIN,
    EUROCOL,
    JWHEEL,
    MAKE,
    MANUCOL,
    MANUFACTOR,
    MODEL,
    PRICEHISTORY,
    RIMS,
    TCERT,
    TECHNIC,
    TXTTABLE,
    TYP_ENVKV,
    TYPE,
    TYPECOL,
    TYRES,
]
