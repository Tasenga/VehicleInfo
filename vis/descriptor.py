from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Column:
    old_name: str
    fin_name: str


@dataclass
class Addition:
    ADDVehType: Column = Column('ADDVehType', 'VehType')
    ADDNatCode: Column = Column('ADDNatCode', 'NatCode')
    ADDID: Column = Column('ADDID', 'id')
    ADDEQCode: Column = Column('ADDEQCode', 'code')
    ADDVal: Column = Column('ADDVal', 'beginDate')  # TO_DATE(ADDVal, 'yyyyMMdd')
    ADDValUntil: Column = Column('ADDValUntil', 'endDate')  # TO_DATE(ADDValUntil, 'yyyyMMdd')
    ADDPrice1: Column = Column('ADDPrice1', 'priceGross')
    ADDPrice2: Column = Column('ADDPrice2', 'priceNet')
    ADDFlag: Column = Column('ADDFlag', 'isOptional')
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
    MODVehType: Column = Column('MODVehType', 'VehType')
    MODNatCode: Column = Column('MODNatCode', 'TYPModCd')
    MODMakCD: Column = Column('MODMakCD', 'schwackeCode')
    MODName: Column = Column('MODName', 'model_name')
    MODName2: Column = Column('MODName2', 'name2')
    MODModelSerCode: Column = Column('MODModelSerCode', 'serialCode')
    MODBegin: Column = Column('MODBegin', 'yearBegin')  # CAST(MODBegin, as INT)
    MODEnd: Column = Column('MODEnd', 'yearEnd')  # CAST(MODEnd, as INT)
    MODImpBegin: Column = Column('MODImpBegin', 'productionBegin')  # TO_DATE(MODImpBegin, 'yyyyMMdd')
    MODImpEnd: Column = Column('MODImpEnd', 'productionEnd')  # TO_DATE(MODImpEnd, 'yyyyMMdd')


@dataclass
class Pricehistory:
    PRHVehType: Column = Column('PRHVehType', 'VehType')
    PRHNatCode: Column = Column('PRHNatCode', 'NatCode')
    PRHCurrency: Column = Column('PRHCurrency', 'currency')
    PRHNP1: Column = Column('PRHNP1', 'gross')
    PRHNP2: Column = Column('PRHNP2', 'net')
    PRHTaxRt: Column = Column('PRHTaxRt', 'taxRate')
    PRHVal: Column = Column('PRHVal', 'beginDate')  # TO_DATE(PRHVal, 'yyyyMMdd')
    PRHValUntil: Column = Column('PRHValUntil', 'endDate')  # TO_DATE(PRHValUntil, 'yyyyMMdd')


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
    TYPImpBegin: Column = Column('TYPImpBegin', 'productionBegin')  # TO_DATE(TYPImpBegin, 'yyyyMM')
    TYPImpEnd: Column = Column('TYPImpEnd', 'productionEnd')  # TO_DATE(TYPImpEnd, 'yyyyMM')
    TYPKW: Column = Column('TYPKW', 'kw')
    TYPHP: Column = Column('TYPHP', 'ps')
    TYPCapTech: Column = Column('TYPCapTech', 'displacement')
    TYPCylinder: Column = Column('TYPCylinder', 'cylinders')
    TYPTXTPollNormCd2: Column = Column('TYPTXTPollNormCd2', 'emissionStandard')
    TYPTotWgt: Column = Column('TYPTotWgt', 'weight')
    TYPLength: Column = Column('TYPLength', 'lenght')
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


ADDITION = Addition()
CONSUMER = Consumer()
ESACO = Esaco()
ESAJOIN = Esajoin()
EUROCOL = Eurocol()
JWHEEL = Jwheel()
MAKE = Make()
MANUCOL = Manucol()
MANUFACTOR = Manufactor()
MODEL = Model()
PRICEHISTORY = Pricehistory()
RIMS = Rims()
TCERT = Tcert()
TECHNIC = Technic()
TXTTABLE = Txttable()
TYP_ENVKV = Typ_envkv()
TYPE = Type()
TYPECOL = Typecol()
TYRES = Tyres()


# ADDVehType = Column('ADDVehType', 'VehType')
# ADDNatCode = Column('ADDNatCode', 'NatCode')
# ADDID = Column('ADDID', 'id')
# ADDEQCode = Column('ADDEQCode', 'code')
# ADDVal = Column('ADDVal', 'beginDate') #TO_DATE(ADDVal, 'yyyyMMdd')
# ADDValUntil = Column('ADDValUntil', 'endDate') #TO_DATE(ADDValUntil, 'yyyyMMdd')
# ADDPrice1 = Column('ADDPrice1', 'priceGross')
# ADDPrice2 = Column('ADDPrice2', 'priceNet')
# ADDFlag = Column('ADDFlag', 'isOptional')
# ADDFlagPack = Column('ADDFlagPack', 'flagPack')
# ADDTargetGrp = Column('ADDTargetGrp', 'targetGroup')
# ADDTaxRt = Column('ADDTaxRt', 'taxRate')
# ADDCurrency = Column('ADDCurrency', 'currency')
#
# ADDITION = [
#     ADDVehType,
#     ADDNatCode,
#     ADDID,
#     ADDEQCode,
#     ADDVal,
#     ADDValUntil,
#     ADDPrice1,
#     ADDPrice2,
#     ADDFlag,
#     ADDFlagPack,
#     ADDTargetGrp,
#     ADDTaxRt,
#     ADDCurrency
# ]
#
# TCOVehType = Column('TCOVehType', 'VehType')
# TCONatCode = Column('TCONatCode', 'NatCode')
# TCOCo2Emi = Column('TCOCo2Emi', 'co2Emission')
# TCOCo2EmiV2 = Column('TCOCo2EmiV2', 'co2Emission2')
# TCOConsUrb = Column('TCOConsUrb', 'urban')
# TCOConsLand = Column('TCOConsLand', 'extraUrban')
# TCOConsTot = Column('TCOConsTot', 'combined')
# TCOConsUrbV2 = Column('TCOConsUrbV2', 'urban2')
# TCOConsLandV2 = Column('TCOConsLandV2', 'extraUrban2')
# TCOConsTotV2 = Column('TCOConsTotV2', 'combined2')
# TCOConsGasUrb = Column('TCOConsGasUrb', 'gasUrban')
# TCOConsGasLand = Column('TCOConsGasLand', 'gasExtraUrban')
# TCOConsGasTot = Column('TCOConsGasTot', 'gasCombined')
# TCOTXTConsGasUnitCd = Column('TCOTXTConsGasUnitCd', 'gasUnit')
# TCOConsPow = Column('TCOConsPow', 'power')
# TCOBatCap = Column('TCOBatCap', 'batteryCapacity')
#
# CONSUMER = [
#     TCOVehType,
#     TCONatCode,
#     TCOCo2Emi,
#     TCOCo2EmiV2,
#     TCOConsUrb,
#     TCOConsLand,
#     TCOConsTot,
#     TCOConsUrbV2,
#     TCOConsLandV2,
#     TCOConsTotV2,
#     TCOConsGasUrb,
#     TCOConsGasLand,
#     TCOConsGasTot,
#     TCOTXTConsGasUnitCd,
#     TCOConsPow,
#     TCOBatCap
# ]
#
# ESGTXTCodeCd2 = Column('ESGTXTCodeCd2', 'name')
# ESGTXTMainGrpCd2 = Column('ESGTXTMainGrpCd2', 'mainGroup')
# ESGTXTSubGrpCd2 = Column('ESGTXTSubGrpCd2', 'subGroup')
#
# ESACO = [
#     ESGTXTCodeCd2,
#     ESGTXTMainGrpCd2,
#     ESGTXTSubGrpCd2
# ]
#
# ESJEQTEQCodeCd = Column('ESJEQTEQCodeCd', 'code')
# ESJTXTESACOCd2 = Column('ESJTXTESACOCd2', 'name')
#
# ESAJOIN = [
#     ESJEQTEQCodeCd,
#     ESJTXTESACOCd2
# ]
#
# ECLColID = Column('ECLColID', 'basicColorCode')
# ECLColName = Column('ECLColName', 'basicColorName')
#
# EUROCOL = [
#     ECLColID,
#     ECLColName
# ]
#
# JWHVehType = Column('JWHVehType', 'VehType')
# JWHNatCode = Column('JWHNatCode', 'NatCode')
# JWHTYRTyreFCd = Column('JWHTYRTyreFCd', 'JWHTYRTyreFCd')
# JWHTYRTyreRCd = Column('JWHTYRTyreRCd', 'JWHTYRTyreRCd')
# JWHRIMRimFCd = Column('JWHRIMRimFCd', 'JWHRIMRimFCd')
# JWHRIMRimRCd = Column('JWHRIMRimRCd', 'JWHRIMRimRCd')
#
# JWHEEL = [
#     JWHVehType,
#     JWHNatCode,
#     JWHTYRTyreFCd,
#     JWHTYRTyreRCd,
#     JWHRIMRimFCd,
#     JWHRIMRimRCd
# ]
#
# MAKVehType = Column('MAKVehType', 'VehType')
# MAKNatCode = Column('MAKNatCode', 'schwackeCode')
# MAKName = Column('MAKName', 'name')
#
# MAKE = [
#     MAKVehType,
#     MAKNatCode,
#     MAKName
# ]
#
# MCLManColCode = Column('MCLManColCode', 'TCLMCLColCd')
# MCLECLColCd = Column('MCLECLColCd', 'basicColorCode')
# MCLColName = Column('MCLColName', 'manufacturerColorName')
# MCLPaintTrimFlag = Column('MCLPaintTrimFlag', 'manufacturerColorType')
#
# MANUCOL = [
#     MCLManColCode,
#     MCLECLColCd,
#     MCLColName,
#     MCLPaintTrimFlag
# ]
#
# MANEQTEQCodeCd = Column('MANEQTEQCodeCd', 'code')
# MANMCode = Column('MANMCode', 'manufacturerCode')
#
# MANUFACTOR = [
#     MANEQTEQCodeCd,
#     MANMCode
# ]
#
# MODVehType = Column('MODVehType', 'VehType')
# MODNatCode = Column('MODNatCode', 'TYPModCd')
# MODMakCD = Column('MODMakCD', 'schwackeCode')
# MODName = Column('MODName', 'name')
# MODName2 = Column('MODName2', 'name2')
# MODModelSerCode = Column('MODModelSerCode', 'serialCode')
# MODBegin = Column('MODBegin', 'yearBegin') #CAST(MODBegin, as INT)
# MODEnd = Column('MODEnd',  'yearEnd')  #CAST(MODEnd, as INT)
# MODImpBegin = Column('MODImpBegin', 'productionBegin')  #TO_DATE(MODImpBegin, 'yyyyMMdd')
# MODImpEnd = Column('MODImpEnd', 'productionEnd')  #TO_DATE(MODImpEnd, 'yyyyMMdd')
#
# MODEL = [
#     MODVehType,
#     MODNatCode,
#     MODMakCD,
#     MODName,
#     MODName2,
#     MODModelSerCode,
#     MODBegin,
#     MODEnd,
#     MODImpBegin,
#     MODImpEnd
# ]
#
# PRHVehType = Column('PRHVehType', 'VehType')
# PRHNatCode = Column('PRHNatCode', 'NatCode')
# PRHCurrency = Column('PRHCurrency', 'currency')
# PRHNP1 = Column('PRHNP1', 'gross')
# PRHNP2 = Column('PRHNP2', 'net')
# PRHTaxRt = Column('PRHTaxRt', 'taxRate')
# PRHVal = Column('PRHVal', 'beginDate') #TO_DATE(PRHVal, 'yyyyMMdd')
# PRHValUntil = Column('PRHValUntil', 'endDate') #TO_DATE(PRHValUntil, 'yyyyMMdd')
#
# PRICEHISTORY = [
#     PRHVehType,
#     PRHNatCode,
#     PRHCurrency,
#     PRHNP1,
#     PRHNP2,
#     PRHTaxRt,
#     PRHVal,
#     PRHValUntil
# ]
#
# RIMVehType = Column('RIMVehType', 'VehType')
# JWHRIMRimFCd = Column('RIMID', 'JWHRIMRimFCd')
# JWHRIMRimRCd = Column('RIMID', 'JWHRIMRimRCd')
# RIMWidth = Column('RIMWidth', 'rimWidth')
# RIMDiameter = Column('RIMDiameter', 'diameter')
#
# RIMS = [
#     RIMVehType,
#     JWHRIMRimFCd,
#     JWHRIMRimRCd,
#     RIMWidth,
#     RIMDiameter
# ]
#
# TCEVehType = Column('TCEVehType', 'VehType')
# TCENatCode = Column('TCENatCode', 'NatCode')
# TCENum = Column('TCENum', 'hsn')
# TCENum2 = Column('TCENum2', 'tsn')
#
# TCERT = [
#     TCEVehType,
#     TCENatCode,
#     TCENum,
#     TCENum2
# ]
#
# TECVehType = Column('TECVehType', 'VehType')
# TECNatCode = Column('TECNatCode', 'NatCode')
# TECTXTEngTypeCd2 = Column('TECTXTEngTypeCd2', 'engineType')
#
# TECHNIC = [
#     TECVehType,
#     TECNatCode,
#     TECTXTEngTypeCd2
# ]
#
# TXTCode = Column('TXTCode', 'TXTCode')
# TXTTextLong = Column('TXTTextLong', 'TXTTextLong')
#
# TXTTABLE = [
#     TXTCode,
#     TXTTextLong
# ]
#
# TENVehType = Column('TENVehType', 'VehType')
# TENNatCode = Column('TENNatCode', 'NatCode')
# TENCo2EffClassCd2 = Column('TENCo2EffClassCd2', 'energyEfficiencyClass')
#
# TYP_ENVKV = [
#     TENVehType,
#     TENNatCode,
#     TENCo2EffClassCd2
# ]
#
# TYPVehType = Column('TYPVehType', 'VehType')
# TYPNatCode = Column('TYPNatCode', 'NatCode')
# TYPModCd = Column('TYPModCd', 'TYPModCd')
# TYPName = Column('TYPName', 'name')
# TYPName2 = Column('TYPName2', 'name2')
# TYPTXTFuelTypeCd2 = Column('TYPTXTFuelTypeCd2', 'fuelType')
# TYPTXTDriveTypeCd2 = Column('TYPTXTDriveTypeCd2', 'driveType')
# TYPTXTTransTypeCd2 = Column('TYPTXTTransTypeCd2', 'transmissionType')
# TYPTXTBodyCo1Cd2 = Column('TYPTXTBodyCo1Cd2', 'bodyType')
# TYPDoor = Column('TYPDoor', 'doors')
# TYPSeat = Column('TYPSeat', 'seats')
# TYPImpBegin = Column('TYPImpBegin', 'productionBegin') #TO_DATE(TYPImpBegin, 'yyyyMM')
# TYPImpEnd = Column('TYPImpEnd', 'productionEnd') #TO_DATE(TYPImpEnd, 'yyyyMM')
# TYPKW = Column('TYPKW', 'kw')
# TYPHP = Column('TYPHP', 'ps')
# TYPCapTech = Column('TYPCapTech', 'displacement')
# TYPCylinder = Column('TYPCylinder', 'cylinders')
# TYPTXTPollNormCd2 = Column('TYPTXTPollNormCd2', 'emissionStandard')
# TYPTotWgt = Column('TYPTotWgt', 'weight')
# TYPLength = Column('TYPLength', 'lenght')
# TYPWidth = Column('TYPWidth', 'width')
#
# TYPE = [
#     TYPVehType,
#     TYPNatCode,
#     TYPModCd,
#     TYPName,
#     TYPName2,
#     TYPTXTFuelTypeCd2,
#     TYPTXTDriveTypeCd2,
#     TYPTXTTransTypeCd2,
#     TYPTXTBodyCo1Cd2,
#     TYPDoor,
#     TYPSeat,
#     TYPImpBegin,
#     TYPImpEnd,
#     TYPKW,
#     TYPHP,
#     TYPCapTech,
#     TYPCylinder,
#     TYPTXTPollNormCd2,
#     TYPTotWgt,
#     TYPLength,
#     TYPWidth
# ]
#
# TCLTypEqtCode = Column('TCLTypEqtCode', 'id')
# TCLMCLColCd = Column('TCLMCLColCd', 'TCLMCLColCd')
# TCLOrdCd = Column('TCLOrdCd', 'orderCode')
#
# TYPECOL = [
#     TCLTypEqtCode,
#     TCLMCLColCd,
#     TCLOrdCd
# ]
#
# TYRVehType = Column('TYRVehType', 'VehType')
# JWHTYRTyreFCd = Column('TYRID', 'JWHTYRTyreFCd')
# JWHTYRTyreRCd = Column('TYRID', 'JWHTYRTyreRCd')
# TYRWidth = Column('TYRWidth', 'width')
# TYRCrossSec = Column('TYRCrossSec', 'aspectRatio')
# TYRDesign = Column('TYRDesign', 'construction')
#
# TYRES = [
#     TYRVehType,
#     JWHTYRTyreFCd,
#     JWHTYRTyreRCd,
#     TYRWidth,
#     TYRCrossSec,
#     TYRDesign
# ]
