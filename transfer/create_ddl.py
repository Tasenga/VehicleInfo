from pathlib import Path
from datetime import datetime

cwd = Path.cwd()


def update_ddl():
    """
    function returns a txt file with list of ddl operation
    with current timestamp
    """

    def change_standart_path(*args):
        return str(Path(cwd, *args)).replace('\\', '/')

    with Path('data_source', 'schwacke_hive_tables_ddl.txt').open('w') as ddl:
        return ddl.write(
            f'''CREATE DATABASE IF NOT EXISTS schwacke
            LOCATION "{change_standart_path('dbs', 'schwacke')}";

CREATE TABLE IF NOT EXISTS schwacke.make (
            MAKMarket CHAR(2),
            MAKVehType SMALLINT,
            MAKLangCode CHAR(4),
            MAKNatCode INT,
            MAKRecStatus SMALLINT,
            MAKRecDate CHAR(8),
            MAKName CHAR(40),
            MAKName2 CHAR(40),
            MAKCompany CHAR(40),
            MAKImporter CHAR(40),
            MAKURLMake CHAR(60),
            MAKURLImp CHAR(60),
            MAKADRCompCd INT,
            MAKADRImpCd INT,
            MAKSort INT
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS schwacke.model (
            MODMarket CHAR(2),
            MODVehType SMALLINT,
            MODLangCode CHAR(4),
            MODNatCode INT,
            MODRecStatus SMALLINT,
            MODRecDate CHAR(8),
            MODMakCD INT,
            MODMakIntCD SMALLINT,
            MODName CHAR(40),
            MODName2 CHAR(40),
            MODNameGrp1 CHAR(50),
            MODNameGrp2 CHAR(50),
            MODModelSerCode CHAR(10),
            MODBegin CHAR(4),
            MODEnd CHAR(4),
            MODImpBegin CHAR(6),
            MODImpEnd CHAR(6),
            MODSuccessor INT,
            MODPrev INT,
            MODSort INT
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS schwacke.type (
            TYPMarket CHAR(2),
            TYPVehType SMALLINT,
            TYPNatCode CHAR(13),
            TYPRecStatus SMALLINT,
            TYPRecDate CHAR(8),
            TYPName CHAR(60),
            TYPName2 CHAR(60),
            TYPTXTSeg1Cd2 CHAR(8),
            TYPTXTSeg2Cd2 CHAR(8),
            TYPTXTSegIntCd2 CHAR(8),
            TYPTXTSegFisCd2 CHAR(8),
            TYPModCd INT,
            TYPMakCd INT,
            TYPModIntCd SMALLINT,
            TYPMakIntCd SMALLINT,
            TYPImpBegin CHAR(6),
            TYPImpEnd CHAR(6),
            TYPTYPBasTypeCd CHAR(13),
            TYPStatus SMALLINT,
            TYPSort INT,
            TYPKW DECIMAL(6, 2),
            TYPHP DECIMAL(6, 2),
            TYPTaxHP DECIMAL(6, 2),
            TYPManCode CHAR(12),
            TYPTXTFuelTypeCd2 CHAR(8),
            TYPTXTBodyCo1Cd2 CHAR(8),
            TYPTXTBodyCo2Cd2 CHAR(8),
            TYPTXTBodyCoIntCd2 CHAR(8),
            TYPDoor SMALLINT,
            TYPCylinder SMALLINT,
            TYPTXTCylArrCd2 CHAR(8),
            TYPCapTech DECIMAL(7, 2),
            TYPTorque DECIMAL(6, 2),
            TYPTXTChargeCd2 CHAR(8),
            TYPValvpCyl SMALLINT,
            TYPTXTExhTreatCd2 CHAR(8),
            TYPTXTPollNormCd2 CHAR(8),
            TYPTXTTransTypeCd2 CHAR(8),
            TYPTXTTrnsTypCd2V2 CHAR(8),
            TYPTXTDriveTypeCd2 CHAR(8),
            TYPNumGearF SMALLINT,
            TYPNumGearFV2 SMALLINT,
            TYPWheelB1 INT,
            TYPWheelB1Max INT,
            TYPTotWgt INT,
            TYPTotWgtV2 INT,
            TYPSeat SMALLINT,
            TYPSeatMax SMALLINT,
            TYPSeat2 CHAR(10),
            TYPSeatMax2 CHAR(10),
            TYPDoorMax SMALLINT,
            TYPRoofLoad SMALLINT,
            TYPLength INT,
            TYPLengthMax INT,
            TYPWidth SMALLINT,
            TYPWidthMax SMALLINT,
            TYPHeight SMALLINT,
            TYPHeightMax SMALLINT,
            TYPTrunkCapMax SMALLINT,
            TYPTrunkCapMed SMALLINT,
            TYPTrunkCapMin SMALLINT,
            TYPCurbWgt INT,
            TYPCurbWgtV2 INT,
            TYPTrunkCapWin SMALLINT,
            TYPSteerPos SMALLINT,
            TYPExistPic SMALLINT,
            TYPExistVideo SMALLINT,
            TYPExistRep SMALLINT,
            TYPTargetGrp SMALLINT,
            TYPMloCd SMALLINT,
            TYPMltCd SMALLINT,
            TYPTseCd SMALLINT,
            TYPConsRating CHAR(1),
            TYPConsRatingV2 CHAR(1),
            TYPConsIndex DECIMAL(6, 4),
            TYPConsIndexV2 DECIMAL(6, 4),
            TYPUVID CHAR(25),
            TYPSecFuelTypCd2 CHAR(8),
            TYPSecKW DECIMAL(6, 2),
            TYPSecTorque DECIMAL(6, 2),
            TYPRoofMaterialCd2 CHAR(8),
            TYPRegTypeCd2 CHAR(8)
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

LOAD DATA INPATH "{change_standart_path('data_source', 'MAKE.gkp')}"
INTO TABLE schwacke.make
    PARTITION(
        data_date_part='{datetime.now().date()}',
        data_timestamp_part='{int(datetime.now().timestamp())}'
    );

LOAD DATA INPATH "{change_standart_path('data_source', 'MODEL.gkp')}"
INTO TABLE schwacke.model
    PARTITION(
        data_date_part='{datetime.now().date()}',
        data_timestamp_part='{int(datetime.now().timestamp())}'
    );

LOAD DATA INPATH "{change_standart_path('data_source', 'TYPE.gkp')}"
INTO TABLE schwacke.type
    PARTITION(
        data_date_part='{datetime.now().date()}',
        data_timestamp_part='{int(datetime.now().timestamp())}'
    );
        '''
        )


def read_ddl():
    with Path(cwd, 'data_source', 'schwacke_hive_tables_ddl.txt').open(
        'r'
    ) as ddl:
        return ddl.read()
