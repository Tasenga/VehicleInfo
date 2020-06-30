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
            MAKMarket VARCHAR(2),
            MAKVehType SMALLINT,
            MAKLangCode VARCHAR(4),
            MAKNatCode INT,
            MAKRecStatus SMALLINT,
            MAKRecDate VARCHAR(8),
            MAKName VARCHAR(40),
            MAKName2 VARCHAR(40),
            MAKCompany VARCHAR(40),
            MAKImporter VARCHAR(40),
            MAKURLMake VARCHAR(60),
            MAKURLImp VARCHAR(60),
            MAKADRCompCd INT,
            MAKADRImpCd INT,
            MAKSort INT
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS schwacke.model (
            MODMarket VARCHAR(2),
            MODVehType SMALLINT,
            MODLangCode VARCHAR(4),
            MODNatCode INT,
            MODRecStatus SMALLINT,
            MODRecDate VARCHAR(8),
            MODMakCD INT,
            MODMakIntCD SMALLINT,
            MODName VARCHAR(40),
            MODName2 VARCHAR(40),
            MODNameGrp1 VARCHAR(50),
            MODNameGrp2 VARCHAR(50),
            MODModelSerCode VARCHAR(10),
            MODBegin VARCHAR(4),
            MODEnd VARCHAR(4),
            MODImpBegin VARCHAR(6),
            MODImpEnd VARCHAR(6),
            MODSuccessor INT,
            MODPrev INT,
            MODSort INT
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS schwacke.type (
            TYPMarket VARCHAR(2),
            TYPVehType SMALLINT,
            TYPNatCode VARCHAR(13),
            TYPRecStatus SMALLINT,
            TYPRecDate VARCHAR(8),
            TYPName VARCHAR(60),
            TYPName2 VARCHAR(60),
            TYPTXTSeg1Cd2 VARCHAR(8),
            TYPTXTSeg2Cd2 VARCHAR(8),
            TYPTXTSegIntCd2 VARCHAR(8),
            TYPTXTSegFisCd2 VARCHAR(8),
            TYPModCd INT,
            TYPMakCd INT,
            TYPModIntCd SMALLINT,
            TYPMakIntCd SMALLINT,
            TYPImpBegin VARCHAR(6),
            TYPImpEnd VARCHAR(6),
            TYPTYPBasTypeCd VARCHAR(13),
            TYPStatus SMALLINT,
            TYPSort INT,
            TYPKW DECIMAL(6, 2),
            TYPHP DECIMAL(6, 2),
            TYPTaxHP DECIMAL(6, 2),
            TYPManCode VARCHAR(12),
            TYPTXTFuelTypeCd2 VARCHAR(8),
            TYPTXTBodyCo1Cd2 VARCHAR(8),
            TYPTXTBodyCo2Cd2 VARCHAR(8),
            TYPTXTBodyCoIntCd2 VARCHAR(8),
            TYPDoor SMALLINT,
            TYPCylinder SMALLINT,
            TYPTXTCylArrCd2 VARCHAR(8),
            TYPCapTech DECIMAL(7, 2),
            TYPTorque DECIMAL(6, 2),
            TYPTXTVARCHARgeCd2 VARCHAR(8),
            TYPValvpCyl SMALLINT,
            TYPTXTExhTreatCd2 VARCHAR(8),
            TYPTXTPollNormCd2 VARCHAR(8),
            TYPTXTTransTypeCd2 VARCHAR(8),
            TYPTXTTrnsTypCd2V2 VARCHAR(8),
            TYPTXTDriveTypeCd2 VARCHAR(8),
            TYPNumGearF SMALLINT,
            TYPNumGearFV2 SMALLINT,
            TYPWheelB1 INT,
            TYPWheelB1Max INT,
            TYPTotWgt INT,
            TYPTotWgtV2 INT,
            TYPSeat SMALLINT,
            TYPSeatMax SMALLINT,
            TYPSeat2 VARCHAR(10),
            TYPSeatMax2 VARCHAR(10),
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
            TYPConsRating VARCHAR(1),
            TYPConsRatingV2 VARCHAR(1),
            TYPConsIndex DECIMAL(6, 4),
            TYPConsIndexV2 DECIMAL(6, 4),
            TYPUVID VARCHAR(25),
            TYPSecFuelTypCd2 VARCHAR(8),
            TYPSecKW DECIMAL(6, 2),
            TYPSecTorque DECIMAL(6, 2),
            TYPRoofMaterialCd2 VARCHAR(8),
            TYPRegTypeCd2 VARCHAR(8)
)
PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS schwacke.txttable (
            TXTMarket VARCHAR(2),
            TXTCode VARCHAR(8),
            TXTLangCode VARCHAR(4),
            TXTTextLong VARCHAR(50),
            TXTTextShort VARCHAR(10),
            TXTRecStatus SMALLINT,
            TXTRecDate VARCHAR(8)
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

LOAD DATA INPATH "{change_standart_path('data_source', 'TXTTABEL.gkp')}"
INTO TABLE schwacke.txttable
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
