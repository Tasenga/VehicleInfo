import logging

from pyspark.sql import SparkSession, dataframe


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def create_tmp_table(spark: SparkSession) -> dataframe.DataFrame:
    '''
    The function gets required information from several hive-tables,
     aggregates it and saves like pyspark dataframe object.
     '''

    _LOGGER.debug(
        f'''start selecting preliminary temporary table.
                  The number of entries in main_table (schwacke.type)
                    {spark.sql("SELECT * FROM schwacke.type").count()}'''
    )

    tmp_table = spark.sql(
        '''SELECT
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
        FROM schwacke.type AS type
            LEFT JOIN
            schwacke.model AS model
            ON
            type.TYPVehType = model.MODVehType AND
            type.TYPModCd = model.MODNatCode
            LEFT JOIN
            schwacke.make AS make
            ON
            type.TYPVehType = make.MAKVehType AND
            type.TYPMakCd = make.MAKNatCode;'''
    )
    tmp_table = tmp_table.drop_duplicates()
    _LOGGER.debug('end selecting preliminary temporary table.')

    _LOGGER.debug('''start selecting data from table of types.''')
    type_table = spark.sql(
        '''SELECT
            TXTCode,
            TXTTextLong
        FROM schwacke.txttable;'''
    )
    _LOGGER.debug('end selecting preliminary temporary table.')

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

    _LOGGER.debug(
        '''start adding vehicle type information
        from the type table to the preliminary temporary table.'''
    )
    for replaced_column in [
        'bodyType',
        'driveType',
        'transmissionType',
    ]:
        tmp_table = type_replace(tmp_table, type_table, replaced_column)
    _LOGGER.debug(
        f'''end adding vehicle type information from
        the type table to the preliminary temporary table.
        The number of entries in final temporary table {tmp_table.count()}'''
    )

    return tmp_table
