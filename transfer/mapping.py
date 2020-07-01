from pyspark.sql.functions import col


def tmp_table(spark):
    tmp_table = spark.sql(
        '''SELECT
        MODNatCode,
        MODName,
        MODName2,
        MAKNatCode,
        MAKName,
        TYPNatCode,
        TYPName,
        TYPDoor,
        TYPSeat,
        TYPTXTBodyCo1Cd2,
        TYPTXTDriveTypeCd2,
        TYPTXTTransTypeCd2
        FROM schwacke.type AS type
            INNER JOIN
            schwacke.model AS model
            ON
            type.TYPVehType = model.MODVehType AND
            type.TYPModCd = model.MODNatCode
            INNER JOIN
            schwacke.make AS make
            ON
            type.TYPVehType = make.MAKVehType AND
            type.TYPMakCd = make.MAKNatCode'''
    )

    type_table = spark.sql(
        '''SELECT
            TXTCode,
            TXTTextLong
        FROM schwacke.txttable'''
    )

    def type_replace(main_df, type_df, replaced_column):
        return (
            main_df.join(
                type_df,
                main_df[replaced_column] == type_df['TXTCode'],
                'inner',
            )
            .withColumn(replaced_column, col("TXTTextLong"))
            .drop("TXTTextLong", "TXTCode")
        )

    for replaced_column in [
        'TYPTXTBodyCo1Cd2',
        'TYPTXTDriveTypeCd2',
        'TYPTXTTransTypeCd2',
    ]:
        tmp_table = type_replace(tmp_table, type_table, replaced_column)

    return tmp_table


def create_dict(row):
    return {
        'schwackeCode': row.TYPNatCode,
        'model': {
            'schwackeCode': row.MODNatCode,
            'name': row.MODName,
            'name2': row.MODName2,
        },
        'make': {'schwackeCode': row.MAKNatCode, 'name': row.MAKName},
        'name': row.TYPName,
        'bodyType': row.TYPTXTBodyCo1Cd2,
        'driveType': row.TYPTXTDriveTypeCd2,
        'transmissionType': row.TYPTXTTransTypeCd2,
        'doors': row.TYPDoor,
        'seats': row.TYPSeat,
    }
