def tmp_table(spark):
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

    type_table = spark.sql(
        '''SELECT
            TXTCode,
            TXTTextLong
        FROM schwacke.txttable;'''
    )

    def type_replace(main_df, type_df, replaced_column):
        return (
            main_df.join(
                type_df,
                main_df[replaced_column] == type_df['TXTCode'],
                'left',
            )
            .drop(replaced_column, "TXTCode")
            .withColumnRenamed('TXTTextLong', replaced_column)
        )

    for replaced_column in [
        'bodyType',
        'driveType',
        'transmissionType',
    ]:
        tmp_table = type_replace(tmp_table, type_table, replaced_column)

    return tmp_table
