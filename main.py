from pyspark.sql import SparkSession

from transfer.create_ddl import update_ddl, read_ddl


if __name__ == '__main__':
    update_ddl()

    spark = (
        SparkSession.builder.appName('VehicleInfo')
        .enableHiveSupport()
        .getOrCreate()
    )

    for ddl in read_ddl().split('\n\n'):
        spark.sql(f'''{ddl}''')

    spark.sql("SELECT * FROM schwacke.make").show()
    spark.sql("SELECT * FROM schwacke.model").show()
    spark.sql("SELECT * FROM schwacke.type").show()
