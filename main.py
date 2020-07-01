from configparser import ConfigParser

from pyspark.sql import SparkSession

from transfer.create_ddl import update_ddl, read_ddl
from transfer.mapping import tmp_table


if __name__ == '__main__':
    config = ConfigParser()
    config.read('config.ini')

    update_ddl()

    spark = (
        SparkSession.builder.appName('VehicleInfo')
        .enableHiveSupport()
        .getOrCreate()
    )

    for ddl in read_ddl().split('\n\n'):
        spark.sql(f'''{ddl}''')

    tmp_table(spark).coalesce(1).write.format('json').save('D:/json3.json')
