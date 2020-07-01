from datetime import datetime
import json
from pathlib import Path

from pyspark.sql import SparkSession

from transfer.create_ddl import update_ddl, read_ddl
from transfer.mapping import tmp_table, create_dict


if __name__ == '__main__':
    update_ddl()

    spark = (
        SparkSession.builder.appName('VehicleInfo')
        .enableHiveSupport()
        .getOrCreate()
    )

    for ddl in read_ddl().split('\n\n'):
        spark.sql(f'''{ddl}''')

    with Path(
        'results', f'output_{int(datetime.now().timestamp())}.json'
    ).open('w') as output:
        for row in tmp_table(spark).collect():
            output.write(json.dumps(create_dict(row)))
            output.write('\n')

    # tmp_table(spark).toPandas().to_json(
    #     'D:/json.json', orient='records', force_ascii=False, lines=True
    # )
