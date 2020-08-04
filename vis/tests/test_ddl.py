from pathlib import Path
from os import remove
from typing import List, Any
from decimal import Decimal
from types import ModuleType

from pyspark.sql import SparkSession
import pytest

from vis.configuration import Configuration
from vis.sql_processing import SQL


spark = SparkSession.builder.enableHiveSupport().getOrCreate()


def teardown_module(module: ModuleType) -> None:
    spark.stop()
    print('spark-session close')


def test_update_sql() -> None:

    configuration = Configuration(
        mode='full',
        source_folder='test_data_source',
        db_name='schwacke_test',
        host='localhost',
        port=27017,
        current_date='2020-07-14',
        current_timestamp='1594726338',
    )
    sql = SQL(configuration, 'full_create_table_template.txt')
    try:
        sql.update_sql(cwd=Path("D:\\project\\VehicleInfo"))
        with Path(f'{sql.sql_directory}', f'{sql.file_sql}').open('r') as sql_file:
            text_sql = sql_file.read()

        with Path(f'{sql.sql_directory}', 'test_create_table.txt').open('r') as control_sql_file:
            control_sql = control_sql_file.read()
    finally:
        remove(Path(f'{sql.file_sql}'))

    assert text_sql == control_sql


@pytest.fixture(
    scope="function",
    params=[
        (
            "ab\t4\t1986546\t8000.65\n\taa\t4\ta",
            spark.createDataFrame(
                [["ab", 4, 1986546, Decimal(8000.65)], ['', None, 4, None]],
                "Varchar: string, SmallInteger: smallint, Integer: int, DEC: Decimal(6, 2)",
            ).collect(),
        ),
        (
            "abcd\t10456564\t0\t800",
            spark.createDataFrame(
                [["ab", None, 0, Decimal(800)]],
                "Varchar: string, SmallInteger: smallint, Integer: int, DEC: Decimal(6, 2)",
            ).collect(),
        ),
        (
            "AS\t4S\t054668878\t80055.55",
            spark.createDataFrame(
                [["AS", None, 54668878, None]],
                "Varchar: string, SmallInteger: smallint, Integer: int, DEC: Decimal(6, 2)",
            ).collect(),
        ),
    ],
)
def param_test(request: pytest.fixture) -> List[Any]:
    return request.param


class TestRunSQL:

    configuration = Configuration(
        mode='short',
        source_folder='test_data_source',
        db_name='schwacke_test_type',
        host='localhost',
        port=27017,
        current_date='test',
        current_timestamp='1',
    )
    sql = SQL(configuration, 'test.txt')
    cwd = str(Path.cwd()).replace('\\', '/')

    def setup(self) -> None:
        with self.sql.file_sql.open('w') as sql_file:
            sql_file.write(
                f'''CREATE DATABASE IF NOT EXISTS {self.sql.configuration.db_name}
                    LOCATION "{self.cwd}/dbs/{self.sql.configuration.db_name}";

                    CREATE TABLE IF NOT EXISTS {self.sql.configuration.db_name}.test_type (
                        Varchar VARCHAR(2),
                        SmallInteger SMALLINT,
                        Integer INT,
                        DEC DECIMAL(6, 2)
                    )
                    PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY '\t'
                    STORED AS TEXTFILE;

                    LOAD DATA INPATH "{self.cwd}/{self.sql.configuration.source_folder}/test_type.gkp"
                    INTO TABLE {self.sql.configuration.db_name}.test_type
                    PARTITION(
                        data_date_part='{self.sql.configuration.current_date}',
                        data_timestamp_part='{self.sql.configuration.current_timestamp}');'''
            )

    def teardown(self) -> None:
        spark.sql(f'''DROP DATABASE {self.sql.configuration.db_name} CASCADE''')

    def test_correct_data_type(self, param_test: List[Any]) -> None:
        (input, expected_output) = param_test
        with Path(self.sql.mode_directory, 'test_type.gkp').open('w') as gkp:
            gkp.write(input)
        self.sql.run_sql(spark)
        test_type_table = spark.sql(
            f'''SELECT Varchar, SmallInteger, Integer, DEC FROM {self.sql.configuration.db_name}.test_type;'''
        ).collect()
        expected_type_table = expected_output
        assert test_type_table == expected_type_table
