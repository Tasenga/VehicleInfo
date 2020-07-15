from pathlib import Path
from os import remove
from typing import List, Any
from decimal import Decimal
from types import ModuleType

from pyspark.sql import SparkSession
import pytest

from vis.configuration import Configuration
from vis.ddl_processing import DDL


spark = SparkSession.builder.enableHiveSupport().getOrCreate()


def teardown_module(module: ModuleType) -> None:
    spark.stop()
    print('spark-session close')


def test_update_ddl() -> None:

    configuration = Configuration(
        mode='full',
        source_folder='test_data_source',
        db_name='schwacke_test',
        host='localhost',
        port=27017,
        current_date='2020-07-14',
        current_timestamp='1594726338',
    )

    try:
        ddl = DDL(configuration)
        ddl.update_ddl(cwd=Path("D:\\project\\VehicleInfo"))
        with Path(ddl.mode_directory, f'{ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt').open(
            'r'
        ) as ddl_file:
            text_ddl = ddl_file.read()

        with Path(ddl.mode_directory, 'control_schwacke_hive_tables_ddl.txt').open('r') as control_ddl_file:
            control_ddl = control_ddl_file.read()
    finally:
        remove(Path(ddl.mode_directory, f'{ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt'))

    assert text_ddl == control_ddl


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


class TestRunDDL:

    configuration = Configuration(
        mode='short',
        source_folder='test_data_source',
        db_name='schwacke_test_type',
        host='localhost',
        port=27017,
        current_date='test',
        current_timestamp='1',
    )
    ddl = DDL(configuration)
    cwd = str(Path.cwd()).replace('\\', '/')

    def setup(self) -> None:
        with self.ddl.file_ddl.open('w') as ddl_file:
            ddl_file.write(
                f'''CREATE DATABASE IF NOT EXISTS {self.ddl.configuration.db_name}
                    LOCATION "{self.cwd}/dbs/{self.ddl.configuration.db_name}";

                    CREATE TABLE IF NOT EXISTS {self.ddl.configuration.db_name}.test_type (
                        Varchar VARCHAR(2),
                        SmallInteger SMALLINT,
                        Integer INT,
                        DEC DECIMAL(6, 2)
                    )
                    PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY '\t'
                    STORED AS TEXTFILE;

                    LOAD DATA INPATH "{self.cwd}/{self.ddl.configuration.source_folder}/test_type.gkp"
                    INTO TABLE {self.ddl.configuration.db_name}.test_type
                    PARTITION(
                        data_date_part='{self.ddl.configuration.current_date}',
                        data_timestamp_part='{self.ddl.configuration.current_timestamp}');'''
            )

    def teardown(self) -> None:
        remove(Path(self.ddl.mode_directory, f'{self.ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt'))
        spark.sql(f'''DROP DATABASE {self.ddl.configuration.db_name} CASCADE''')

    def test_correct_data_type(self, param_test: List[Any]) -> None:
        (input, expected_output) = param_test
        with Path(self.ddl.mode_directory, 'test_type.gkp').open('w') as gkp:
            gkp.write(input)
        self.ddl.run_ddl(spark)
        test_type_table = spark.sql(
            f'''SELECT Varchar, SmallInteger, Integer, DEC FROM {self.ddl.configuration.db_name}.test_type;'''
        ).collect()
        expected_type_table = expected_output
        assert test_type_table == expected_type_table
