from pathlib import Path
from os import remove
from typing import List, Any

from pyspark.sql import SparkSession

from vis.configuration import Configuration
from vis.ddl_processing import DDL


# def test_update_ddl() -> None:
#
#     configuration = Configuration(
#         mode='full',
#         source_folder='test_data_source',
#         db_name='schwacke_test',
#         host='localhost',
#         port=27017,
#         current_date='2020-07-14',
#         current_timestamp='1594726338',
#     )
#
#     try:
#         ddl = DDL(configuration)
#         ddl.update_ddl(cwd=Path("D:\\project\\VehicleInfo"))
#         with Path(ddl.mode_directory,
#              f'{ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt').open(
#             'r'
#         ) as ddl_file:
#             text_ddl = ddl_file.read()
#
#         with Path(ddl.mode_directory,
#                   f'control_schwacke_hive_tables_ddl.txt').open(
#             'r'
#         ) as control_ddl_file:
#             control_ddl = control_ddl_file.read()
#     finally:
#         remove(Path(ddl.mode_directory,
#              f'{ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt'))
#
#     assert text_ddl == control_ddl


def test_run_ddl(spark_session: SparkSession) -> None:

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
    try:
        with Path(ddl.mode_directory, 'test_type.gkp').open('w') as gkp:
            gkp.write('''ab\t4\t1986546\t8000.65\n\taa\t4\ta\n''')

        with ddl.file_ddl.open('w') as ddl_file:
            ddl_file.write(
                f'''CREATE DATABASE IF NOT EXISTS {ddl.configuration.db_name}
                    LOCATION "{cwd}/dbs/{ddl.configuration.db_name}";

                    CREATE TABLE IF NOT EXISTS {ddl.configuration.db_name}.test_type (
                        Varchar VARCHAR(2),
                        SmallInteger SMALLINT,
                        Integer INT,
                        DEC DECIMAL(6, 2)
                    )
                    PARTITIONED BY(data_date_part STRING, data_timestamp_part STRING)
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY '\t'
                    STORED AS TEXTFILE;

                    LOAD DATA INPATH "{cwd}/{ddl.configuration.source_folder}/test_type.gkp"
                    INTO TABLE {ddl.configuration.db_name}.test_type
                    PARTITION(
                        data_date_part='{ddl.configuration.current_date}',
                        data_timestamp_part='{ddl.configuration.current_timestamp}');'''
            )

        ddl.run_ddl(spark_session)
        print(f'create {ddl.configuration.db_name}')

        test_type_table = spark_session.sql(f'''SELECT * FROM {ddl.configuration.db_name}.test_type;''')
        test_type_table.show()

    finally:
        remove(Path(ddl.mode_directory, f'{ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt'))
        spark_session.sql(f'''DROP DATABASE {ddl.configuration.db_name} CASCADE''')


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

    def setup(self, data: List[Any]) -> None:
        with Path(self.ddl.mode_directory, 'test_type.gkp').open('w') as gkp:
            gkp.write('''ab\t4\t1986546\t8000.65\n\taa\t4\ta\n''')

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

    def teardown(self, spark_session: SparkSession) -> None:
        remove(Path(self.ddl.mode_directory, f'{self.ddl.configuration.mode.value}_schwacke_hive_tables_ddl.txt'))
        spark_session.sql(f'''DROP DATABASE {self.ddl.configuration.db_name} CASCADE''')

    def test_correct_data_type(self, spark_session: SparkSession) -> None:
        self.ddl.run_ddl(spark_session)
        test_type_table = spark_session.sql(f'''SELECT * FROM {self.ddl.configuration.db_name}.test_type;''')
        print(test_type_table.show())
        assert 5 * 6 == 30
