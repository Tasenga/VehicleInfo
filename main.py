import logging
from pathlib import Path
import argparse
from typing import Tuple
import sys

from pyspark.sql import SparkSession

from vis.ddl_processing import DDL
from vis.dataframeworker import DataFrameWorker
from vis.databaseworker import DatabaseWorker
from vis.configuration import Configuration


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def get_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser()

    def file(astring: str) -> str:
        '''validate of input argument from ArgumentParser'''
        if not astring.endswith(".ini"):
            raise Exception("must be ini file type")
        return astring

    parser.add_argument(
        "-i",
        "--input",
        type=file,
        required=True,
        help='''type name of configuration file
                with relative path from root directory.
                For example, config.ini.
                Make sure that file have .ini type.''',
        metavar="INPUTTING FILE",
    )
    return parser


def run_main(spark: SparkSession, configuration: Configuration) -> Tuple[DataFrameWorker, DatabaseWorker]:

    ddl = DDL(
        configuration=configuration, template_file_name=f'{configuration.mode.value}_create_table_template_ddl.txt'
    )

    ddl.update_ddl()
    ddl.run_ddl(spark)

    if configuration.mode.value == 'short':
        tmp_table = DataFrameWorker.create_short_tmp_table(spark, configuration)
    else:
        tmp_table = DataFrameWorker.create_short_tmp_table(spark, configuration)  # TODO

    tmp_table.write_to_file()
    _LOGGER.debug('''json file with resulting data was created''')

    mongo_collection = DatabaseWorker.connect(configuration.host, configuration.port, tmp_table)
    mongo_collection.write_to_mongodb()
    _LOGGER.debug('''data was added to mongodb''')
    return tmp_table, mongo_collection


if __name__ == '__main__':

    namespace = get_parser().parse_args()
    config_file = Path(Path.cwd(), namespace.input)

    configuration = Configuration.from_file(config_file)
    _LOGGER.debug(f"current configuration {configuration}")

    with SparkSession.builder.appName('VehicleInfo').enableHiveSupport().getOrCreate() as spark:
        spark.sparkContext.setLogLevel('ERROR')
        logging.basicConfig(
            stream=sys.stdout,
            format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d  %H:%M:%S',
        )
        run_main(spark, configuration)
