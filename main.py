import logging
from pathlib import Path
import argparse
import sys

from pyspark.sql import SparkSession

from script.ddl_processing import DDL
from script.dfworker import DfWorker
from script.dbworker import DbWorker
from script.configuration import Configuration
from script.global_test import global_test


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def catch_args() -> argparse.ArgumentParser:
    '''functions parses con'''

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


def run_main(spark: SparkSession, configuration: Configuration) -> None:

    ddl = DDL(configuration=configuration)

    ddl.update_ddl()
    ddl.run_ddl(spark)

    tmp_table = DfWorker.create_tmp_table(spark, configuration)

    tmp_table.write_to_file()
    _LOGGER.debug('''json file with resulting data was created''')

    mongo_collection = DbWorker.connect('localhost', 27017, tmp_table)
    mongo_collection.write_to_mongodb()
    _LOGGER.debug('''data was added to mongodb''')

    if configuration.mode.value == 'test':
        global_test(tmp_table, mongo_collection)


if __name__ == '__main__':

    namespace = catch_args().parse_args()
    config_file = Path(Path.cwd(), namespace.input)

    configuration = Configuration.from_file(config_file)

    spark = (
        SparkSession.builder.appName('VehicleInfo')
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')
    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d  %H:%M:%S',
    )

    _LOGGER.debug(f"current configuration {configuration}")

    run_main(spark, configuration)
