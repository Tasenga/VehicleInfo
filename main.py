import logging
from datetime import datetime
from pathlib import Path
import argparse
from shutil import copy

from pyspark.sql import SparkSession

from script.ddl_processing import DDL
from script.df_worker import DF_WORKER
from script.db_worker import DB_WORKER
from script.configuration import Configuration


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def catch_args() -> argparse.Namespace:
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
    namespace = parser.parse_args()
    return namespace


def set_custom_logging(
    spark: SparkSession, configuration: Configuration
) -> None:
    spark.sparkContext.setLogLevel('ERROR')
    log_folder = Path(Path.cwd(), configuration.mode_folder.value, 'logs')
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file = Path(
        log_folder,
        f'log_{datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")}.log',
    )

    logging.root.handlers = [
        logging.FileHandler(log_file, mode='w'),
        logging.StreamHandler(),
    ]

    logging.basicConfig(
        format='%(asctime)s - %(funcName)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d  %H:%M:%S',
    )


def run_main(spark: SparkSession, configuration: Configuration) -> None:

    ddl = DDL(configuration=configuration)

    ddl.update_ddl()
    ddl.run_ddl(spark)

    tmp_table = DF_WORKER.create_tmp_table(spark, configuration)

    tmp_table.write_to_file()
    _LOGGER.debug('''json file with resulting data was created''')

    mongo_collection = DB_WORKER.connect('localhost', 27017, tmp_table)
    mongo_collection.write_to_mongodb()
    _LOGGER.debug('''data was added to mongodb''')

    if configuration.mode.value == 'test':
        mode_folder = Path(
            Path.cwd(), tmp_table.configuration.mode_folder.value
        )
        for path in Path(mode_folder, 'data_source', 'data').iterdir():
            if path.is_file():
                copy(str(path), str(Path(mode_folder, 'data_source')))

        result_from_file = tmp_table.read_from_file()

        print(result_from_file)

        result_from_db = mongo_collection.read_from_mongodb()

        print(result_from_db)

        mongo_collection.drop_collection_mongodb()


if __name__ == '__main__':
    config_file = Path(Path.cwd(), catch_args().input)
    configuration = Configuration.from_file(config_file)

    spark = (
        SparkSession.builder.appName('VehicleInfo')
        .enableHiveSupport()
        .getOrCreate()
    )

    set_custom_logging(spark, configuration)

    _LOGGER.debug(f"current configuration {configuration}")

    run_main(spark, configuration)
