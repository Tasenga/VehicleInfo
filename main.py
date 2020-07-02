import logging
from datetime import datetime
from pathlib import Path
import argparse

from pyspark.sql import SparkSession

# from pymongo import MongoClient

from script.ddl_processing import DDL
from script.mapping import create_tmp_table
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


def run_main() -> None:
    pass


def run_test() -> None:
    pass


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

    ddl = DDL(configuration=configuration)
    ddl.update_ddl()

    for ddl in ddl.read_ddl().split('\n\n'):
        spark.sql(f'''{ddl}''')
        _LOGGER.debug(f"operation was completed: {ddl}")

    # def save_files
    tmp_table_folder = Path(
        Path.cwd(), configuration.mode_folder.value, 'result'
    )
    tmp_table_folder.mkdir(parents=True, exist_ok=True)
    create_tmp_table(spark).coalesce(1).write.json(
        str(
            Path(
                tmp_table_folder,
                f'{datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")}',
            )
        )
    )
    _LOGGER.debug('''json file with resulting data was created''')

    # class database with methods connect write read
    # client = MongoClient("localhost", 27017)
    # db = client['vehicles']
    # collection_currency = db['variants']

    # collection_currency.insert_many([json.loads(row)
    # for row in tmp_table(spark).toJSON().collect()])

    # client.close()
