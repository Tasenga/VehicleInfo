from pathlib import Path
from shutil import copy, rmtree
from datetime import datetime
from json import loads

from pyspark.sql import SparkSession

from main import run_main
from vis.configuration import Configuration


def test_run_main_short(spark_session: SparkSession) -> None:

    configuration = Configuration(
        mode='short',
        source_folder='test_data_source',
        db_name='schwacke_test',
        host='localhost',
        port=27017,
        current_date=str(datetime.now().date()),
        current_timestamp=str(int(datetime.now().timestamp())),
    )

    mode_folder = Path(Path.cwd(), configuration.source_folder)
    for path in Path(mode_folder, 'archive').iterdir():
        if path.is_file():
            copy(str(path), str(Path(mode_folder)))

    tmp_table, mongo_collection = run_main(spark_session, configuration)

    try:
        result_from_file = tmp_table.read_from_file()
        result_from_db = mongo_collection.read_from_mongodb()
        for dict in result_from_db:
            dict.pop('_id')
        with Path(mode_folder, 'control_output.json').open(
            'r'
        ) as file_result:
            control_result = [loads(row) for row in file_result]

    finally:
        rmtree(
            Path(
                tmp_table.tmp_table_folder,
                f'{tmp_table.configuration.current_date}'
                f'_{tmp_table.configuration.current_timestamp}',
            ),
            ignore_errors=True,
        )
        mongo_collection.drop_collection_mongodb()

    assert result_from_db == control_result
    assert result_from_file == control_result
