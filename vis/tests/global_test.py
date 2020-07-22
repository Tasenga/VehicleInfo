#  type: ignore

from pathlib import Path
from shutil import copy, rmtree
from datetime import datetime
from json import loads

from pyspark.sql import SparkSession
import pytest

from main import run_main
from vis.configuration import Configuration, Mode


configuration_short = Configuration(
    mode='short',
    source_folder='test_data_source',
    db_name='schwacke_test',
    host='localhost',
    port=27017,
    current_date=str(datetime.now().date()),
    current_timestamp=str(int(datetime.now().timestamp())),
)

configuration_full = Configuration(
    mode='full',
    source_folder='test_data_source',
    db_name='schwacke_test',
    host='localhost',
    port=27017,
    current_date=str(datetime.now().date()),
    current_timestamp=str(int(datetime.now().timestamp())),
)


def setup() -> None:
    mode_folder = Path(Path.cwd(), 'test_data_source')
    for path in Path(mode_folder, 'archive').iterdir():
        if path.is_file():
            copy(str(path), str(Path(mode_folder)))


@pytest.mark.parametrize("configuration", [configuration_short, configuration_full])
@pytest.mark.critital_tests
def test_run_main(spark_session: SparkSession, configuration: Configuration) -> None:

    tmp_table, mongo_collection = run_main(spark_session, configuration)
    try:
        result_from_file = []
        result_from_file = tmp_table.read_from_file(result_from_file)

        result_from_db = mongo_collection.read_from_mongodb()
        for dictionary in result_from_db:
            dictionary.pop('_id')

        if configuration.mode == Mode.short:
            control_file_path = Path(Path.cwd(), configuration.source_folder, 'control_short_output.json')
        else:
            control_file_path = Path(Path.cwd(), configuration.source_folder, 'control_full_output.json')

        with control_file_path.open('r') as file_result:
            control_result = [loads(row) for row in file_result]

        assert result_from_db == control_result
        assert result_from_file == control_result

    finally:
        rmtree(
            Path(
                Path(Path.cwd(), 'result'),
                f'{tmp_table.configuration.db_name}_{tmp_table.configuration.mode.value}_'
                f'{tmp_table.configuration.current_date}_{tmp_table.configuration.current_timestamp}',
            ),
            ignore_errors=True,
        )
        mongo_collection.drop_collection_mongodb()
