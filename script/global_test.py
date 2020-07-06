from pathlib import Path
from shutil import copy
import logging

from script.df_worker import DF_WORKER
from script.db_worker import DB_WORKER


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def global_test(DF_WORKER: DF_WORKER, DB_WORKER: DB_WORKER) -> None:
    '''The function compares data from result file and mongo collection
    with control output file'''
    mode_folder = Path(Path.cwd(), DF_WORKER.configuration.mode_folder.value)
    for path in Path(mode_folder, 'data_source', 'data').iterdir():
        if path.is_file():
            copy(str(path), str(Path(mode_folder, 'data_source')))

    result_from_file = DF_WORKER.read_from_file()
    result_from_db = DB_WORKER.read_from_mongodb()

    with Path(mode_folder, 'data_source', 'control_output.json').open(
        'r'
    ) as file_result:
        control_result = file_result.read()

    DB_WORKER.drop_collection_mongodb()

    for dict in result_from_db:
        dict.pop('_id')

    if str(result_from_db) == str(control_result):
        _LOGGER.debug('SUCCESS DB entries')
    else:
        _LOGGER.debug('ERROR DB entries')

    if str(result_from_file) == str(control_result):
        _LOGGER.debug('SUCCESS file entries')
    else:
        _LOGGER.debug('ERROR file entries')
