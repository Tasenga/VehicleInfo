from pathlib import Path
from shutil import copy
import logging

from script.dfworker import DfWorker
from script.dbworker import DbWorker


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


def global_test(DfWorker: DfWorker, DbWorker: DbWorker) -> None:
    '''The function compares data from result file and mongo collection
    with control output file'''
    mode_folder = Path(Path.cwd(), DfWorker.configuration.mode_folder.value)
    for path in Path(mode_folder, 'data_source', 'data').iterdir():
        if path.is_file():
            copy(str(path), str(Path(mode_folder, 'data_source')))

    result_from_file = DfWorker.read_from_file()
    result_from_db = DbWorker.read_from_mongodb()

    with Path(mode_folder, 'data_source', 'control_output.json').open(
        'r'
    ) as file_result:
        control_result = file_result.read()

    DbWorker.drop_collection_mongodb()

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
