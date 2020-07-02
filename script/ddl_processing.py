from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import logging

from .configuration import Configuration


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)
cwd = Path.cwd()


@dataclass
class DDL:
    '''
    The object of the DDL class contains customer parameters
    to create, update and read customizing DDL operations
    to extract data from the data source to hive-tables.
    '''

    configuration: Configuration

    @property
    def mode_directory(self) -> Path:
        return Path(cwd, self.configuration.mode_folder.value, 'data_source')

    def update_ddl(self) -> None:
        '''
        The function writes a text file with a list of ddl operations
        according to the template with the current time stamp
        and the current project directory according to the configuration.
        '''

        with Path(self.mode_directory, 'template_ddl.txt').open(
            'r'
        ) as example:
            template = example.read()

        change_list = {
            '{ROOT_DIRECTORY}': str(Path.cwd()).replace('\\', '/'),
            '{MODE_FOLDER}': self.configuration.mode_folder.value,
            '{CURRENT_DATE_%Y-%m-%d}': str(datetime.now().date()),
            '{CURRENT_TIMESTAMP}': str(int(datetime.now().timestamp())),
        }

        for old, new in change_list.items():
            template = template.replace(old, new)

        with Path(self.mode_directory, 'schwacke_hive_tables_ddl.txt').open(
            'w'
        ) as ddl_file:
            ddl_file.write(template)

        _LOGGER.debug(
            '''text file with a list of ddl operations
                        according to the template was created successful'''
        )

    def read_ddl(self) -> str:
        '''
        the function reads a text file with a list of ddl operations
        according to the template with the current time stamp
        and the current project directory according to the configuration
        to single string object
        '''
        with Path(self.mode_directory, 'schwacke_hive_tables_ddl.txt').open(
            'r'
        ) as ddl:
            _LOGGER.debug(
                '''text file with a list of ddl operations is available'''
            )
            return ddl.read()
