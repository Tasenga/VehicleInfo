from pathlib import Path
from dataclasses import dataclass
import logging
from os import remove
from typing import Optional

from pyspark.sql import SparkSession, dataframe

from .configuration import Configuration


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


@dataclass
class DDL:
    '''
    The object of the DDL class contains customer parameters to create, update and read customizing DDL operations
    to extract data from the data source to hive-tables.
    '''

    configuration: Configuration
    template_file_name: str

    @property
    def mode_directory(self) -> Path:
        return Path(Path.cwd(), self.configuration.source_folder)

    @property
    def ddl_directory(self) -> Path:
        return Path(Path.cwd(), "template_ddl")

    @property
    def file_ddl(self) -> Path:
        return Path(f'{self.ddl_directory}', f'{self.configuration.current_timestamp}_{self.template_file_name}')

    def update_ddl(self, cwd: Path = Path.cwd()) -> None:
        '''
        The function writes a text file with a list of ddl operations
        according to the template with the current timestamp and the current project directory
        according to the configuration.
        '''

        with Path(f'{self.ddl_directory}', self.template_file_name).open('r') as example:
            template = example.read()

        change_list = {
            '{ROOT_DIRECTORY}': str(cwd).replace('\\', '/'),
            '{SOURCE_FOLDER}': self.configuration.source_folder,
            '{CURRENT_DATE_%Y-%m-%d}': self.configuration.current_date,
            '{CURRENT_TIMESTAMP}': self.configuration.current_timestamp,
            '{DB_NAME}': f'{self.configuration.db_name}_{self.configuration.mode.value}',
        }

        for old, new in change_list.items():
            template = template.replace(old, new)

        with self.file_ddl.open('w') as ddl_file:
            ddl_file.write(template)

        _LOGGER.debug('''text file with a list of ddl operations according to the template was created successful''')

    def run_ddl(self, spark: SparkSession) -> Optional[dataframe.DataFrame]:
        '''
        the function runs ddl operations from the text file with a list of ddl operations
        according to the template with the current time stamp
        and the current project directory according to the configuration.
        Then function will remove this txt file
        '''
        try:
            with self.file_ddl.open('r') as ddl:
                _LOGGER.debug('''text file with a list of ddl operations is available''')
                for operation in ddl.read().split('\n\n'):
                    if operation.startswith("SELECT"):
                        return spark.sql(f'''{operation}''')
                    else:
                        spark.sql(f'''{operation}''')
        finally:
            remove(f'{self.file_ddl}')
        return None
