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
class SQL:
    '''
    The object of the SQL class contains customer parameters to create, update
    and read customizing SQL (ddl, dml) operations to extract data from the data source to hive-tables.
    '''

    configuration: Configuration
    template_file_name: str

    @property
    def mode_directory(self) -> Path:
        return Path(Path.cwd(), self.configuration.source_folder)

    @property
    def sql_directory(self) -> Path:
        return Path(Path.cwd(), "template_sql")

    @property
    def file_sql(self) -> Path:
        return Path(f'{self.sql_directory}', f'{self.configuration.current_timestamp}_{self.template_file_name}')

    def update_sql(self, cwd: Path = Path.cwd()) -> None:
        '''
        The function writes a text file with a list of sql operations
        according to the template with the current timestamp and the current project directory
        according to the configuration.
        '''

        with Path(f'{self.sql_directory}', self.template_file_name).open('r') as example:
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

        with self.file_sql.open('w') as sql_file:
            sql_file.write(template)

        _LOGGER.debug('''text file with a list of sql operations according to the template was created successful''')

    def run_sql(self, spark: SparkSession) -> Optional[dataframe.DataFrame]:
        '''
        the function runs sql operations from the text file with a list of sql operations
        according to the template with the current time stamp
        and the current project directory according to the configuration.
        Then function will remove this txt file
        '''
        result = None
        try:
            with self.file_sql.open('r') as sql:
                _LOGGER.debug('''text file with a list of sql operations is available''')
                for operation in sql.read().split('\n\n'):
                    if operation.startswith("SELECT"):
                        result = spark.sql(f'''{operation}''')
                    else:
                        spark.sql(f'''{operation}''')
        finally:
            remove(f'{self.file_sql}')
        return result
