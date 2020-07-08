from pathlib import Path
from dataclasses import dataclass
import logging

from pyspark.sql import SparkSession

from .configuration import Configuration


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


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
        return Path(Path.cwd(), self.configuration.source_folder)

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
            '{SOURCE_FOLDER}': self.configuration.source_folder,
            '{CURRENT_DATE_%Y-%m-%d}': self.configuration.current_date,
            '{CURRENT_TIMESTAMP}': self.configuration.current_timestamp,
            '{DB_NAME}': self.configuration.db_name,
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

    def run_ddl(self, spark: SparkSession) -> None:
        '''
        the function runs ddl operations
        from the text file with a list of ddl operations
        according to the template with the current time stamp
        and the current project directory according to the configuration
        '''
        with Path(self.mode_directory, 'schwacke_hive_tables_ddl.txt').open(
            'r'
        ) as ddl:
            _LOGGER.debug(
                '''text file with a list of ddl operations is available'''
            )
            for operation in ddl.read().split('\n\n'):
                spark.sql(f'''{operation}''')
