from __future__ import annotations
from pathlib import Path
from typing import Type
from enum import Enum
import logging
from configparser import ConfigParser
from datetime import datetime

from pydantic import BaseModel


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


class Mode(Enum):
    test = "test"
    main = "main"


class Configuration(BaseModel):
    ''''''

    mode: Mode = Mode.main
    mode_folder: Mode = Mode.main
    db_name: str = 'schwacke'
    current_date: str = str(datetime.now().date())
    current_timestamp: str = str(int(datetime.now().timestamp()))

    @classmethod
    def from_file(cls: Type, file_path: Path) -> Configuration:
        '''# ...'''
        config = ConfigParser()
        config.read(file_path)
        return cls(
            mode=config['PARAMETRS']['mode'],
            mode_folder=config['PARAMETRS']['mode'],
            db_name=config['PARAMETRS']['db_name'],
        )
