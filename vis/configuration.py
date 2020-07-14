from __future__ import annotations
from pathlib import Path
from typing import Type
import logging
from configparser import ConfigParser
from enum import Enum
from datetime import datetime

from pydantic import BaseModel


_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)


class Mode(Enum):
    short = "short"
    full = "full"


class Configuration(BaseModel):

    mode: Mode = Mode.full
    source_folder: str
    db_name: str
    host: str
    port: int
    current_date: str = str(datetime.now().date())
    current_timestamp: str = str(int(datetime.now().timestamp()))

    @classmethod
    def from_file(cls: Type, file_path: Path) -> Configuration:
        config = ConfigParser()
        config.read(file_path)
        return cls(**config['PARAMETERS'])
