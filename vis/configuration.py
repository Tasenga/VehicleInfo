from __future__ import annotations
from pathlib import Path
from typing import Type, Dict, Any
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


class ConfigurationForProcessing(BaseModel):

    DEFAULT: Dict[str, Any]
    addition: Dict[str, Any]
    consumer: Dict[str, Any]
    esaco: Dict[str, Any]
    esajoin: Dict[str, Any]
    eurocol: Dict[str, Any]
    jwheel: Dict[str, Any]
    make: Dict[str, Any]
    manucol: Dict[str, Any]
    manufactor: Dict[str, Any]
    model: Dict[str, Any]
    pricehistory: Dict[str, Any]
    rims: Dict[str, Any]
    tcert: Dict[str, Any]
    technic: Dict[str, Any]
    txttable: Dict[str, Any]
    typ_envkv: Dict[str, Any]
    type: Dict[str, Any]
    typecol: Dict[str, Any]
    tyres: Dict[str, Any]

    @classmethod
    def from_file(cls: Type, file_path: Path) -> ConfigurationForProcessing:
        config = ConfigParser()
        config.optionxform = str  # type: ignore
        config.read(file_path)
        return cls(**config)
