from __future__ import annotations
from pymongo import MongoClient
from dataclasses import dataclass
from typing import List
from json import loads

from .df_worker import DF_WORKER


@dataclass
class DB_WORKER:
    collection: MongoClient
    table: DF_WORKER

    @classmethod
    def connect(cls, host: str, port: int, table: DF_WORKER) -> DB_WORKER:
        client = MongoClient(host, port)
        db = client[table.configuration.db_name]
        collection = db['variants']
        return cls(collection=collection, table=table)

    def write_to_mongodb(self) -> None:
        '''The function write with one json
        file with all source table rows via one partition'''

        self.collection.insert_many(
            [loads(row) for row in self.table.table.toJSON().collect()]
        )

    def read_from_mongodb(self) -> List:
        '''The function reads mongodb collection with
        results and returns list of dictionaries'''
        return [row for row in self.collection.find()]

    def drop_collection_mongodb(self) -> None:
        '''The functions deletes collection without
        without the ability to recover it'''
        self.collection.drop()
