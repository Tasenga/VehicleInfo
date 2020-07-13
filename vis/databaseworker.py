from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict
from json import loads

from pymongo import MongoClient

from .dataframeworker import DataFrameWorker


@dataclass
class DatabaseWorker:
    client: MongoClient
    collection: MongoClient
    table: DataFrameWorker

    @classmethod
    def connect(
        cls, host: str, port: int, table: DataFrameWorker
    ) -> DatabaseWorker:
        client = MongoClient(host, port)
        db = client[table.configuration.db_name]
        collection = db['variants']
        return cls(client=client, collection=collection, table=table)

    def write_to_mongodb(self) -> None:
        '''The function write with one json
        file with all source table rows via one partition'''
        with self.client:
            self.collection.insert_many(
                [loads(row) for row in self.table.table.toJSON().collect()]
            )

    def read_from_mongodb(self) -> List[Dict]:
        '''The function reads mongodb collection with
        results and returns list of dictionaries'''
        with self.client:
            return [row for row in self.collection.find()]

    def drop_collection_mongodb(self) -> None:
        '''The functions deletes collection without
        without the ability to recover it'''
        with self.client:
            self.collection.drop()
