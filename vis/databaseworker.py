from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Union
from json import loads

from pymongo import MongoClient

from .dataframeworker import check_resulting_file
from vis.configuration import Configuration


@dataclass
class DatabaseWorker:
    client: MongoClient
    collection: MongoClient
    configuration: Configuration

    @classmethod
    def connect(cls, host: str, port: int, configuration: Configuration) -> DatabaseWorker:
        client = MongoClient(host, port)
        db = client[configuration.db_name]
        collection = db['variants']
        return cls(client=client, collection=collection, configuration=configuration)

    @check_resulting_file
    def write_to_mongodb(self, file_result: List[Union[str, bytes, bytearray]]) -> None:
        '''The function write data from json file into mongodb'''
        self.collection.insert_many([loads(row) for row in file_result])

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
