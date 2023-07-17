from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from typing import Dict, List, Optional, Any
from pymongo.errors import (
    ConnectionFailure,
    PyMongoError,
    CollectionInvalid,
    OperationFailure,
)
import logging

logging.basicConfig(
    level=logging.DEBUG,
    filename="data.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)


class MongoDb:
    def __init__(self, host: str, port: str, db_name: Database) -> None:
        self.client: MongoClient = MongoClient(f"mongodb://{host}:{port}")
        self.db = self.client[f"{db_name}"]

    @classmethod
    def create_database(cls, host: str, port: str, db_name: str) -> "MongoDb":
        return cls(host=host, port=port, db_name=db_name)

    def create_collection(self, collection: Collection) -> Optional[bool]:
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                self.db.create_collection(collection)
                break
            except ConnectionFailure as e:
                logging.warning("Connection Error:", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
            except CollectionInvalid as e:
                logging.warning("We have occured erro while creating: ", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
            except PyMongoError as e:
                logging.warning("We encountered some problems: ", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
        else:
            logging.info("We hav reached maximum tries")
            return False

    def insert_item(self, collection: Collection, document: Dict[str, Any]) -> None:
        this_collection = self.db[f"{collection}"]
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                result = this_collection.insert_one(document)
                logging.info(f"Inserted document {result.inserted_id}")
                break
            except ConnectionFailure as e:
                logging.warning("Connection Error:", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
            except CollectionInvalid as e:
                logging.warning("We have occured erro while creating: ", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
            except PyMongoError as e:
                logging.warning("We encountered some problems: ", str(e))
                retries += 1
                logging.info(f"Retrying... (Attempt {retries}/{max_retries})")
                return False
        else:
            logging.info("We hav reached maximum tries")
            return False

    def apply_validation_rule(
        self, collection: Collection, validation_rule: Dict[str, Any]
    ) -> None:
        this_collection = self.db[f"{collection}"]
        try:
            self.db.command("collMod", this_collection.name, **validation_rule)
            logging.info("Validation rule created")
        except OperationFailure as e:
            logging.warning(f"Failed to enable schema validation: {e}")

    def filter_documents(
        self, collection: Collection, filter_criteria: Dict[str, Any]
    ) -> Optional[Cursor]:
        try:
            this_collection = self.db[f"{collection}"]
            pipeline = [{"$match": filter_criteria}]
            return this_collection.aggregate(pipeline)
        except CollectionInvalid as e:
            logging.info(f"There are no such Collection!", str(e))
            return None
        except ConnectionFailure as e:
            logging.info(f"We have connection issues!", str(e))
            return None
        except PyMongoError as e:
            logging.info(f"We have unknown issues!", str(e))
            return None

    def sort_documents(
        self, collection: Collection, sort_criteria: Dict[str, int]
    ) -> Optional[Cursor]:
        try:
            this_collection = self.db[f"{collection}"]
            pipeline = [{"$sort": sort_criteria}]
            return this_collection.aggregate(pipeline)
        except CollectionInvalid as e:
            logging.info(f"There are no such Collection!", str(e))
            return None
        except ConnectionFailure as e:
            logging.info(f"We have connection issues!", str(e))
            return None
        except PyMongoError as e:
            logging.info(f"We have unknown issues!", str(e))
            return None

    def project_documents(
        self, collection: Collection, projection_criteria: Dict[str, Any]
    ) -> Optional[Cursor]:
        try:
            this_collection = self.db[f"{collection}"]
            pipeline = [{"$project": projection_criteria}]
            return this_collection.aggregate(pipeline)
        except CollectionInvalid as e:
            logging.info(f"There are no such Collection!", str(e))
            return None
        except ConnectionFailure as e:
            logging.info(f"We have connection issues!", str(e))
            return None
        except PyMongoError as e:
            logging.info(f"We have unknown issues!", str(e))
            return None

    def unique_search_task_four(
        self, collection: Collection, pipline: Dict[str, Any]
    ) -> Optional[Cursor]:
        try:
            this_collection = self.db[f"{collection}"]
            return this_collection.aggregate(pipline)
        except CollectionInvalid as e:
            logging.info(f"There are no such Collection!", str(e))
            return None
        except ConnectionFailure as e:
            logging.info(f"We have connection issues!", str(e))
            return None
        except PyMongoError as e:
            logging.info(f"We have unknown issues!", str(e))
            return None
