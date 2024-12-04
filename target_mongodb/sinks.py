"""MongoDB target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union

from singer_sdk.sinks import BatchSink

import requests
import urllib.parse
import datetime
import pymongo
from bson.objectid import ObjectId

class MongoDbSink(BatchSink):
    """MongoDB target sink class."""

    max_size = 1000000  # Set a smaller batch size for better scalability

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Get connection configs
        connection_string = self.config.get("connection_string")
        db_name = self.config.get("db_name", self.config.get("database"))
        config = self.config

        # Build connection string if not provided
        if not connection_string:
            if config["srv"]:
                connection_string = (
                    f"mongodb+srv://{config['user']}:{config['password']}@{config['host']}/{db_name}"
                    f"?authSource={config['auth_database']}"
                )
            else:
                if "port" in config:
                    connection_string = (
                        f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{db_name}"
                        f"?authSource={config['auth_database']}"
                    )
                else:
                    connection_string = (
                        f"mongodb://{config['user']}:{config['password']}@{config['host']}/{db_name}"
                        f"?authSource={config['auth_database']}"
                    )

        # Set the collection based on the current stream
        collection_name = urllib.parse.quote(self.stream_name)

        client = pymongo.MongoClient(
            connection_string, connectTimeoutMS=2000, retryWrites=True, ssl=config.get("ssl", False)
        )
        db = client[db_name]

        records = context["records"]

        if len(self.key_properties) > 0:
            primary_id = self.key_properties[0]
            bulk_operations = []

            for record in records:
                find_id = record.get(primary_id)

                # Handle ObjectId if the primary key is `_id`
                if primary_id == "_id":
                    try:
                        find_id = ObjectId(find_id)
                    except Exception as e:
                        self.logger.warn(f"Malformed id: {find_id}. Skipping this record. Error: {e}")
                        continue

                    record.pop("_id", None)

                # Create an UpdateOne operation with upsert=True
                bulk_operations.append(
                    pymongo.UpdateOne(
                        {primary_id: find_id},
                        {"$set": record},
                        upsert=True,
                    )
                )

            if bulk_operations:
                # Execute the bulk write operation
                result = db[collection_name].bulk_write(bulk_operations, ordered=False)
                self.logger.info(
                    f"Bulk write completed: {result.modified_count} updated, {result.upserted_count} upserted."
                )
        else:
            # If no primary key, insert all records
            db[collection_name].insert_many(records)

        self.logger.info(f"Uploaded {len(records)} records into {collection_name}")

        # Clean up records
        context["records"] = []
