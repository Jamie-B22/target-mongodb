"""MongoDB target class."""

from pathlib import Path
from typing import List

from singer_sdk.target_base import Target
from singer_sdk.sinks import Sink
from singer_sdk import typing as th

from target_mongodb.sinks import (
    MongoDbSink,
)


class TargetMongoDb(Target):
    """Sample target for MongoDB."""

    name = "target-mongodb"
    config_jsonschema = th.PropertiesList(
        th.Property("connection_string", th.StringType),
        th.Property("db_name", th.StringType)
    ).to_dict()
    default_sink_class = MongoDbSink

if __name__ == '__main__':
    TargetMongoDb.cli()    
