import uuid
from unittest.mock import patch

import pymongo
import pytest
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo import ReadPreference
from pymongo.errors import CollectionInvalid, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database


def test_create_collection_forwards_to_pymongo():
    with DBClient("mongodb://localhost", connect=False) as client:
        database = Database(client, "create_collection_unit_test")
        codec_options = CodecOptions(document_class=SON)
        read_preference = ReadPreference.SECONDARY
        write_concern = WriteConcern(w=2)
        read_concern = ReadConcern("majority")
        session = object()
        parent_collection = pymongo.collection.Collection(
            database,
            "created",
            codec_options=codec_options,
            read_preference=read_preference,
            write_concern=write_concern,
            read_concern=read_concern,
        )

        with patch.object(
            pymongo.database.Database,
            "create_collection",
            autospec=True,
            return_value=parent_collection,
        ) as parent_create:
            result = database.create_collection(
                "created",
                codec_options=codec_options,
                read_preference=read_preference,
                write_concern=write_concern,
                read_concern=read_concern,
                session=session,
                check_exists=False,
                capped=True,
                size=4096,
            )

        parent_create.assert_called_once_with(
            database,
            "created",
            codec_options=codec_options,
            read_preference=read_preference,
            write_concern=write_concern,
            read_concern=read_concern,
            session=session,
            check_exists=False,
            capped=True,
            size=4096,
        )
        assert isinstance(result, Collection)
        assert result.name == "created"
        assert result.codec_options == codec_options
        assert result.read_preference == read_preference
        assert result.write_concern == write_concern
        assert result.read_concern == read_concern


def test_create_collection_propagates_pymongo_error():
    with DBClient("mongodb://localhost", connect=False) as client:
        database = Database(client, "create_collection_error_test")
        error = OperationFailure("create failed", code=123)

        with patch.object(
            pymongo.database.Database,
            "create_collection",
            autospec=True,
            side_effect=error,
        ) as parent_create:
            with pytest.raises(OperationFailure) as exc_info:
                database.create_collection("created")

        assert exc_info.value is error
        parent_create.assert_called_once_with(
            database,
            "created",
            codec_options=None,
            read_preference=None,
            write_concern=None,
            read_concern=None,
            session=None,
            check_exists=True,
        )


def test_create_collection_with_mongodb():
    with DBClient("mongodb://localhost", serverSelectionTimeoutMS=5000) as client:
        client.admin.command("ping")
        database_name = f"create_collection_{uuid.uuid4().hex}"
        database = Database(client, database_name)
        validator = {"value": {"$type": "int"}}

        try:
            collection = database.create_collection(
                "configured",
                validator=validator,
                validationLevel="strict",
            )
            assert isinstance(collection, Collection)
            assert collection.name == "configured"

            collection_info = database.command(
                {"listCollections": 1, "filter": {"name": "configured"}}
            )["cursor"]["firstBatch"][0]
            assert collection_info["options"]["validator"] == validator
            assert collection_info["options"]["validationLevel"] == "strict"

            with pytest.raises(CollectionInvalid):
                database.create_collection("configured")

            with client.start_session() as session:
                session_collection = database.create_collection(
                    "with_session", session=session
                )
            assert isinstance(session_collection, Collection)
            assert "with_session" in database.list_collection_names()
        finally:
            client.drop_database(database_name)
