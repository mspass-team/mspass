import pymongo


class Collection(pymongo.database.Collection):
    """
    A modified Mongo collection class.

    The native collection class of Mongo does not have __setstate__ and
    __getstate__ defined, which prevents it from being serialized. We add
    the two methods here as an addition so that the collection can be passed
    around by the scheduler and worker.
    """

    def __init__(self, *args, **kwargs):
        super(Collection, self).__init__(*args, **kwargs)

    def __getstate__(self):
        ret = self.__dict__.copy()
        ret["_BaseObject__codec_options"] = self.codec_options.__repr__()

        # Store database connection info separately to recreate Database handle
        # Use public attribute 'database' instead of private '_Collection__database'
        db = self.database  # pymongo 4.x uses public attribute
        if hasattr(db, "name"):
            ret["_mspass_db_name"] = db.name
        if hasattr(db, "client") and hasattr(db.client, "_mspass_db_host"):
            ret["_mspass_db_host"] = db.client._mspass_db_host

        # Don't pickle the Database object itself
        if "_Collection__database" in ret:
            del ret["_Collection__database"]

        return ret

    def __setstate__(self, data):
        from bson.codec_options import CodecOptions, TypeRegistry, DatetimeConversion
        from bson.binary import UuidRepresentation

        # Extract connection info before updating __dict__
        db_host = data.pop("_mspass_db_host", None)
        db_name = data.pop("_mspass_db_name", None)

        # Pop ALL codec_options fields (Collection also has _codec_options and _BaseObject__codec_options)
        codec_options_repr = data.pop("_BaseObject__codec_options", None)
        base_codec_options = data.pop("_codec_options", None)

        # Eval the codec_options repr string
        if codec_options_repr and isinstance(codec_options_repr, str):
            codec_options_obj = eval(codec_options_repr)
        else:
            codec_options_obj = codec_options_repr

        # Update attributes (now data has NO codec_options fields)
        self.__dict__.update(data)

        # Set BOTH codec_options attributes with the same object
        self._BaseObject__codec_options = codec_options_obj
        self._codec_options = codec_options_obj

        # Recreate Database handle if we have connection info
        if db_host is not None and db_name is not None:
            from mspasspy.db.client import DBClient
            from mspasspy.db.database import Database

            client = DBClient(db_host)
            self._Collection__database = Database(client, db_name)

    def __getitem__(self, name):
        return Collection(
            self.database,
            "%s.%s" % (self.name, name),
            False,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )

    def with_options(
        self,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        return Collection(
            self.database,
            self.name,
            False,
            codec_options or self.codec_options,
            read_preference or self.read_preference,
            write_concern or self.write_concern,
            read_concern or self.read_concern,
        )
