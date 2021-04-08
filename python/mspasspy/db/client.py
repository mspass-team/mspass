import pymongo
from mspasspy.db.database import Database

class DBClient(pymongo.MongoClient):
    """
    A client-side representation of MongoDB.

    This is a wrapper around the :class:`~pymongo.MongoClient` for convenience.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__default_database_name = self._MongoClient__default_database_name

    def __getitem__(self, name):
        """
        Get a database by name.
        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.
        :Parameters:
          - `name`: the name of the database to get
        """
        return Database(self, name)

    def get_default_database(self, default=None, codec_options=None,
        read_preference=None, write_concern=None, read_concern=None):
        if self.__default_database_name is None and default is None:
            raise pymongo.errors.ConfigurationError(
                'No default database name defined or provided.')

        return Database(
            self, self.__default_database_name or default, codec_options,
            read_preference, write_concern, read_concern)

    def get_database(self, name=None, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        if name is None:
            if self.__default_database_name is None:
                raise pymongo.errors.ConfigurationError('No default database defined')
            name = self.__default_database_name

        return Database(
            self, name, codec_options, read_preference,
            write_concern, read_concern)