import pymongo
from pymongo import uri_parser
from typing import Any
from mspasspy.db.database import Database


class DBClient(pymongo.MongoClient):
    """
    A client-side top-level handle into MongoDB.

    MongoDB uses the client server model for transactions.   An instance
    of this class must be created in any MsPASS job using the MongoDB
    database to set up the communciation channels between the you
    (the client) and an instance of the MongoDB server.
    This class is a little more han a wrapper around the
    :class:`~pymongo.MongoClient` created for convenience.
    In most cases there is functionally little difference from
    creating a MongoClient or the MsPASS DBClient (this class).
    """

    def __init__(self, host=None, *args, **kwargs):
        super().__init__(host=host, *args, **kwargs)
        self.__default_database_name = None
        if host and isinstance(host, str):
            try:
                parsed_uri = uri_parser.parse_uri(host)
                self.__default_database_name = parsed_uri.get('database')
            except Exception:
                pass
        self._mspass_db_host = host
        # Store args/kwargs for pickle reconstruction
        self._mspass_connection_args = args
        self._mspass_connection_kwargs = kwargs

    def _repr_helper(self) -> str:
        def option_repr(option: str, value: Any) -> str:
            """Fix options whose __repr__ isn't usable in a constructor."""
            if option == "document_class":
                if value is dict:
                    return "document_class=dict"
                else:
                    return f"document_class={value.__module__}.{value.__name__}"
            if option in pymongo.common.TIMEOUT_OPTIONS and value is not None:
                return f"{option}={int(value * 1000)}"

            return f"{option}={value!r}"

        # Host first...
        if self._mspass_db_host:
            options = ["host='{}'".format(self._mspass_db_host)]
        else:
            options = [
                "host=%r"
                % [
                    "%s:%d" % (host, port) if port is not None else host
                    for host, port in self._topology_settings.seeds
                ]
            ]
        # ... then everything in self._constructor_args...
        options.extend(
            option_repr(key, self.options._options[key])
            for key in self._constructor_args
        )
        # ... then everything else.
        options.extend(
            option_repr(key, self.options._options[key])
            for key in self.options._options
            if key not in set(self._constructor_args)
            and key != "username"
            and key != "password"
        )
        return ", ".join(options)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._repr_helper()})"

    def __getstate__(self):
        """
        Pickle only connection parameters, not the active MongoClient internals.
        This prevents thread lock serialization issues.
        """
        return {
            'host': self._mspass_db_host,
            'args': getattr(self, '_mspass_connection_args', ()),
            'kwargs': getattr(self, '_mspass_connection_kwargs', {}),
            'default_database_name': self.__default_database_name,
        }

    def __setstate__(self, state):
        """
        Restore DBClient from pickled state by creating a new connection.
        The new connection is created via __init__, which will have its own
        thread management, but it's a fresh object that can be pickled again.
        """
        # Extract parameters
        host = state.get('host')
        args = state.get('args', ())
        kwargs = state.get('kwargs', {})
        
        # Reinitialize (creates new connection)
        self.__init__(host, *args, **kwargs)
        
        # Restore default database name if it was overridden
        saved_db_name = state.get('default_database_name')
        if saved_db_name is not None:
            self.__default_database_name = saved_db_name

    def __getitem__(self, name):
        """
        Get a database by name.
        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.
        :Parameters:
          - `name`: the name of the database to get
        """
        return Database(self, name)

    def get_default_database(
        self,
        default=None,
        schema=None,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        if self.__default_database_name is None and default is None:
            raise pymongo.errors.ConfigurationError(
                "No default database name defined or provided."
            )

        return Database(
            self,
            self.__default_database_name or default,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
            schema=schema,
        )

    def get_database(
        self,
        name=None,
        schema=None,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        if name is None:
            if self.__default_database_name is None:
                raise pymongo.errors.ConfigurationError("No default database defined")
            name = self.__default_database_name

        return Database(
            self,
            name,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
            schema=schema,
        )
