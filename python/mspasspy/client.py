import os
import pymongo
from urllib.parse import urlsplit

from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
except Exception as err:
    SparkConf = None
    SparkContext = None
    SparkSession = None
    _mspasspy_has_pyspark = False
    _mspasspy_pyspark_import_error = err
else:
    _mspasspy_has_pyspark = True
    _mspasspy_pyspark_import_error = None

try:
    from dask.distributed import Client as DaskClient
except ImportError as err:
    DaskClient = None
    MongoDBWorker = None
    _mspasspy_has_dask_distributed = False
    _mspasspy_dask_import_error = err
else:
    from mspasspy.util.db_utils import MongoDBWorker

    _mspasspy_has_dask_distributed = True
    _mspasspy_dask_import_error = None

from mspasspy.ccore.utility import MsPASSError


def _require_pyspark():
    if _mspasspy_has_pyspark:
        return

    message = "Spark scheduler was requested, but PySpark could not be imported"
    if _mspasspy_pyspark_import_error is not None:
        message += ": " + str(_mspasspy_pyspark_import_error)
    raise MsPASSError(message + ".", "Fatal")


def _require_dask():
    if _mspasspy_has_dask_distributed:
        return

    message = "Dask scheduler was requested, but dask.distributed could not be imported"
    if _mspasspy_dask_import_error is not None:
        message += ": " + str(_mspasspy_dask_import_error)
    raise MsPASSError(message + ".", "Fatal")


def _is_local_spark_master(master):
    return master == "local" or (
        isinstance(master, str) and master.startswith("local[") and master.endswith("]")
    )


def _build_scheduler_endpoint(
    scheduler_address,
    scheduler_port=None,
    default_port=None,
    default_scheme=None,
):
    endpoint = scheduler_address
    if default_scheme == "spark" and _is_local_spark_master(endpoint):
        return endpoint
    if default_scheme and "://" not in endpoint:
        endpoint = default_scheme + "://" + endpoint

    parse_target = endpoint if "://" in endpoint else "//" + endpoint
    parsed_address = urlsplit(parse_target)
    try:
        embedded_port = parsed_address.port
    except ValueError as err:
        raise MsPASSError(
            "Invalid scheduler address: " + scheduler_address,
            "Fatal",
        ) from err

    if embedded_port is not None:
        return endpoint

    if scheduler_port is None or scheduler_port == "":
        scheduler_port = default_port
    if scheduler_port is None or scheduler_port == "":
        return endpoint

    parsed_address = parsed_address._replace(
        netloc=parsed_address.netloc + ":" + str(scheduler_port)
    )
    endpoint = parsed_address.geturl()
    if "://" not in scheduler_address:
        endpoint = endpoint.removeprefix("//")
    return endpoint


def _build_database_address(database_address, database_port=None):
    if database_port is None or database_port == "":
        return database_address

    has_scheme = "://" in database_address
    parsed_address = urlsplit(
        database_address if has_scheme else "//" + database_address
    )
    if parsed_address.port is not None:
        return database_address

    parsed_address = parsed_address._replace(
        netloc=parsed_address.netloc + ":" + str(database_port)
    )
    result = parsed_address.geturl()
    return result if has_scheme else result[2:]


def _build_dask_scheduler_address(scheduler_address, scheduler_port=None):
    return _build_scheduler_endpoint(
        scheduler_address,
        scheduler_port=scheduler_port,
        default_port="8786",
    )


class Client:
    """
    A client-side representation of MSPASS.

    This is the only client users should use in MSPASS. The client manages all the other clients or instances.
    It creates and manages a Database client.
    It creates and manages a Global Hisotry Manager.
    It creates and manages a scheduler(spark/dask)

    For the address and port of each client/instances, we first check the user specified parameters, if not then
    serach the environment varibales values, if not againm then use the default settings.

    An existing :class:`dask.distributed.Client` can be supplied with ``dask_client``.
    This is useful for externally managed Dask clusters, including Dask Gateway
    clusters.  The caller owns the external cluster and should keep it alive while
    the MsPASS client is using it.
    """

    def __init__(
        self,
        database_host=None,
        scheduler=None,
        scheduler_host=None,
        job_name="mspass",
        database_name="mspass",
        schema=None,
        collection=None,
        dask_client=None,
    ):
        # job_name should be a string
        if database_host is not None and not type(database_host) is str:
            raise MsPASSError(
                "database_host should be a string but "
                + str(type(database_host))
                + " is found.",
                "Fatal",
            )
        if scheduler is not None and scheduler not in ("dask", "spark", "none"):
            raise MsPASSError(
                "scheduler should be dask, spark, or none but "
                + str(scheduler)
                + " is found.",
                "Fatal",
            )
        if scheduler_host is not None and not type(scheduler_host) is str:
            raise MsPASSError(
                "scheduler_host should be a string but "
                + str(type(scheduler_host))
                + " is found.",
                "Fatal",
            )
        if job_name is not None and not type(job_name) is str:
            raise MsPASSError(
                "job_name should be a string but " + str(type(job_name)) + " is found.",
                "Fatal",
            )
        if database_name is not None and not type(database_name) is str:
            raise MsPASSError(
                "database_name should be a string but "
                + str(type(database_name))
                + " is found.",
                "Fatal",
            )
        # collection should be a string
        if collection is not None and type(collection) is not str:
            raise MsPASSError(
                "collection should be a string but "
                + str(type(collection))
                + " is found.",
                "Fatal",
            )
        if dask_client is not None:
            _require_dask()
            if scheduler == "none":
                raise MsPASSError(
                    "dask_client cannot be used when scheduler is none.",
                    "Fatal",
                )
            if scheduler == "spark":
                raise MsPASSError(
                    "dask_client can only be used with the dask scheduler.",
                    "Fatal",
                )
            if not _mspasspy_has_dask_distributed or not isinstance(
                dask_client, DaskClient
            ):
                raise MsPASSError(
                    "dask_client should be a dask.distributed.Client but "
                    + str(type(dask_client))
                    + " is found.",
                    "Fatal",
                )

        # check env variables
        MSPASS_DB_ADDRESS = os.environ.get("MSPASS_DB_ADDRESS")
        MONGODB_PORT = os.environ.get("MONGODB_PORT")
        MSPASS_SCHEDULER = os.environ.get("MSPASS_SCHEDULER")
        MSPASS_SCHEDULER_ADDRESS = os.environ.get("MSPASS_SCHEDULER_ADDRESS")
        DASK_SCHEDULER_PORT = os.environ.get("DASK_SCHEDULER_PORT")
        SPARK_MASTER_PORT = os.environ.get("SPARK_MASTER_PORT")

        if (
            dask_client is None
            and not _mspasspy_has_pyspark
            and (
                scheduler == "spark"
                or (scheduler is None and MSPASS_SCHEDULER == "spark")
            )
        ):
            _require_pyspark()
        if not _mspasspy_has_dask_distributed and (
            scheduler == "dask" or (scheduler is None and MSPASS_SCHEDULER == "dask")
        ):
            _require_dask()

        # create a database client
        # priority: parameter -> env -> default
        if database_host:
            database_address = database_host
        elif MSPASS_DB_ADDRESS:
            database_address = MSPASS_DB_ADDRESS
        else:
            database_address = "127.0.0.1"
        database_address = _build_database_address(database_address, MONGODB_PORT)

        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            raise MsPASSError(
                "Runntime error: cannot create a database client with: "
                + database_address,
                "Fatal",
            )

        # set default database name
        self._default_database_name = database_name
        self._default_schema = schema
        self._default_collection = collection

        # create a Global History Manager
        global_history_manager_db = Database(
            self._db_client, database_name, schema=schema
        )
        self._global_history_manager = GlobalHistoryManager(
            global_history_manager_db, job_name, collection=collection
        )

        # set scheduler
        self._scheduler_disabled = False
        if dask_client is not None:
            self._scheduler = "dask"
        elif scheduler:
            if scheduler == "none":
                self._scheduler = None
                self._scheduler_disabled = True
            else:
                self._scheduler = scheduler
        elif MSPASS_SCHEDULER:
            if MSPASS_SCHEDULER not in ("dask", "spark", "none"):
                raise MsPASSError(
                    "MSPASS_SCHEDULER should be dask, spark, or none but "
                    + str(MSPASS_SCHEDULER)
                    + " is found.",
                    "Fatal",
                )
            if MSPASS_SCHEDULER == "none":
                self._scheduler = None
                self._scheduler_disabled = True
            else:
                self._scheduler = MSPASS_SCHEDULER
        else:
            if _mspasspy_has_dask_distributed:
                self._scheduler = "dask"
            elif _mspasspy_has_pyspark:
                self._scheduler = "spark"
            else:
                self._scheduler = None

        # scheduler configuration
        if self._scheduler == "spark":
            if scheduler_host:
                scheduler_address = scheduler_host
            elif MSPASS_SCHEDULER_ADDRESS:
                scheduler_address = MSPASS_SCHEDULER_ADDRESS
            else:
                scheduler_address = None

            implicit_local_scheduler = scheduler_address is None
            if implicit_local_scheduler:
                spark_master_url = "local"
            else:
                spark_master_url = _build_scheduler_endpoint(
                    scheduler_address,
                    scheduler_port=SPARK_MASTER_PORT,
                    default_scheme="spark",
                )

            # sanity check
            try:
                spark_context, spark_context_owned = self._create_spark_context(
                    spark_master_url,
                    allow_existing_local=implicit_local_scheduler,
                )
            except Exception as err:
                raise MsPASSError(
                    "Runntime error: cannot create a spark configuration with: "
                    + spark_master_url,
                    "Fatal",
                ) from err
            self._commit_spark_scheduler(
                spark_context,
                spark_context.master,
                owned=spark_context_owned,
            )

        elif self._scheduler == "dask":
            # if no defind scheduler_host and no MSPASS_SCHEDULER_ADDRESS, use local cluster to create a client
            if dask_client is not None:
                try:
                    self._register_dask_plugin(dask_client)
                except Exception as err:
                    raise MsPASSError(
                        "Runntime error: cannot configure the provided dask client",
                        "Fatal",
                    ) from err
                self._commit_dask_scheduler(dask_client, None, owned=False)
            elif not scheduler_host and not MSPASS_SCHEDULER_ADDRESS:
                try:
                    new_dask_client = self._create_dask_client()
                except Exception as err:
                    raise MsPASSError(
                        "Runntime error: cannot create a local dask client",
                        "Fatal",
                    ) from err
                self._commit_dask_scheduler(new_dask_client, None, owned=True)
            else:
                if scheduler_host:
                    scheduler_address = scheduler_host
                else:
                    scheduler_address = MSPASS_SCHEDULER_ADDRESS

                dask_client_address = _build_dask_scheduler_address(
                    scheduler_address, DASK_SCHEDULER_PORT
                )
                # sanity check
                try:
                    new_dask_client = self._create_dask_client(dask_client_address)
                except Exception as err:
                    raise MsPASSError(
                        "Runntime error: cannot create a dask client with: "
                        + dask_client_address,
                        "Fatal",
                    ) from err
                self._commit_dask_scheduler(
                    new_dask_client, dask_client_address, owned=True
                )
        elif not self._scheduler_disabled:
            print("There is no spark or dask installed, this client has no scheduler")

    def _register_dask_plugin(self, dask_client):
        mongo_plugin = MongoDBWorker(self, dbclient_key="dbclient")
        dask_client.register_plugin(mongo_plugin, name="mongodb_worker")

    def _create_dask_client(self, scheduler_address=None):
        _require_dask()
        if scheduler_address is None:
            dask_client = DaskClient()
        else:
            dask_client = DaskClient(scheduler_address)
        try:
            self._register_dask_plugin(dask_client)
        except Exception:
            dask_client.close()
            raise
        return dask_client

    @staticmethod
    def _create_spark_context(spark_master_url, allow_existing_local=False):
        previous_context = getattr(SparkContext, "_active_spark_context", None)
        spark = (
            SparkSession.builder.appName("mspass")
            .master(spark_master_url)
            .getOrCreate()
        )
        spark_context = spark.sparkContext
        reused_context = spark_context is previous_context
        compatible_existing_local = (
            allow_existing_local
            and reused_context
            and _is_local_spark_master(spark_context.master)
        )
        if spark_context.master != spark_master_url and not compatible_existing_local:
            if not reused_context:
                spark_context.stop()
            raise RuntimeError(
                "SparkSession.getOrCreate() returned master "
                + str(spark_context.master)
                + " instead of "
                + spark_master_url
            )
        return spark_context, not reused_context

    def _commit_dask_scheduler(self, dask_client, scheduler_address, owned):
        old_scheduler = getattr(self, "_scheduler", None)
        old_dask_client = getattr(self, "_dask_client", None)
        old_dask_owned = getattr(self, "_dask_client_owned", False)
        old_spark_context = getattr(self, "_spark_context", None)
        old_spark_owned = getattr(self, "_spark_context_owned", False)

        self._scheduler = "dask"
        self._scheduler_disabled = False
        self._dask_client = dask_client
        self._dask_client_address = scheduler_address
        self._dask_client_owned = owned
        for attribute in (
            "_spark_context",
            "_spark_master_url",
            "_spark_context_owned",
        ):
            if hasattr(self, attribute):
                delattr(self, attribute)

        if old_scheduler == "dask" and old_dask_client is not None and old_dask_owned:
            old_dask_client.close()
        elif (
            old_scheduler == "spark"
            and old_spark_context is not None
            and old_spark_owned
        ):
            old_spark_context.stop()

    def _commit_spark_scheduler(self, spark_context, spark_master_url, owned):
        old_scheduler = getattr(self, "_scheduler", None)
        old_dask_client = getattr(self, "_dask_client", None)
        old_dask_owned = getattr(self, "_dask_client_owned", False)
        old_spark_context = getattr(self, "_spark_context", None)
        old_spark_owned = getattr(self, "_spark_context_owned", False)

        self._scheduler = "spark"
        self._scheduler_disabled = False
        self._spark_context = spark_context
        self._spark_master_url = spark_master_url
        self._spark_context_owned = owned
        for attribute in (
            "_dask_client",
            "_dask_client_address",
            "_dask_client_owned",
        ):
            if hasattr(self, attribute):
                delattr(self, attribute)

        if old_scheduler == "dask" and old_dask_client is not None and old_dask_owned:
            old_dask_client.close()
        elif (
            old_scheduler == "spark"
            and old_spark_context is not None
            and old_spark_owned
        ):
            old_spark_context.stop()

    def get_database_client(self):
        """
        Get the database client in the global history manager

        :return: :class:`mspasspy.db.database.Database`
        """
        return self._db_client

    def get_database(self, database_name=None):
        """
        Get a database by database_name, if database_name is not specified, use the default one

        :param database_name: the name of database
        :type database_name: :class:`str`
        :return: :class:`mspasspy.db.database.Database`
        """
        if not database_name:
            database_name = self._default_database_name
        return Database(self._db_client, database_name, schema=self._default_schema)

    def get_global_history_manager(self):
        """
        Get the global history manager with this client

        :return: :class:`mspasspy.global_history.manager.GlobalHistoryManager`
        """
        return self._global_history_manager

    def get_scheduler(self):
        """
        Get the scheduler(spark/dask) with this client

        :return: :class:`pyspark.SparkContext`/:class:`dask.distributed.Client`/None
        """
        if self._scheduler == "spark":
            return self._spark_context
        elif self._scheduler == "dask":
            return self._dask_client
        elif self._scheduler_disabled:
            return None
        else:
            print(
                "There is no spark or dask installed, this client has no scheduler, returned None"
            )
            return None

    def close_scheduler(self):
        """Detach the active scheduler and release it when owned by this client.

        A local Dask client or Spark context created by this :class:`Client` is
        closed or stopped exactly once.  A Dask client supplied by the caller
        and a Spark context reused from the caller are detached but remain
        running.  Repeated calls are no-ops.

        Scheduler state is cleared before an owned resource is released.  If
        ``close`` or ``stop`` raises, that exception is propagated and this
        client remains detached from the scheduler.
        """
        scheduler = getattr(self, "_scheduler", None)
        dask_client = getattr(self, "_dask_client", None)
        dask_owned = getattr(self, "_dask_client_owned", False)
        spark_context = getattr(self, "_spark_context", None)
        spark_owned = getattr(self, "_spark_context_owned", False)

        self._scheduler = None
        self._scheduler_disabled = True
        for attribute in (
            "_dask_client",
            "_dask_client_address",
            "_dask_client_owned",
            "_spark_context",
            "_spark_master_url",
            "_spark_context_owned",
        ):
            if hasattr(self, attribute):
                delattr(self, attribute)

        if scheduler == "dask" and dask_client is not None and dask_owned:
            dask_client.close()
        elif scheduler == "spark" and spark_context is not None and spark_owned:
            spark_context.stop()

    def set_database_client(self, database_host, database_port=None):
        """
        Replace the database client and its global-history database together.

        The replacement database client is connected and validated first.  A
        new history database and :class:`GlobalHistoryManager` are then built
        with the current history database name, schema objects, job name, and
        collection.  Only after all of those steps succeed are the database and
        history references committed to this client.  A failure therefore
        leaves the current database client, history manager, and scheduler
        unchanged.

        An explicit port is appended only when ``database_host`` does not
        already contain one.  This applies to bare hosts, MongoDB URIs, and
        bracketed IPv6 addresses.

        :param database_host: the host address of database client
        :type database_host: :class:`str`
        :param database_port: the port of database client
        :type database_port: :class:`str`

        :raises MsPASSError: if the replacement database client, history
            database, or history manager cannot be constructed and validated.
        """
        database_address = _build_database_address(database_host, database_port)
        replacement_db_client = None
        current_history_manager = self._global_history_manager
        current_history_db = current_history_manager.history_db
        try:
            replacement_db_client = DBClient(database_address)
            replacement_db_client.server_info()
            replacement_history_db = Database(
                replacement_db_client,
                current_history_db.name,
                db_schema=current_history_db.database_schema,
                md_schema=current_history_db.metadata_schema,
            )
            replacement_history_manager = GlobalHistoryManager(
                replacement_history_db,
                current_history_manager.job_name,
                collection=current_history_manager.collection,
            )
        except Exception as err:
            if replacement_db_client is not None:
                replacement_db_client.close()
            raise MsPASSError(
                "Runntime error: cannot create a database client with: "
                + database_address,
                "Fatal",
            ) from err

        self._db_client, self._global_history_manager = (
            replacement_db_client,
            replacement_history_manager,
        )

    def set_global_history_manager(self, history_db, job_name, collection=None):
        """
        Set a global history manager by history_db, job_name(and collection)

        :param history_db: the database will be set in the global history manager
        :type history_db: :class:`mspasspy.db.database.Database`
        :param job_name: the job name will be set in the global history manager
        :type job_name: :class:`str`
        :param collection: the collection name will be set in the history_db
        :type collection: :class:`str`
        """
        if not isinstance(history_db, Database):
            raise TypeError(
                "history_db should be a mspasspy.db.Database but "
                + str(type(history_db))
                + " is found."
            )
        if not type(job_name) is str:
            raise TypeError(
                "job_name should be a string but " + str(type(job_name)) + " is found."
            )
        if collection is not None and type(collection) is not str:
            raise TypeError(
                "collection should be a string but "
                + str(type(collection))
                + " is found."
            )

        self._global_history_manager = GlobalHistoryManager(
            history_db, job_name, collection=collection
        )

    def set_scheduler(self, scheduler, scheduler_host, scheduler_port=None):
        """
        Replace the scheduler used by this client.

        The replacement scheduler is constructed and validated before any
        scheduler state in this object is changed.  Consequently, a failure to
        construct or validate the replacement leaves the current scheduler and
        its ownership unchanged.  After a successful commit, a displaced Dask
        client or Spark context is closed only when it was created by this
        :class:`Client`; a Dask client supplied by the caller is never closed.

        Cleanup of an owned, displaced scheduler happens after the replacement
        is committed.  If its ``close`` or ``stop`` method raises, that exception
        is propagated and the replacement remains the active scheduler.  This
        distinction lets callers determine the active state with
        :meth:`get_scheduler` without mistaking a cleanup failure for a failed
        connection.

        Requesting the currently active Spark master is an idempotent no-op.
        Switching an active Spark context to another master is rejected because
        ``SparkSession.getOrCreate`` cannot guarantee that transition.

        :param scheduler: the scheduler type, should be either dask or spark
        :type scheduler: :class:`str`
        :param scheduler_host: the host address of scheduler
        :type scheduler_host: :class:`str`
        :param scheduler_port: the port of scheduler
        :type scheduler_port: :class:`str`

        :raises MsPASSError: if arguments are invalid, an optional scheduler
            dependency is unavailable, the replacement cannot be constructed or
            validated, or an active Spark master switch is requested.
        :raises Exception: propagates an exception raised while closing or
            stopping a displaced Client-owned scheduler after the replacement
            has been committed.
        """
        if scheduler != "dask" and scheduler != "spark":
            raise MsPASSError(
                "scheduler should be either dask or spark but "
                + str(scheduler)
                + " is found.",
                "Fatal",
            )
        if not type(scheduler_host) is str:
            raise MsPASSError(
                "scheduler_host should be a string but "
                + str(type(scheduler_host))
                + " is found.",
                "Fatal",
            )

        if scheduler == "spark":
            _require_pyspark()
            spark_master_url = _build_scheduler_endpoint(
                scheduler_host,
                scheduler_port=scheduler_port,
                default_scheme="spark",
            )
            if getattr(self, "_scheduler", None) == "spark":
                active_master = getattr(self, "_spark_master_url", None)
                if active_master is None:
                    active_master = getattr(
                        getattr(self, "_spark_context", None), "master", None
                    )
                if active_master == spark_master_url:
                    return
                raise MsPASSError(
                    "Runntime error: cannot create a spark configuration with: "
                    + spark_master_url
                    + "; refusing to change active Spark master from "
                    + str(active_master)
                    + " because SparkSession.getOrCreate() cannot guarantee the switch.",
                    "Fatal",
                )

            try:
                spark_context, spark_context_owned = self._create_spark_context(
                    spark_master_url
                )
            except Exception as err:
                raise MsPASSError(
                    "Runntime error: cannot create a spark configuration with: "
                    + spark_master_url,
                    "Fatal",
                ) from err
            self._commit_spark_scheduler(
                spark_context,
                spark_context.master,
                owned=spark_context_owned,
            )

        else:
            _require_dask()
            dask_client_address = _build_dask_scheduler_address(
                scheduler_host, scheduler_port
            )
            try:
                dask_client = self._create_dask_client(dask_client_address)
            except Exception as err:
                raise MsPASSError(
                    "Runntime error: cannot create a dask client with: "
                    + dask_client_address,
                    "Fatal",
                ) from err
            self._commit_dask_scheduler(dask_client, dask_client_address, owned=True)
