import os
import pymongo

from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from mspasspy.ccore.utility import MsPASSError
from dask.distributed import Client as DaskClient

class Client:
    """
    A client-side representation of MSPASS.

    This is the only client users should use in MSPASS. The client manages all the other clients or instances.
    It creates and manages a Database client.
    It creates and manages a Global Hisotry Manager.
    It creates and manages a scheduler(spark/dask)

    For the address and port of each client/instances, we first check the user specified parameters, if not then
    serach the environment varibales values, if not againm then use the default settings.
    """
    def __init__(self, database_host=None, scheduler=None, scheduler_host=None, job_name='mspass', database_name='mspass', schema=None, collection=None):
        # job_name should be a string
        if database_host is not None and not type(database_host) is str:
            raise MsPASSError('database_host should be a string but ' + str(type(database_host)) +  ' is found.', 'Fatal')
        if scheduler is not None and scheduler != 'dask' and scheduler != 'spark':
            raise MsPASSError('scheduler should be either dask or spark but ' + str(scheduler) +  ' is found.', 'Fatal')
        if scheduler_host is not None and not type(scheduler_host) is str:
            raise MsPASSError('scheduler_host should be a string but ' + str(type(scheduler_host)) +  ' is found.', 'Fatal')
        if job_name is not None and not type(job_name) is str:
            raise MsPASSError('job_name should be a string but ' + str(type(job_name)) +  ' is found.', 'Fatal')
        if database_name is not None and not type(database_name) is str:
            raise MsPASSError('database_name should be a string but ' + str(type(database_name)) +  ' is found.', 'Fatal')
        # collection should be a string
        if collection is not None and type(collection) is not str:
            raise MsPASSError('collection should be a string but ' + str(type(collection)) + ' is found.', 'Fatal')

        # check env variables
        MSPASS_DB_ADDRESS = os.environ.get('MSPASS_DB_ADDRESS')
        MONGODB_PORT = os.environ.get('MONGODB_PORT')
        MSPASS_SCHEDULER = os.environ.get('MSPASS_SCHEDULER')
        MSPASS_SCHEDULER_ADDRESS = os.environ.get('MSPASS_SCHEDULER_ADDRESS')
        DASK_SCHEDULER_PORT = os.environ.get('DASK_SCHEDULER_PORT')
        SPARK_MASTER_PORT = os.environ.get('SPARK_MASTER_PORT')

        # create a database client
        # priority: parameter -> env -> default
        database_host_has_port = False
        if database_host:
            database_address = database_host
            # check if database_host contains port number already
            if ':' in database_address:
                database_host_has_port = True

        elif MSPASS_DB_ADDRESS:
            database_address = MSPASS_DB_ADDRESS
        else:
            database_address = 'localhost'
        # add port
        if not database_host_has_port and MONGODB_PORT:
            database_address += ':' + MONGODB_PORT

        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            raise MsPASSError('Runntime error: cannot create a database client with: ' + database_address, 'Fatal')

        # set default database name
        self._default_database_name = database_name
        self._default_schema = schema
        self._default_collection = collection

        # create a Global History Manager
        if schema:
            global_history_manager_db = Database(self._db_client, database_name, db_schema=schema)
        else:
            global_history_manager_db = Database(self._db_client, database_name)
        self._global_history_manager = GlobalHistoryManager(global_history_manager_db, job_name, collection=collection)

        # set scheduler
        if scheduler:
            self._scheduler = scheduler
        elif MSPASS_SCHEDULER:
            self._scheduler = MSPASS_SCHEDULER
        else:
            self._scheduler = 'dask'

        # scheduler configuration
        if self._scheduler == 'spark':
            scheduler_host_has_port = False
            if scheduler_host:
                self._spark_master_url = scheduler_host
                # add spark:// prefix if not exist
                if 'spark://' not in scheduler_host:
                    self._spark_master_url = 'spark://' + self._spark_master_url
                # check if spark host address contains port number already
                if self._spark_master_url.count(':') == 2:
                    scheduler_host_has_port = True
                
            elif MSPASS_SCHEDULER_ADDRESS:
                self._spark_master_url = MSPASS_SCHEDULER_ADDRESS
                # add spark:// prefix if not exist
                if 'spark://' not in MSPASS_SCHEDULER_ADDRESS:
                    self._spark_master_url = 'spark://' + self._spark_master_url
            else:
                self._spark_master_url = 'local'

            # add port number
            # 1. not the default 'local'
            # 2. scheduler_host and does not contain port number
            # 3. SPARK_MASTER_PORT exists
            if (scheduler_host or MSPASS_SCHEDULER_ADDRESS) and not scheduler_host_has_port and SPARK_MASTER_PORT:
                self._spark_master_url += ':' + SPARK_MASTER_PORT
            
            # sanity check
            try:
                spark = SparkSession.builder.appName('mspass').master(self._spark_master_url).getOrCreate()
                self._spark_context = spark.sparkContext
            except Exception as err:
                raise MsPASSError('Runntime error: cannot create a spark configuration with: ' + self._spark_master_url, 'Fatal')

        elif self._scheduler == 'dask':
            # if no defind scheduler_host and no MSPASS_SCHEDULER_ADDRESS, use local cluster to create a client
            if not scheduler_host and not MSPASS_SCHEDULER_ADDRESS:
                self._dask_client = DaskClient()
            else:
                scheduler_host_has_port = False
                # set host
                if scheduler_host:
                    self._dask_client_address = scheduler_host
                    # check if scheduler_host contains port number already
                    if ':' in scheduler_host:
                        scheduler_host_has_port = True
                else:
                    self._dask_client_address = MSPASS_SCHEDULER_ADDRESS
                
                # add port
                if not scheduler_host_has_port and DASK_SCHEDULER_PORT:
                    self._dask_client_address += ':' + DASK_SCHEDULER_PORT
                else:
                    # use to port 8786 by default if not specified
                    self._dask_client_address += ':8786'
                # sanity check
                try:
                    self._dask_client = DaskClient(self._dask_client_address)
                except Exception as err:
                    raise MsPASSError('Runntime error: cannot create a dask client with: ' + self._dask_client_address, 'Fatal')

    def __del__(self):
        # close spark context
        # if hasattr(self, '_spark_context') and isinstance(self._spark_context, SparkContext):
        #     self._spark_context.stop()
        #     SparkSession._instantiatedContext = None
        
        # close dask client
        if hasattr(self, '_dask_client') and isinstance(self._dask_client, DaskClient):
            self._dask_client.close()

        # close database client
        if hasattr(self, '_db_client') and isinstance(self._db_client, DBClient):
            self._db_client.close()

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
            return Database(self._db_client, self._default_database_name)
        return Database(self._db_client, database_name)

    def get_global_history_manager(self):
        """
        Get the global history manager with this client

        :return: :class:`mspasspy.global_history.manager.GlobalHistoryManager`
        """
        return self._global_history_manager

    def get_scheduler(self):
        """
        Get the scheduler(spark/dask) with this client

        :return: :class:`pyspark.SparkContext`/:class:`dask.distributed.Client`
        """
        if self._scheduler == 'spark':
            return self._spark_context
        else:
            return self._dask_client


    def set_database_client(self, database_host, database_port=None):
        """
        Set a database client by database_host(and database_port)

        :param database_host: the host address of database client
        :type database_host: :class:`str`
        :param database_port: the port of database client
        :type database_port: :class:`str`
        """
        database_host_has_port = False
        database_address = database_host
        # check if port is already in the database_host address
        if ':' in database_host:
            database_host_has_port = True
        # add port
        if not database_host_has_port and database_port:
            database_address += ':' + database_port
        # sanity check
        temp_db_client = self._db_client
        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            # restore the _db_client
            self._db_client = temp_db_client
            raise MsPASSError('Runntime error: cannot create a database client with: ' + database_address, 'Fatal')
        # if success, close previous DBClient
        temp_db_client.close()

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
            raise TypeError('history_db should be a mspasspy.db.Database but ' + str(type(history_db)) +  ' is found.')
        if not type(job_name) is str:
            raise TypeError('job_name should be a string but ' + str(type(job_name)) +  ' is found.')
        if collection is not None and type(collection) is not str:
            raise TypeError('collection should be a string but ' + str(type(collection)) + ' is found.')

        self._global_history_manager = GlobalHistoryManager(history_db, job_name, collection=collection)

    def set_scheduler(self, scheduler, scheduler_host, scheduler_port=None):
        """
        Set a scheduler by scheduler type, scheduler_host(and scheduler_port)

        :param scheduler: the scheduler type, should be either dask or spark
        :type scheduler: :class:`str`
        :param scheduler_host: the host address of scheduler
        :type scheduler_host: :class:`str`
        :param scheduler_port: the port of scheduler
        :type scheduler_port: :class:`str`
        """
        if scheduler != 'dask' and scheduler != 'spark':
            raise MsPASSError('scheduler should be either dask or spark but ' + str(scheduler) +  ' is found.', 'Fatal')
        
        prev_scheduler = self._scheduler
        self._scheduler = scheduler
        if scheduler == 'spark':
            scheduler_host_has_port = False
                
            self._spark_master_url = scheduler_host
            # add spark:// prefix if not exist
            if 'spark://' not in scheduler_host:
                self._spark_master_url = 'spark://' + self._spark_master_url
            # check if spark host address contains port number already
            if self._spark_master_url.count(':') == 2:
                scheduler_host_has_port = True

            # add port
            if not scheduler_host_has_port and scheduler_port:
                self._spark_master_url += ':' + scheduler_port
            
            # sanity check
            prev_spark_context = None
            prev_spark_conf = None
            if hasattr(self, '_spark_context'):
                prev_spark_context = self._spark_context
                prev_spark_conf = self._spark_context.getConf()
            try:
                if hasattr(self, '_spark_context') and isinstance(self._spark_context, SparkContext):
                    # update the confinguration
                    spark_conf = self._spark_context._conf.setMaster(self._spark_master_url)
                else:
                    spark_conf = SparkConf().setAppName('mspass').setMaster(self._spark_master_url)
                # stop the previous spark context
                # FIXME if the new context does not start, we shouldn't stop the previous here.
                #if prev_spark_context:
                #    prev_spark_context.stop()
                # create a new spark context -> might cause error so that execute exception code
                spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
                self._spark_context = spark.sparkContext
            except Exception as err:
                # restore the spark context by the previous spark configuration
                if prev_spark_conf:
                    self._spark_context = SparkContext.getOrCreate(conf=prev_spark_conf)
                # restore the scheduler type
                if self._scheduler == 'spark' and prev_scheduler == 'dask':
                    self._scheduler = prev_scheduler
                raise MsPASSError('Runntime error: cannot create a spark configuration with: ' + self._spark_master_url, 'Fatal')
            # close previous dask client if success
            if hasattr(self, '_dask_client'):
                del self._dask_client

        elif scheduler == 'dask':
            scheduler_host_has_port = False
            self._dask_client_address = scheduler_host
            # check if scheduler_host contains port number already
            if ':' in scheduler_host:
                scheduler_host_has_port = True

            # add port
            if not scheduler_host_has_port:
                if scheduler_port:
                    self._dask_client_address += ':' + scheduler_port
                else:
                    # use to port 8786 by default if not specified
                    self._dask_client_address += ':8786'
            
            # sanity check
            prev_dask_client = None
            if hasattr(self, '_dask_client'):
                prev_dask_client = self._dask_client
            try:
                # create a new dask client
                self._dask_client = DaskClient(self._dask_client_address)
            except Exception as err:
                # restore the dask client if exists
                if prev_dask_client:
                    self._dask_client = prev_dask_client
                # restore the scheduler type
                if self._scheduler == 'dask' and prev_scheduler == 'spark':
                    self._scheduler = prev_scheduler
                raise MsPASSError('Runntime error: cannot create a dask client with: ' + self._dask_client_address, 'Fatal')
            # close previous dask client if success setting new dask client
            if prev_dask_client:
                prev_dask_client.close()
            # remove previous spark context if success setting new dask client
            if hasattr(self, '_spark_context'):
                del self._spark_context
