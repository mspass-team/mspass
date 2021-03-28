import os
import pymongo

from mspasspy.db.client import Client as DBClient
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager
from pyspark import SparkConf, SparkContext
from mspasspy.ccore.utility import MsPASSError
from dask.distributed import Client as DaskClient

# TODO docstring

class Client:
    """
    A client-side representation of MSPASS.
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
        if database_host:
            database_address = database_host
        elif MSPASS_DB_ADDRESS:
            database_address = MSPASS_DB_ADDRESS
        else:
            database_address = 'localhost'
        # add port
        if MONGODB_PORT:
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
            if scheduler_host:
                self._spark_master_url = 'spark://' + scheduler_host
            elif MSPASS_SCHEDULER_ADDRESS:
                self._spark_master_url = 'spark://' + MSPASS_SCHEDULER_ADDRESS
            else:
                self._spark_master_url = 'local'

            # add port
            if (scheduler_host or MSPASS_SCHEDULER_ADDRESS) and SPARK_MASTER_PORT:
                self._spark_master_url += ':' + SPARK_MASTER_PORT
            
            # sanity check
            try:
                spark_conf = SparkConf().setAppName('mspass').setMaster(self._spark_master_url)
                self._spark_context = SparkContext.getOrCreate(conf=spark_conf)
            except Exception as err:
                raise MsPASSError('Runntime error: cannot create a spark configuration with: ' + self._spark_master_url, 'Fatal')

        elif self._scheduler == 'dask':
            # if no defind scheduler_host and no MSPASS_SCHEDULER_ADDRESS, use local cluster to create a client
            if not scheduler_host and not MSPASS_SCHEDULER_ADDRESS:
                self._dask_client = DaskClient()
            else:
                # set host
                if scheduler_host:
                    self._dask_client_address = scheduler_host
                else:
                    self._dask_client_address = MSPASS_SCHEDULER_ADDRESS
                
                # add port
                if DASK_SCHEDULER_PORT:
                    self._dask_client_address += ':' + DASK_SCHEDULER_PORT
                else:
                    # use to port 8786 by default if not specified
                    self._dask_client_address += ':8786'
                # sanity check
                try:
                    self._dask_client = DaskClient(self._dask_client_address)
                except Exception as err:
                    raise MsPASSError('Runntime error: cannot create a dask client with: ' + self._dask_client_address, 'Fatal')

    
    def get_database(self, database_name=None):
        if not database_name:
            return Database(self._db_client, self._default_database_name)
        return Database(self._db_client, database_name)

    def get_global_history_manager(self):
        return self._global_history_manager

    def get_scheduler(self):
        if self._scheduler == 'spark':
            return self._spark_context
        else:
            return self._dask_client


    def set_database_client(self, database_host, database_port=None):
        database_address = database_host
        # add port
        if database_port:
            database_address += ':' + database_port
        # sanity check
        try:
            self._db_client = DBClient(database_address)
            self._db_client.server_info()
        except Exception as err:
            raise MsPASSError('Runntime error: cannot create a database client with: ' + database_address, 'Fatal')

    def set_global_history_manager(self, history_db, job_name, collection=None):
        if not isinstance(history_db, Database):
            raise TypeError('history_db should be a mspasspy.db.Database but ' + str(type(history_db)) +  ' is found.')
        if not type(job_name) is str:
            raise TypeError('job_name should be a string but ' + str(type(job_name)) +  ' is found.')
        if collection is not None and type(collection) is not str:
            raise TypeError('collection should be a string but ' + str(type(collection)) + ' is found.')

        self._global_history_manager = GlobalHistoryManager(history_db, job_name, collection=collection)

    def set_scheduler(self, scheduler, scheduler_host, scheduler_port=None):
        if scheduler != 'dask' and scheduler != 'spark':
            raise MsPASSError('scheduler should be either dask or spark but ' + str(scheduler) +  ' is found.', 'Fatal')
        
        self._scheduler = scheduler
        if scheduler == 'spark':
            self._spark_master_url = 'spark://' + scheduler_host
            # add port
            if scheduler_port:
                self._spark_master_url += ':' + scheduler_port
            
            # sanity check
            try:
                spark_conf = SparkConf().setAppName('mspass').setMaster(self._spark_master_url)
                self._spark_context = SparkContext.getOrCreate(conf=spark_conf)
            except Exception as err:
                raise MsPASSError('Runntime error: cannot create a spark configuration with: ' + self._spark_master_url, 'Fatal')

        elif scheduler == 'dask':
            self._dask_client_address = scheduler_host
            # add port
            if scheduler_port:
                self._dask_client_address += ':' + scheduler_port
            else:
                # use to port 8786 by default if not specified
                self._dask_client_address += ':8786'
            
            # sanity check
            try:
                self._dask_client = DaskClient(self._dask_client_address)
            except Exception as err:
                raise MsPASSError('Runntime error: cannot create a dask client with: ' + self._dask_client_address, 'Fatal')
