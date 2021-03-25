import os
import pymongo

from mspasspy.db.client import Client as DBClient
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager
from pyspark import SparkConf, SparkContext
from mspasspy.ccore.utility import MsPASSError
from dask.distributed import Client as DaskClient

class Client:
    """
    A client-side representation of MSPASS.
    """
    def __init__(self, database_host='localhost', scheduler='dask', scheduler_host='localhost', job_name='mspass', database_name='mspass', schema=None, collection=None):
        # job_name should be a string
        if not type(database_host) is str:
            raise MsPASSError('database_host should be a string but ' + str(type(database_host)) +  ' is found.', 'Fatal')
        if scheduler != 'dask' and scheduler != 'spark':
            raise MsPASSError('scheduler should be either dask or spark but ' + str(scheduler) +  ' is found.', 'Fatal')
        if not type(scheduler_host) is str:
            raise MsPASSError('scheduler_host should be a string but ' + str(type(scheduler_host)) +  ' is found.', 'Fatal')
        if not type(job_name) is str:
            raise MsPASSError('job_name should be a string but ' + str(type(job_name)) +  ' is found.', 'Fatal')
        if not type(database_name) is str:
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
        if MSPASS_DB_ADDRESS:
            if MONGODB_PORT:
                self._db_client = DBClient(MSPASS_DB_ADDRESS + ':' + MONGODB_PORT)
            else:
                self._db_client = DBClient(MSPASS_DB_ADDRESS)
        else:
            if MONGODB_PORT:
                self._db_client = DBClient(database_host + ':' + MONGODB_PORT)
            else:
                self._db_client = DBClient(database_host)
        
        # set default database name
        self.default_database_name = database_name

        # create a Global History Manager
        if schema:
            global_history_manager_db = Database(self._db_client, database_name, db_schema=schema)
        else:
            global_history_manager_db = Database(self._db_client, database_name)
        self._global_history_manager = GlobalHistoryManager(global_history_manager_db, job_name, collection=collection)

        # set the configuration of a scheduler 
        self._scheduler = scheduler
        if MSPASS_SCHEDULER:
            self._scheduler = scheduler

        if self._scheduler == 'spark':
            if MSPASS_SCHEDULER_ADDRESS:
                if SPARK_MASTER_PORT:
                    self._spark_master_url = 'spark://' + MSPASS_SCHEDULER_ADDRESS + ':' + SPARK_MASTER_PORT
                else:
                    self._spark_master_url = 'spark://' + MSPASS_SCHEDULER_ADDRESS
            else:
                self._spark_master_url = 'local'
            self._spark_conf = SparkConf().setAppName('mspass').setMaster(self._spark_master_url)

        elif self._scheduler == 'dask':
            if MSPASS_SCHEDULER_ADDRESS:
                # use to port 8786 by default if not specified
                if not DASK_SCHEDULER_PORT:
                    DASK_SCHEDULER_PORT = '8786'
                self._dask_client = DaskClient(MSPASS_SCHEDULER_ADDRESS + ':' + DASK_SCHEDULER_PORT)
            # if no dask_port, use local cluster to create a client
            else:
                self._dask_client = DaskClient()

    def get_database(self, database_name=None):
        if not database_name:
            return Database(self._db_client, self.default_database_name)
        return Database(self._db_client, database_name)

    def get_global_history_manager(self):
        return self._global_history_manager

    def get_spark_context(self):
        return SparkContext.getOrCreate(conf=self._spark_conf)

    def get_dask_client(self):
        return self._dask_client