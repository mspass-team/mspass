import os
import yaml
import pymongo
import pyspark
import dask.bag as daskbag

from bson.objectid import ObjectId
from mspasspy.ccore.utility import MsPASSError
from mspasspy.db.database import Database
from datetime import datetime
from dill.source import getsource

def mspass_map(self, func, *args, global_history=None, **kwargs):
    if global_history:
        alg_name = func.__name__
        alg_id = global_history.new_alg_id()
        parameters = getsource(func)
        global_history.logging(alg_name, alg_id, parameters)
    return self.map(func, *args, **kwargs)

def mspass_spark_reduce(self, func, *args, global_history=None, **kwargs):
    if global_history:
        alg_name = func.__name__
        alg_id = global_history.new_alg_id()
        parameters = getsource(func)
        global_history.logging(alg_name, alg_id, parameters)
    return self.reduce(func, *args, **kwargs)

def mspass_dask_reduce(self, func, *args, global_history=None, **kwargs):
    if global_history:
        alg_name = func.__name__
        alg_id = global_history.new_alg_id()
        parameters = getsource(func)
        global_history.logging(alg_name, alg_id, parameters)
    return self.fold(func, *args, **kwargs)

class GlobalHistoryManager:
    def __init__(self, client, database_name, job_name, schema=None, collection=None):
        # job_name should be a string
        if not type(job_name) is str:
            raise MsPASSError('job_name should be a string but ' + str(type(job_name)) +  ' is found.', 'Fatal')
        self.job_name = job_name
        # generate an bson UUID for this job, should be unique on the application level
        self.job_id = ObjectId()

        # MongoDB client
        self.client = client

        # database_name should be a string
        if not type(database_name) is str:
            raise MsPASSError('database_name should be a string but ' + str(type(database_name)) + ' is found.', 'Fatal')
        # used for creating mongodb connection when loggings
        self.database_name = database_name

        self.schema = schema
        # database_name should be a string
        if not type(collection) is str:
            raise MsPASSError('collection should be a string but ' + str(type(collection)) + ' is found.', 'Fatal')
        self.collection = collection

        # modify pyspark/dask map to our defined map
        pyspark.RDD.mspass_map = mspass_map
        daskbag.mspass_map = mspass_map

        #modify pyspark/dask reduce to our defined reduce
        pyspark.RDD.mspass_reduce = mspass_spark_reduce
        daskbag.mspass_reduce = mspass_dask_reduce

    def logging(self, alg_name, alg_id, parameters):
        if self.schema:
            db = Database(self.client, self.database_name, db_schema=self.schema)
        else:
            db = Database(self.client, self.database_name)
        
        # current timestamp when logging into database
        timestamp = datetime.utcnow().timestamp()
        if not self.collection:
            # use the `history` collection defined in database schema
            self.collection = db.database_schema.default_name('history')

        db[self.collection].insert_one({
            'time': timestamp,
            'job_id': self.job_id,
            'job_name': self.job_name,
            'alg_name': alg_name,
            'alg_id': alg_id,
            'parameters': parameters
        })

    def new_alg_id(self):
        return ObjectId()