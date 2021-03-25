import os
import yaml
import pymongo
import pyspark
import dask.bag as daskbag

from bson.objectid import ObjectId
from mspasspy.ccore.utility import MsPASSError
from datetime import datetime
from dill.source import getsource

def mspass_spark_map(self, func, *args, global_history=None, **kwargs):
    if global_history:
        # get the whole lambda function
        alg_string = getsource(func)
        # truncate to get the algorithm name and parameters
        alg = alg_string[alg_string.index(':')+1 : alg_string.index(')')]
        # get the algorithm name
        alg_name = alg[:alg.index('(')].strip()
        # get the parameters
        parameters = alg[alg.index('(')+1:].strip()
        
        # get the alg_id if exists, else create a new one
        alg_id = global_history.get_alg_id(alg_name, parameters)
        if not alg_id:
            alg_id = ObjectId()
        global_history.logging(alg_name, alg_id, parameters)
    return self.map(func, *args, **kwargs)

def mspass_dask_map(self, func, *args, global_history=None, **kwargs):
    if global_history:
        alg_name = func.__name__
        # extract parameters
        args_str = ",".join(f"{value}" for value in args)
        kwargs_str = ",".join(f"{key}={value}" for key, value in kwargs.items())
        parameters = args_str
        if kwargs_str:
            parameters += "," + kwargs_str

        # get the alg_id if exists, else create a new one
        alg_id = global_history.get_alg_id(alg_name, parameters)
        if not alg_id:
            alg_id = ObjectId()
        global_history.logging(alg_name, alg_id, parameters)
    return self.map(func, *args, **kwargs)

def mspass_spark_reduce(self, func, *args, global_history=None, **kwargs):
    if global_history:
        # get the whole lambda function
        alg_string = getsource(func)
        # truncate to get the algorithm name and parameters
        alg = alg_string[alg_string.index(':')+1 : alg_string.index(')')]
        # get the algorithm name
        alg_name = alg[:alg.index('(')].strip()
        # get the parameters
        parameters = alg[alg.index('(')+1:].strip()
        
        # get the alg_id if exists, else create a new one
        alg_id = global_history.get_alg_id(alg_name, parameters)
        if not alg_id:
            alg_id = ObjectId()
        global_history.logging(alg_name, alg_id, parameters)
    return self.reduce(func, *args, **kwargs)

def mspass_dask_reduce(self, func, *args, global_history=None, **kwargs):
    if global_history:
        # get the whole lambda function
        alg_string = getsource(func)
        # truncate to get the algorithm name and parameters
        alg = alg_string[alg_string.index(':')+1 : alg_string.index(')')]
        # get the algorithm name
        alg_name = alg[:alg.index('(')].strip()
        # get the parameters
        parameters = alg[alg.index('(')+1:].strip()

        # get the alg_id if exists, else create a new one
        alg_id = global_history.get_alg_id(alg_name, parameters)
        if not alg_id:
            alg_id = ObjectId()
        global_history.logging(alg_name, alg_id, parameters)
    return self.fold(func, *args, **kwargs)

class GlobalHistoryManager:
    def __init__(self, database_instance, job_name, collection=None):
        self.job_name = job_name
        # generate an bson UUID for this job, should be unique on the application level
        self.job_id = ObjectId()

        self.history_db = database_instance

        self.collection = collection
        if not self.collection:
            # use the `history` collection defined in database schema
            self.collection = 'history'

        # modify pyspark/dask map to our defined map
        pyspark.RDD.mspass_map = mspass_spark_map
        daskbag.Bag.mspass_map = mspass_dask_map

        #modify pyspark/dask reduce to our defined reduce
        pyspark.RDD.mspass_reduce = mspass_spark_reduce
        daskbag.Bag.mspass_reduce = mspass_dask_reduce

    def logging(self, alg_name, alg_id, parameters):
        # current timestamp when logging into database
        timestamp = datetime.utcnow().timestamp()

        self.history_db[self.collection].insert_one({
            'time': timestamp,
            'job_id': self.job_id,
            'job_name': self.job_name,
            'alg_name': alg_name,
            'alg_id': alg_id,
            'parameters': parameters
        })

    def get_alg_id(self, alg_name, parameters):
        # no alg_name and parameters combination in the database
        if not self.history_db[self.collection].count_documents({'alg_name': alg_name, 'parameters': parameters}):
            return None
        
        doc = self.history_db[self.collection].find_one({'alg_name': alg_name, 'parameters': parameters})
        return doc['alg_id']

    def get_alg_list(self, job_name, job_id=None):
        query = {'job_name': job_name}
        if job_id:
            query['job_id'] = job_id
        alg_list = []
        docs = self.history_db[self.collection].find(query)
        for doc in docs:
            alg_list.append(doc)
        return alg_list

    def set_alg_name_and_parameters(self, alg_id, alg_name, parameters):
        doc = self.history_db[self.collection].find_one({'alg_id': alg_id})
        if not doc:
            raise MsPASSError('No such history record with alg_id = ' + alg_id, 'Fatal')
        
        update_dict = {}
        update_dict['alg_name'] = alg_name
        update_dict['parameters'] = parameters
        self.history_db[self.collection].update_many({'alg_id': alg_id}, {'$set': update_dict})