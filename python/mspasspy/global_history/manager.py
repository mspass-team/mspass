import os
import yaml
import pymongo
import pyspark
import collections
import dask.bag as daskbag

from bson.objectid import ObjectId
from mspasspy.ccore.utility import (MsPASSError, AntelopePf)
from mspasspy.util.converter import AntelopePf2dict 
from datetime import datetime
from dill.source import getsource

import mspasspy.algorithms.signals as signals


def check_and_parse_file(arg):
    """
     A helper function to check and parse the file content of an argument.
     If the arg is not a filepath or is not supported, return the original arg back. If the arg is a supported 
     filepath it will be parsed into python object, and then turned into a dict. 
     Now we support pf files and yaml files.
    :param args: an argument to check
    :return: An dict of file content, or the original arg.
    """
    if((isinstance(arg, os.PathLike) or isinstance(arg, str) or isinstance(arg, bytes)) and os.path.isfile(arg)):
        file_path = str(arg)
        if(file_path.endswith('.pf')):
            pf = AntelopePf(file_path)
            pf_value = AntelopePf2dict(pf)
            return pf_value
        elif(file_path.endswith('.yaml')):
            with open(file_path, "r") as yaml_file:
                yaml_value = (yaml.safe_load(yaml_file))
            return yaml_value
    return arg

def mspass_spark_map(self, func, *args, global_history=None, object_history=False, alg_id=None,
                     alg_name=None, parameters=None, **kwargs):
    """
     This decorator method add more functionaliy on the standard spark map method and be a part of member functions
     in the spark RDD library. Instead of performing the normal map function, if user provides global history manager,
     alg_id(optional), alg_name(optional) and parameters(optional) as input, the global history manager will log down
     the usage of the algorithm. Also, if user set object_history to be True, then each mspass object in this map function
     will save the object level history.

    :param func: target function
    :param global_history: a user specified global history manager
    :type global_history: :class:`GlobalHistoryManager`
    :param object_history: save the each object's history in the map when True
    :param alg_id: a user specified alg_id for the map operation
    :type alg_id: :class:`str`/:class:`bson.objectid.ObjectId`
    :param alg_name: a user specified alg_name for the map operation
    :type alg_name: :class:`str`
    :param parameters: a user specified parameters for the map operation
    :type parameters: :class:`str`
    :return: a spark `RDD` format of objects.
    """
    if not parameters:
        #   preprocess parameters and parse files, store in parsed_args_list and parsed_kwargs_dict
        parsed_args_list = list()
        for i in range(len(args)):
            val = args[i]
            new_val = check_and_parse_file(val)
            parsed_args_list.append(new_val)

        parsed_kwargs_dict = collections.OrderedDict()
        for key in kwargs.keys():
            val = kwargs[key]
            new_val = check_and_parse_file(val)
            parsed_kwargs_dict[key] = new_val

        # extract list parameters
        args_str = ",".join(f"{value}" for value in parsed_args_list)

        # extract dict parameters
        parameters_dict = collections.OrderedDict()
        for key, value in parsed_kwargs_dict.items():
            parameters_dict[key] = value
        parameters_dict['object_history'] = object_history
        if alg_name:
            parameters_dict['alg_name'] = alg_name
        if alg_id:
            parameters_dict['alg_id'] = alg_id
        kwargs_str = ",".join(f"{key}={value}" for key,
                              value in parameters_dict.items())

        if args_str:
            parameters = args_str + "," + kwargs_str
        else:
            parameters = kwargs_str

    if not alg_name:
        # if not exists, use the name of the func
        alg_name = func.__name__

    # get the alg_id if exists, else create a new one
    if not alg_id:
        # get the alg_id if exists
        if global_history:
            alg_id = global_history.get_alg_id(alg_name, parameters)
        # else create a new one
        if not alg_id:
            alg_id = ObjectId()

    # save the global history
    if global_history:
        global_history.logging(alg_id, alg_name, parameters)

    # read_data method
    if alg_name.rfind('read_data') != -1 and alg_name.rfind('read_data') + 9 == len(alg_name):
        if global_history:
            return self.map(lambda wf: func(wf, *args, alg_name=alg_name, alg_id=str(alg_id), **kwargs))
        else:
            return self.map(lambda wf: func(wf, *args, **kwargs))

    # save_data method
    if alg_name.rfind('save_data') != -1 and alg_name.rfind('save_data') + 9 == len(alg_name):
        # (return_code, mspass_object) is return for save_data, otherwise the original mspass_object is unchanged
        if global_history:
            return self.map(lambda wf: (func(wf, *args, alg_name=alg_name, alg_id=str(alg_id), **kwargs), wf))
        else:
            return self.map(lambda wf: (func(wf, *args, **kwargs), wf))

    # save the object history
    if object_history:
        return self.map(lambda wf: func(wf, *args, object_history=object_history, alg_name=alg_name, alg_id=str(alg_id), **kwargs))

    return self.map(lambda wf: func(wf, *args, object_history=object_history, **kwargs))


def mspass_dask_map(self, func, *args, global_history=None, object_history=False, alg_id=None,
                    alg_name=None, parameters=None, **kwargs):
    """
     This decorator method add more functionaliy on the standard dask map method and be a part of member functions
     in the dask bag library. Instead of performing the normal map function, if user provides global history manager,
     alg_id(optional), alg_name(optional) and parameters(optional) as input, the global history manager will log down
     the usage of the algorithm. Also, if user set object_history to be True, then each mspass object in this map function
     will save the object level history.

    :param func: target function
    :param global_history: a user specified global history manager
    :type global_history: :class:`GlobalHistoryManager`
    :param object_history: save the each object's history in the map when True
    :param alg_id: a user specified alg_id for the map operation
    :type alg_id: :class:`str`/:class:`bson.objectid.ObjectId`
    :param alg_name: a user specified alg_name for the map operation
    :type alg_name: :class:`str`
    :param parameters: a user specified parameters for the map operation
    :type parameters: :class:`str`
    :return: a dask `bag` format of objects.
    """
    if not parameters:
        #   preprocess parameters and parse files, store in parsed_args_list and parsed_kwargs_dict
        parsed_args_list = list()
        for i in range(len(args)):
            val = args[i]
            new_val = check_and_parse_file(val)
            parsed_args_list.append(new_val)

        parsed_kwargs_dict = collections.OrderedDict()
        for key in kwargs.keys():
            val = kwargs[key]
            new_val = check_and_parse_file(val)
            parsed_kwargs_dict[key] = new_val

        # extract list parameters
        args_str = ",".join(f"{value}" for value in parsed_args_list)

        # extract dict parameters
        parameters_dict = collections.OrderedDict()
        for key, value in parsed_kwargs_dict.items():
            parameters_dict[key] = value
        parameters_dict['object_history'] = object_history
        if alg_name:
            parameters_dict['alg_name'] = alg_name
        if alg_id:
            parameters_dict['alg_id'] = alg_id
        kwargs_str = ",".join(f"{key}={value}" for key,
                              value in parameters_dict.items())

        if args_str:
            parameters = args_str + "," + kwargs_str
        else:
            parameters = kwargs_str

    if not alg_name:
        alg_name = func.__name__

    # get the alg_id if exists, else create a new one
    if not alg_id:
        # get the alg_id if exists
        alg_id = global_history.get_alg_id(alg_name, parameters)
        # else create a new one
        if not alg_id:
            alg_id = ObjectId()

    # save the global history
    if global_history:
        global_history.logging(alg_id, alg_name, parameters)

    # read_data method
    if alg_name.rfind('read_data') != -1 and alg_name.rfind('read_data') + 9 == len(alg_name):
        if global_history:
            return self.map(lambda wf: func(wf, *args, alg_name=alg_name, alg_id=str(alg_id), **kwargs))
        else:
            return self.map(lambda wf: func(wf, *args, **kwargs))

    # save_data method
    if alg_name.rfind('save_data') != -1 and alg_name.rfind('save_data') + 9 == len(alg_name):
        # (return_code, mspass_object) is return for save_data, otherwise the original mspass_object is unchanged
        if global_history:
            return self.map(lambda wf: (func(wf, *args, alg_name=alg_name, alg_id=str(alg_id), **kwargs), wf))
        else:
            return self.map(lambda wf: (func(wf, *args, **kwargs), wf))

    # save the object history
    if object_history:
        return self.map(func, *args, object_history=object_history, alg_name=alg_name, alg_id=str(alg_id), **kwargs)

    return self.map(func, *args, object_history=object_history, **kwargs)


def mspass_spark_reduce(self, func, *args, global_history=None, object_history=False, alg_id=None,
                        alg_name=None, parameters=None, **kwargs):
    """
     This decorator method add more functionaliy on the standard spark reduce method and be a part of member functions
     in the spark RDD library. Instead of performing the normal reduce function, if user provides global history manager,
     alg_id(optional), alg_name(optional) and parameters(optional) as input, the global history manager will log down
     the usage of the algorithm. Also, if user set object_history to be True, then each mspass object in this reduce function
     will save the object level history.

    :param func: target function
    :param global_history: a user specified global history manager
    :type global_history: :class:`GlobalHistoryManager`
    :param object_history: save the each object's history in the reduce when True
    :param alg_id: a user specified alg_id for the reduce operation
    :type alg_id: :class:`str`/:class:`bson.objectid.ObjectId`
    :param alg_name: a user specified alg_name for the reduce operation
    :type alg_name: :class:`str`
    :param parameters: a user specified parameters for the reduce operation
    :type parameters: :class:`str`
    :return: a spark `RDD` format of objects.
    """
    if not parameters:
        #   preprocess parameters and parse files, store in parsed_args_list and parsed_kwargs_dict
        parsed_args_list = list()
        for i in range(len(args)):
            val = args[i]
            new_val = check_and_parse_file(val)
            parsed_args_list.append(new_val)

        parsed_kwargs_dict = collections.OrderedDict()
        for key in kwargs.keys():
            val = kwargs[key]
            new_val = check_and_parse_file(val)
            parsed_kwargs_dict[key] = new_val

        # extract list parameters
        args_str = ",".join(f"{value}" for value in parsed_args_list)

        # extract dict parameters
        parameters_dict = collections.OrderedDict()
        for key, value in parsed_kwargs_dict.items():
            parameters_dict[key] = value
        parameters_dict['object_history'] = object_history
        if alg_name:
            parameters_dict['alg_name'] = alg_name
        if alg_id:
            parameters_dict['alg_id'] = alg_id
        kwargs_str = ",".join(f"{key}={value}" for key,
                              value in parameters_dict.items())

        if args_str:
            parameters = args_str + "," + kwargs_str
        else:
            parameters = kwargs_str

    if not alg_name:
        # if not exists, use the name of the func
        alg_name = func.__name__

    # get the alg_id if exists, else create a new one
    if not alg_id:
        # get the alg_id if exists
        alg_id = global_history.get_alg_id(alg_name, parameters)
        # else create a new one
        if not alg_id:
            alg_id = ObjectId()

    # save the global history
    if global_history:
        global_history.logging(alg_id, alg_name, parameters)

    # save the object history
    if object_history:
        return self.reduce(lambda a, b: func(a, b, *args, object_history=object_history, alg_name=alg_name, alg_id=str(alg_id), **kwargs))

    return self.reduce(lambda a, b: func(a, b, *args, object_history=object_history, **kwargs))


def mspass_dask_fold(self, func, *args, global_history=None, object_history=False, alg_id=None,
                     alg_name=None, parameters=None, **kwargs):
    """
     This decorator method add more functionaliy on the standard dask fold method and be a part of member functions
     in the dask bag library. Instead of performing the normal fold function, if user provides global history manager,
     alg_id(optional), alg_name(optional) and parameters(optional) as input, the global history manager will log down
     the usage of the algorithm. Also, if user set object_history to be True, then each mspass object in this fold function
     will save the object level history.

    :param func: target function
    :param global_history: a user specified global history manager
    :type global_history: :class:`GlobalHistoryManager`
    :param object_history: save the each object's history in the fold when True
    :param alg_id: a user specified alg_id for the fold operation
    :type alg_id: :class:`str`/:class:`bson.objectid.ObjectId`
    :param alg_name: a user specified alg_name for the fold operation
    :type alg_name: :class:`str`
    :param parameters: a user specified parameters for the fold operation
    :type parameters: :class:`str`
    :return: a dask `bag` format of objects.
    """
    if not parameters:
        #   preprocess parameters and parse files, store in parsed_args_list and parsed_kwargs_dict
        parsed_args_list = list()
        for i in range(len(args)):
            val = args[i]
            new_val = check_and_parse_file(val)
            parsed_args_list.append(new_val)

        parsed_kwargs_dict = collections.OrderedDict()
        for key in kwargs.keys():
            val = kwargs[key]
            new_val = check_and_parse_file(val)
            parsed_kwargs_dict[key] = new_val

        # extract list parameters
        args_str = ",".join(f"{value}" for value in parsed_args_list)

        # extract dict parameters
        parameters_dict = collections.OrderedDict()
        for key, value in parsed_kwargs_dict.items():
            parameters_dict[key] = value
        parameters_dict['object_history'] = object_history
        if alg_name:
            parameters_dict['alg_name'] = alg_name
        if alg_id:
            parameters_dict['alg_id'] = alg_id
        kwargs_str = ",".join(f"{key}={value}" for key,
                              value in parameters_dict.items())

        if args_str:
            parameters = args_str + "," + kwargs_str
        else:
            parameters = kwargs_str

    if not alg_name:
        # if not exists, use the name of the func
        alg_name = func.__name__

    # get the alg_id if exists, else create a new one
    if not alg_id:
        # get the alg_id if exists
        alg_id = global_history.get_alg_id(alg_name, parameters)
        # else create a new one
        if not alg_id:
            alg_id = ObjectId()

    # save the global history
    if global_history:
        global_history.logging(alg_id, alg_name, parameters)

    # save the object history
    if object_history:
        return self.fold(lambda a, b: func(a, b, *args, object_history=object_history, alg_name=alg_name, alg_id=str(alg_id), **kwargs))

    return self.fold(lambda a, b: func(a, b, *args, object_history=object_history, **kwargs))


class GlobalHistoryManager:
    """
    A Global History Mananger handler.

    This is a handler used in the mspass_client, normally user should not directly create
    a Global History Manager by his own. Instead, user should get the Global History Manager
    through mspass client's methods.
    """

    def __init__(self, database_instance, job_name, collection=None):
        self.job_name = job_name
        # generate an bson UUID for this job, should be unique on the application level
        self.job_id = ObjectId()

        self.history_db = database_instance

        self.collection = collection
        if not self.collection:
            # use the `history` collection defined in database schema
            self.collection = self.history_db.database_schema.default_name('history_global')

        # create unique index -> (alg_name, parameters)
        self.history_db[self.collection].create_index(
            [("alg_name", pymongo.TEXT), ("parameters", pymongo.TEXT)],
        )

        # modify pyspark/dask map to our defined map
        pyspark.RDD.mspass_map = mspass_spark_map
        daskbag.Bag.mspass_map = mspass_dask_map

        # modify pyspark/dask reduce to our defined reduce
        pyspark.RDD.mspass_reduce = mspass_spark_reduce
        daskbag.Bag.mspass_reduce = mspass_dask_fold

    def logging(self, alg_id, alg_name, parameters):
        """
        Save the usage of the algorithm in the map/reduce operation

        :param alg_id: the UUID of the combination of algorithm_name and parameters
        :type alg_id: :class:`bson.objectid.ObjectId`
        :param alg_name: the name of the algorithm
        :type alg_name: :class:`str`
        :param parameters: the parameters of the algorithm
        :type parameters: :class:`str`
        """
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
        """
        Save the usage of the algorithm in the map/reduce operation

        :param alg_name: the name of the algorithm
        :type alg_name: :class:`str`
        :param alg_id: the UUID of the combination of algorithm_name and parameters
        :type alg_id: :class:`bson.objectid.ObjectId`
        :param parameters: the parameters of the algorithm
        :type parameters: :class:`str`
        """
        # no alg_name and parameters combination in the database
        if not self.history_db[self.collection].count_documents({'alg_name': alg_name, 'parameters': parameters}):
            return None

        doc = self.history_db[self.collection].find_one(
            {'alg_name': alg_name, 'parameters': parameters})
        return doc['alg_id']

    def get_alg_list(self, job_name, job_id=None):
        """
        Get a list of history records by job name(and job_id)

        :param job_name: the name of the job
        :type job_name: :class:`str`
        :param job_id: the UUID of the job
        :type job_id: :class:`bson.objectid.ObjectId`
        """
        query = {'job_name': job_name}
        if job_id:
            query['job_id'] = job_id
        alg_list = []
        docs = self.history_db[self.collection].find(query)
        for doc in docs:
            alg_list.append(doc)
        return alg_list

    def set_alg_name_and_parameters(self, alg_id, alg_name, parameters):
        """
        Set the alg_name and parameters by a user specified alg_id

        :param alg_id: the UUID of the combination of algorithm_name and parameters, used to find the records
        :type alg_id: :class:`bson.objectid.ObjectId`
        :param alg_name: the name of the algorithm user would like to set
        :type alg_name: :class:`str`
        :param parameters: the parameters of the algorithm user would like to set
        :type parameters: :class:`str`
        """
        doc = self.history_db[self.collection].find_one({'alg_id': alg_id})
        if not doc:
            raise MsPASSError(
                'No such history record with alg_id = ' + alg_id, 'Fatal')

        update_dict = {}
        update_dict['alg_name'] = alg_name
        update_dict['parameters'] = parameters
        self.history_db[self.collection].update_many(
            {'alg_id': alg_id}, {'$set': update_dict})
