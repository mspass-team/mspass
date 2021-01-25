"""
Tools for connecting to MongoDB.
"""
import os
import copy
import pathlib
import pickle
import struct
import sys
from array import array

import dask.bag as daskbag
import gridfs
import pymongo
from bson.objectid import ObjectId
import numpy as np
from obspy import Inventory
from obspy import UTCDateTime

from mspasspy.ccore.seismic import (TimeSeries,
                                    Seismogram,
                                    _CoreSeismogram,
                                    TimeReferenceType,
                                    DoubleVector,
                                    TimeSeriesEnsemble,
                                    SeismogramEnsemble)
from mspasspy.ccore.utility import (Metadata,
                                    MsPASSError,
                                    dmatrix,
                                    ProcessingHistory)
from mspasspy.db.schema import DatabaseSchema, MetadataSchema


def read_distributed_data(client_arg, db_name, cursors, object_type, load_history=True, exclude_keys=[],
                          format='spark', spark_context=None):
    """
     This method takes a list of mongodb cursors as input, constructs a mspasspy object for each cursor in a distributed
     manner, and return all of the mspasspy objects using the format required by the distributed computing framework
     (spark RDD or dask bag).

    :param client_arg: the argument to initialize a :class:`mspasspy.db.Client`.
    :param db_name: the database name in mongodb.
    :param cursors: mongodb cursors where each corresponds to a stored mspasspy object.
    :param object_type: either "TimeSeries" or "Seismogram"
    :type object_type: :class:`str`
    :param load_history: `True` to load object-level history into mspasspy objects.
    :param exclude_keys: the metadata attributes you want to exclude from being read.
    :type exclude_keys: a :class:`list` of :class:`str`
    :param format: "spark" or "dask".
    :type format: :class:`str`
    :param spark_context: user specified spark context.
    :type spark_context: :class:`pyspark.SparkContext`
    :return: a spark `RDD` or dask `bag` format of mspasspy objects.
    """
    if format == 'spark':
        list_ = spark_context.parallelize(cursors)
        return list_.map(lambda cur: _read_distributed_data(client_arg, db_name, cur['_id'], object_type, load_history, exclude_keys))
    elif format == 'dask':
        list_ = daskbag.from_sequence(cursors)
        return list_.map(lambda cur: _read_distributed_data(client_arg, db_name, cur['_id'], object_type, load_history, exclude_keys))
    else:
        raise TypeError("Only spark and dask are supported")


def _read_distributed_data(client_arg, db_name, id, object_type, load_history=True, exclude_keys=[]):
    """
     A helper method used in the distributed map operation. It creates a mongodb connection with provided
     configurations, reads data from the database, constructs a mspasspy object and returns it.

    :param client_arg: the argument to initialize a :class:`mspasspy.db.Client`.
    :param db_name: the database name in mongodb.
    :param id: the `bson.ObjectId` of the mspasspy object stored in mongodb
    :type id: :class:'bson.objectid.ObjectId'.
    :param object_type: either "TimeSeries" or "Seismogram".
    :type object_type: :class:`str`
    :param load_history: `True` to load object-level history into the mspasspy object.
    :param exclude_keys: the metadata attributes you want to exclude from being read.
    :type exclude_keys: a :class:`list` of :class:`str`
    :return: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
    """
    from mspasspy.db.client import Client
    client = Client(client_arg)
    db = Database(client, db_name)
    return db.read_data(id, object_type, load_history, exclude_keys)


class Database(pymongo.database.Database):
    """
    A MongoDB database handler.

    This is a wrapper around the :class:`~pymongo.database.Database` with
    methods added to handle MsPASS data.  The one and only constructor
    uses a database handle normally created with a variant of this pair
    of commands:
        client=MongoClient()
        db=client['database_name']
    where database_name is variable and the name of the database you
    wish to access with this handle.
    """

    def __init__(self, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.metadata_schema = MetadataSchema()
        self.database_schema = DatabaseSchema()

    def __getstate__(self):
        ret = self.__dict__.copy()
        ret['_Database__client'] = self.client.__repr__()
        return ret

    def __setstate__(self, data):
        from pymongo import MongoClient
        data['_Database__client'] = eval(data['_Database__client'])
        self.__dict__.update(data)

    def set_metadata_schema(self, schema):
        """
        Set metadata_schema defined in the Database class.

        :param schema: a instance of :class:`mspsspy.db.schema.MetadataSchema`
        """
        self.metadata_schema = schema

    def set_database_schema(self, schema):
        """
        Set database_schema defined in the Database class.

        :param schema: a instance of :class:`mspsspy.db.schema.DatabaseSchema`
        """
        self.database_schema = schema

    def read_data(self, object_id, object_type='TimeSeries', load_history=True, exclude_keys=[], collection=None):
        """
        Reads and returns the mspasspy object stored in the database.

        :param object_id: mongodb "_id" of the mspasspy object.
        :type object_id: :class:`bson.objectid.ObjectId`
        :param object_type: either "TimeSeries" or "Seismogram".
        :type object_type: :class:`str`
        :param load_history: `True` to load object-level history into the mspasspy object.
        :param exclude_keys: the metadata attributes you want to exclude from being read.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param collection: the collection name in the database that the object is stored. If not specified, use the defined collection in the schema.
        :return: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        """
        if object_type not in ['TimeSeries', 'Seismogram']:
            raise KeyError("only TimeSeries and Seismogram are supported")
        # todo support miniseed, hdf5

        schema = self.metadata_schema
        if object_type == 'TimeSeries':
            read_metadata_schema = schema.TimeSeries
        else:
            read_metadata_schema = schema.Seismogram

        # use _id to get the collection this object belongs to
        wf_collection = read_metadata_schema.collection('_id') if not collection else collection

        col = self[wf_collection]
        object_doc = col.find_one({'_id': object_id})
        if not object_doc:
            return None

        # 1. build metadata as dict
        md = Metadata()
        for k in object_doc:
            if k not in exclude_keys and read_metadata_schema.is_defined(k) and not read_metadata_schema.is_alias(k):
                md[k] = object_doc[k]

        # build a set of collection to read in normalized attributes
        col_set = set()
        for k in read_metadata_schema.keys():
            col = read_metadata_schema.collection(k)
            if col:
                col_set.add(col)
        col_set.discard(wf_collection)

        col_dict = {}
        for col in col_set:
            col_dict[col] = self[col].find_one({'_id': object_doc[col + '_id']})

        for k in read_metadata_schema.keys():
            col = read_metadata_schema.collection(k)
            if col != wf_collection and k not in exclude_keys:
                md[k] = col_dict[col][self.database_schema[col].unique_name(k)]

        for k in md:
            if read_metadata_schema.is_defined(k):
                if not isinstance(md[k], read_metadata_schema.type(k)):
                    raise TypeError('{} has type {}, forbidden by definition'.format(k, type(md[k])))

        if object_type == 'TimeSeries':
            # FIXME: This is awkward. Need to revisit when we have proper constructors.
            mspass_object = TimeSeries({k: md[k] for k in md}, np.ndarray([0], dtype=np.float64))
            mspass_object.npts = object_doc['npts']
        else:
            mspass_object = Seismogram(_CoreSeismogram(md, False))

        # 2.load data from different modes
        mode = object_doc['storage_mode']
        if mode == "file":
            self._read_data_from_dfile(mspass_object, object_doc['dir'], object_doc['dfile'], object_doc['foff'])
        elif mode == "gridfs":
            self._read_data_from_gridfs(mspass_object, object_doc['gridfs_id'])
        elif mode == "url":
            pass  # todo for future
        else:
            raise TypeError("Unknown storage mode: {}".format(mode))

        # 3.load history
        if load_history:
            history_obj_id_name = self.database_schema.default_name('history_object') + '_id'
            self._load_history(mspass_object, object_doc[history_obj_id_name])

        mspass_object.live = True
        mspass_object.clear_modified()
        return mspass_object

    def save_data(self, mspass_object, storage_mode='gridfs', include_undefined=False, dfile=None, dir=None,
                  exclude_keys=[], collection=None):
        """
        Save the mspasspy object (metadata attributes, processing history, elogs and data) in the mongodb database.

        :param mspass_object: the object you want to save.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param storage_mode: "gridfs" stores the object in the mongodb grid file system (recommended). "file" stores
            the object in a binary file, which requires `dfile` and `dir`.
        :type storage_mode: :class:`str`
        :param include_undefined: `True` to also save the metadata attributes not defined in the schema.
        :param dfile: file name if using "file" storage mode.
        :type dfile: :class:`str`
        :param dir: file directory if using "file" storage mode.
        :type dir: :class:`str`
        :param exclude_keys: the metadata attributes you want to exclude from being stored.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the schema.
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")
        if storage_mode not in ['file', 'gridfs']:
            raise TypeError("Unknown storage mode: {}".format(storage_mode))
        # below we try to capture permission issue before writing anything to the database.
        # However, in the case that a storage is almost full, exceptions can still be 
        # thrown, which could mess up the database record.
        if storage_mode == 'file':
            if not dfile and not dir:
                # Note the following uses the dir and dfile defined in the data object.
                # It will ignore these two keys already in the collection in an update
                # transaction, and the dir and dfile in the collection will be replaced.
                if ('dir' not in mspass_object) or ('dfile' not in mspass_object):
                    raise ValueError(
                        'dir or dfile is not specified in data object')
                dir = os.path.abspath(mspass_object['dir'])
                dfile = mspass_object['dfile']
            else:
                dir = os.path.abspath(dir)
            fname = os.path.join(dir, dfile)
            if os.path.exists(fname):
                if not os.access(fname, os.W_OK):
                    raise PermissionError(
                        'No write permission to the save file: {}'.format(fname))
            else:
                # the following loop finds the top level of existing parents to fname
                # and check for write permission to that directory. 
                for path_item in pathlib.PurePath(fname).parents:
                    if os.path.exists(path_item):
                        if not os.access(path_item, os.W_OK | os.X_OK):
                            raise PermissionError(
                                'No write permission to the save directory: {}'.format(dir))
                        break

        schema = self.metadata_schema
        if isinstance(mspass_object, TimeSeries):
            save_schema = schema.TimeSeries
        else:
            save_schema = schema.Seismogram

        if mspass_object.live:
            # This function is needed to make sure the metadata define the time
            # standard consistently
            self._sync_time_metadata(mspass_object)
            # 1. save metadata
            self.update_metadata(mspass_object, include_undefined, exclude_keys, collection)

            # 2. save data
            wf_collection = save_schema.collection('_id') if not collection else collection
            col = self[wf_collection]
            object_doc = col.find_one({'_id': mspass_object['_id']})
            filter_ = {'_id': mspass_object['_id']}
            update_dict = {'storage_mode': storage_mode}

            if storage_mode == "file":
                foff = self._save_data_to_dfile(mspass_object, dir, dfile)
                update_dict['dir'] = dir
                update_dict['dfile'] = dfile
                update_dict['foff'] = foff
            elif storage_mode == "gridfs":
                old_gridfs_id = None if 'gridfs_id' not in object_doc else object_doc['gridfs_id']
                gridfs_id = self._save_data_to_gridfs(mspass_object, old_gridfs_id)
                update_dict['gridfs_id'] = gridfs_id
            #TODO will support url mode later 
            #elif storage_mode == "url":
            #    pass

            col.update_one(filter_, {'$set': update_dict})
        else:
            # FIXME: we could have recorded the full stack here, but need to revise the logger object
            # to make it more powerful for Python logging.
            mspass_object.elog.log_verbose(
                sys._getframe().f_code.co_name, "Skipped saving dead object")
            self._save_elog(mspass_object)

    def update_metadata(self, mspass_object, include_undefined=False, exclude_keys=[], collection=None):
        """
        Update (or save if it's a new object) the mspasspy object, including saving the processing history, elogs
        and metadata attributes.

        :param mspass_object: the object you want to update.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param include_undefined: `True` to also update the metadata attributes not defined in the schema.
        :param exclude_keys: a list of metadata attributes you want to exclude from being updated.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the schema.
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")

        if mspass_object.live:
            schema = self.metadata_schema
            if isinstance(mspass_object, TimeSeries):
                update_metadata_def = schema.TimeSeries
            else:
                update_metadata_def = schema.Seismogram

            wf_collection = update_metadata_def.collection('_id') if not collection else collection
            col = self[wf_collection]
            object_doc = None

            new_insertion = False
            if '_id' not in mspass_object:
                new_insertion = True

            if not new_insertion:
                object_doc = col.find_one({'_id': mspass_object['_id']})

            # 1. create the dict of metadata to be saved in wf
            insert_dict = {}

            self._sync_metadata_before_update(mspass_object)
            copied_metadata = Metadata(mspass_object)

            update_metadata_def.clear_aliases(copied_metadata)

            for k in copied_metadata:
                if not str(copied_metadata[k]).strip():
                    copied_metadata.erase(k)

            for k in copied_metadata:
                if k in exclude_keys:
                    continue
                if update_metadata_def.is_defined(k):
                    if update_metadata_def.readonly(k):
                        continue
                    if not isinstance(copied_metadata[k], update_metadata_def.type(k)):
                        try:
                            # The following convert the actual value in a dict to a required type.
                            # This is because the return of type() is the class reference.
                            insert_dict[k] = update_metadata_def.type(
                                k)(copied_metadata[k])
                        except Exception as err:
                            raise MsPASSError('Failure attempting to convert key {} from {} to {}'.format(
                                k, copied_metadata[k], update_metadata_def.type(k)), 'Fatal') from err
                    else:
                        insert_dict[k] = copied_metadata[k]
                elif include_undefined:
                    insert_dict[k] = copied_metadata[k]

            # 2. save history
            if not mspass_object.is_empty():
                history_obj_id_name = self.database_schema.default_name('history_object') + '_id'
                old_history_object_id = None if new_insertion or history_obj_id_name not in object_doc else object_doc[history_obj_id_name]
                history_object_id = self._save_history(mspass_object, old_history_object_id)
                insert_dict.update({history_obj_id_name: history_object_id})

            # 3. save error logs
            if mspass_object.elog.size() != 0:
                elog_id_name = self.database_schema.default_name('elog') + '_id'
                old_elog_id = None if new_insertion or elog_id_name not in object_doc else object_doc[elog_id_name]
                elog_id = self._save_elog(mspass_object, old_elog_id)  # elog ids will be updated in the wf col when saving metadata
                insert_dict.update({elog_id_name: elog_id})

            if '_id' not in copied_metadata:  # new_insertion
                mspass_object['_id'] = col.insert_one(insert_dict).inserted_id
            else:
                filter_ = {'_id': copied_metadata['_id']}
                col.update_one(filter_, {'$set': insert_dict})

            # 4. need to save the wf_id back to elog entry if this is an insert
            if new_insertion and mspass_object.elog.size() != 0:
                elog_col = self[self.database_schema.default_name('elog')]
                wf_id_name = wf_collection + '_id'
                filter_ = {'_id': elog_id}
                elog_col.update_one(filter_, {'$set': {wf_id_name: mspass_object['_id']}})
        else:
            # FIXME: we could have recorded the full stack here, but need to revise the logger object
            # to make it more powerful for Python logging.
            mspass_object.elog.log_verbose(
                sys._getframe().f_code.co_name, "Skipped updating the metadata of a dead object")


    def read_ensemble_data(self, objectid_list, ensemble_type='TimeSeriesEnsemble', load_history=True, exclude_keys=[]):
        """
        Reads and returns the mspasspy ensemble object stored in the database.

        :param objectid_list: a :class:`list` of :class:`bson.objectid.ObjectId`, where each belongs to a mspasspy object.
        :param ensemble_type: either "TimeSeriesEnsemble" or "SeismogramEnsemble". This implies all of mspasspy objects
            required should either all be :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :type ensemble_type: :class:`str`
        :param load_history: `True` to also update the metadata attributes not defined in the schema.
        :param exclude_keys: the metadata attributes you want to exclude from being read.
        :type exclude_keys: a :class:`list` of :class:`str`
        :return: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        """
        if ensemble_type not in ['TimeSeriesEnsemble', 'SeismogramEnsemble']:
            raise KeyError("only TimeSeriesEnsemble and SeismogramEnsemble are supported")

        object_type = 'TimeSeries'
        if ensemble_type == 'TimeSeriesEnsemble':
            ensemble = TimeSeriesEnsemble(len(objectid_list))
        else:
            object_type = 'Seismogram'
            ensemble = SeismogramEnsemble(len(objectid_list))

        for i in range(len(objectid_list)):
            ensemble.member.append(self.read_data(
                objectid_list[i], object_type, load_history, exclude_keys))

        return ensemble

    def save_ensemble_data(self, ensemble_object, storage_mode='gridfs', dfile_list=None, dir_list=None,
                           include_undefined=False, exclude_keys=[], exclude_objects=[], collection=None):
        """
        Save the mspasspy ensemble object (metadata attributes, processing history, elogs and data) in the mongodb
        database.

        :param ensemble_object: the ensemble you want to save.
        :type ensemble_object: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param storage_mode: "gridfs" stores the object in the mongodb grid file system (recommended). "file" stores
            the object in a binary file, which requires `dfile` and `dir`.
        :type storage_mode: :class:`str`
        :param dfile_list: A :class:`list` of file names if using "file" storage mode. File name is `str` type.
        :param dir_list: A :class:`list` of file directories if using "file" storage mode. File directory is `str` type.
        :param include_undefined: `True` to also update the metadata attributes not defined in the schema.
        :param exclude_keys: the metadata attributes you want to exclude from being stored.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param exclude_objects: A list of indexes, where each specifies a object in the ensemble you want to exclude from being saved. Starting from 0.
        :type exclude_objects: :class:`list`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the schema.
        """
        # This pushes ensemble's metadata to each member.  We pass the exclude
        # keys as the effort required to pass ones not meant for ensemble is tiny
        self._sync_ensemble_metadata(ensemble_object,do_not_copy=exclude_keys)

        if not dfile_list:
            dfile_list = [None for _ in range(len(ensemble_object.member))]
        if not dir_list:
            dir_list = [None for _ in range(len(ensemble_object.member))]

        if storage_mode in ["file", "gridfs"]:
            j = 0
            for i in range(len(ensemble_object.member)):
                if i not in exclude_objects:
                    self.save_data(ensemble_object.member[i], storage_mode, include_undefined, dfile_list[j],
                                   dir_list[j], exclude_keys, collection)
                    j += 1
        elif storage_mode == "url":
            pass
        else:
            raise TypeError("Unknown storage mode: {}".format(storage_mode))

    def update_ensemble_metadata(self, ensemble_object, include_undefined=False, exclude_keys=[], exclude_objects=[],
                                 collection=None):
        """
        Update (or save if it's new) the mspasspy ensemble object, including saving the processing history, elogs
        and metadata attributes.

        :param ensemble_object: the ensemble you want to update.
        :type ensemble_object: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param include_undefined: `True` to also update the metadata attributes not defined in the schema.
        :param exclude_keys: the metadata attributes you want to exclude from being updated.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param exclude_objects: a list of indexes, where each specifies a object in the ensemble you want to
        exclude from being saved. The index starts at 0.
        :type exclude_objects: :class:`list`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the
        schema.
        """
        for i in range(len(ensemble_object.member)):
            if i not in exclude_objects:
                self.update_metadata(ensemble_object.member[i], include_undefined, exclude_keys, collection)

    def detele_wf(self, object_id, object_type, collection=None):
        """
        Delete a mspasspy object.

        :param object_id: object id of the object.
        :type object_id: :class:`bson.objectid.ObjectId`
        :param object_type: either "TimeSeries" or "Seismogram".
        :type object_type: :class:`str`
        :param collection: the name of the collection that the object is stored. If not specified, use the defined name
                in the schema.
        """
        if object_type not in ["TimeSeries", "Seismogram"]:
            raise TypeError("only TimeSeries and Seismogram are supported")

        if not collection:
            schema = self.metadata_schema
            if object_type == "TimeSeries":
                detele_schema = schema.TimeSeries
            else:
                detele_schema = schema.Seismogram

            collection = detele_schema.collection('_id') if not collection else collection

        self[collection].delete_one({'_id': object_id})

    def delete_gridfs(self, gridfs_id):
        """
        Delete a grid document.

        :param gridfs_id: id of the document.
        :type gridfs_id: :class:`bson.objectid.ObjectId`
        """
        gfsh = gridfs.GridFS(self)
        if gfsh.exists(gridfs_id):
            gfsh.delete(gridfs_id)

    def _sync_metadata_before_update(self, d):
        """
        Synchronize a few broken attributes before saving.

        :param d: a mspasspy object.
        :type d: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        """
        if d.tref == TimeReferenceType.Relative:
            d.put_string('time_standard', 'relative')
        else:
            d.put_string('time_standard', 'UTC')

    def _save_history(self, mspass_object, prev_history_object_id=None, collection=None):
        """
        Save the processing history of a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param prev_history_object_id: the previous history object id (if it has).
        :type prev_history_object_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to store the history object. If not specified, use the defined
        collection in the schema.
        :return: current history_object_id.
        """
        if not collection:
            collection = self.database_schema.default_name('history_object')
        history_col = self[collection]
        proc_history = ProcessingHistory(mspass_object)
        current_uuid = proc_history.id() # uuid in the current node
        history_binary = pickle.dumps(proc_history)
        # todo save jobname jobid when global history module is done
        try:
            if prev_history_object_id:
                # overwrite history
                history_col.delete_one({'_id': prev_history_object_id})
                history_col.insert_one({'_id': current_uuid, 'nodesdata': history_binary})
            else:
                # new insertion
                history_col.insert_one({'_id': current_uuid, 'nodesdata': history_binary})
        except pymongo.errors.DuplicateKeyError as e:
            raise MsPASSError("The history object to be saved has a duplicate uuid", "Fatal") from e

        return current_uuid

    def _load_history(self, mspass_object, history_object_id, collection=None):
        """
        Load (in place) the processing history into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param history_object_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to load the processing history. If not specified, use the defined
        collection in the schema.
        """
        if not collection:
            collection = self.database_schema.default_name('history_object')
        res = self[collection].find_one({'_id': history_object_id})
        mspass_object.load_history(pickle.loads(res['nodesdata']))

    def _save_elog(self, mspass_object, elog_id=None, collection=None):
        """
        Save error log for a data object. Data objects in MsPASS contain an error log object used to post any
        errors handled by processing functions. This function will delete the old elog entry if `elog_id` is given.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param elog_id: the previous elog object id to be appended with.
        :type elog_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to save the elogs. If not specified, use the defined
        collection in the schema.
        :return: updated elog_id.
        """
        if isinstance(mspass_object, TimeSeries):
            update_metadata_def = self.metadata_schema.TimeSeries
        elif isinstance(mspass_object, Seismogram):
            update_metadata_def = self.metadata_schema.Seismogram
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")
        wf_id_name = update_metadata_def.collection('_id') + '_id'

        if not collection:
            collection = self.database_schema.default_name('elog')
        
        oid = None
        if '_id' in mspass_object:
            oid = mspass_object['_id']

        elog = mspass_object.elog
        n = elog.size()
        if n != 0:
            logdata = []
            docentry = {'logdata': logdata}
            errs = elog.get_error_log()
            jobid = elog.get_job_id()
            for x in errs:
                logdata.append({'job_id': jobid, 'algorithm': x.algorithm, 'badness': str(x.badness),
                            'error_message': x.message, 'process_id': x.p_id})
            if oid:
                docentry[wf_id_name] = oid

            if not mspass_object.live:
                docentry['gravestone'] = dict(mspass_object)

            if elog_id:
                # append elog
                elog_doc = self[collection].find_one({'_id': elog_id})
                # only append when previous elog exists
                if elog_doc:
                    # if the same object was updated twice, the elog entry will be duplicated
                    # the following list comprehension line removes the duplicates and preserves
                    # the order. May need some practice to see if such a behavior makes sense.
                    [elog_doc['logdata'].append(x) for x in logdata if x not in elog_doc['logdata']]
                    docentry['logdata'] = elog_doc['logdata']
                    self[collection].delete_one({'_id': elog_id})
                # note that is should be impossible for the old elog to have gravestone entry
                # so we ignore the handling of that attribute here.
                ret_elog_id = self[collection].insert_one(docentry).inserted_id
            else:
                # new insertion
                ret_elog_id = self[collection].insert_one(docentry).inserted_id
            return ret_elog_id


    @staticmethod
    def _read_data_from_dfile(mspass_object, dir, dfile, foff):
        """
        Read the stored data from a file and loads it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :param foff: offset that marks the starting of the data in the file.
        """
        fname = os.path.join(dir, dfile)
        with open(fname, mode='rb') as fh:
            fh.seek(foff)
            float_array = array('d')
            if isinstance(mspass_object, TimeSeries):
                if not mspass_object.is_defined('npts'):
                    raise KeyError("npts is not defined")
                float_array.frombytes(fh.read(mspass_object.get('npts') * 8))
                mspass_object.data = DoubleVector(float_array)
            elif isinstance(mspass_object, Seismogram):
                if not mspass_object.is_defined('npts'):
                    raise KeyError("npts is not defined")
                float_array.frombytes(fh.read(mspass_object.get('npts') * 8 * 3))
                print(len(float_array))
                mspass_object.data = dmatrix(3, mspass_object.get('npts'))
                for i in range(3):
                    for j in range(mspass_object.get('npts')):
                        mspass_object.data[i, j] = float_array[i * mspass_object.get('npts') + j]
            else:
                raise TypeError("only TimeSeries and Seismogram are supported")

    @staticmethod
    def _save_data_to_dfile(mspass_object, dir, dfile):
        """
        Saves sample data as a binary dump of the sample data. Save a mspasspy object as a pure binary dump of
        the sample data in native (Fortran) order. Opens the file and ALWAYS appends data to the end of the file.

        This method is subject to several issues to beware of before using them:
        (1) they are subject to damage by other processes/program, (2) updates are nearly impossible without
        stranding (potentially large quantities) of data in the middle of files or
        corrupting a file with a careless insert, and (3) when the number of files
        gets large managing them becomes difficult.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :return: Position of first data sample (foff).
        """
        fname = os.path.join(dir, dfile)
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        with open(fname, mode='a+b') as fh:
            foff = fh.seek(0, 2)
            if isinstance(mspass_object, TimeSeries):
                ub = bytes(np.array(mspass_object.data))  # fixme DoubleVector
            elif isinstance(mspass_object, Seismogram):
                ub = bytes(mspass_object.data)
            else:
                raise TypeError("only TimeSeries and Seismogram are supported")
            fh.write(ub)
        return foff

    def _save_data_to_gridfs(self, mspass_object, gridfs_id=None):
        """
        Save a mspasspy object sample data to MongoDB grid file system. We recommend to use this method
        for saving a mspasspy object inside MongoDB.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param gridfs_id: if the data is already stored and you want to update it, you should provide the object id
        of the previous data, which will be deleted. A new document will be inserted instead.
        :type gridfs_id: :class:`bson.objectid.ObjectId`.
        :return inserted gridfs object id.
        """
        gfsh = gridfs.GridFS(self)
        if gridfs_id and gfsh.exists(gridfs_id):
            gfsh.delete(gridfs_id)
        if isinstance(mspass_object, Seismogram):
            ub = bytes(mspass_object.data)
        else:
            ub = bytes(np.array(mspass_object.data))
        return gfsh.put(pickle.dumps(ub))

    def _read_data_from_gridfs(self, mspass_object, gridfs_id):
        """
        Read data stored in gridfs and load it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param gridfs_id: the object id of the data stored in gridfs.
        :type gridfs_id: :class:`bson.objectid.ObjectId`
        """
        gfsh = gridfs.GridFS(self)
        fh = gfsh.get(file_id=gridfs_id)
        ub = pickle.load(fh)
        fmt = "@%dd" % int(len(ub) / 8)
        x = struct.unpack(fmt, ub)
        if isinstance(mspass_object, TimeSeries):
            mspass_object.data = DoubleVector(x)
        elif isinstance(mspass_object, Seismogram):
            if not mspass_object.is_defined('npts'):
                raise KeyError("npts is not defined")
            if len(x) != (3 * mspass_object['npts']):
                emess = "Size mismatch in sample data. Number of points in gridfs file = %d but expected %d" \
                        % (len(x), (3 * mspass_object['npts']))
                raise ValueError(emess)
            mspass_object.data = dmatrix(3, mspass_object['npts'])
            for i in range(3):
                for j in range(mspass_object['npts']):
                    mspass_object.data[i, j] = x[i * mspass_object['npts'] + j]
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")

    @staticmethod
    def _extract_locdata(chanlist):
        """
        Parses the list returned by obspy channels attribute
        for a Station object and returns a dict of unique
        edepth values keyed by loc code.  This algorithm
        would be horribly inefficient for large lists with
        many duplicates, but the assumption here is the list
        will always be small
        """
        alllocs = {}
        for chan in chanlist:
            alllocs[chan.location_code] = [
                chan.start_date,
                chan.end_date,
                chan.latitude,
                chan.longitude,
                chan.elevation,
                chan.depth]
        return alllocs

    def _site_is_not_in_db(self, record_to_test):
        """
        Small helper functoin for save_inventory.
        Tests if dict content of record_to_test is
        in the site collection.  Inverted logic in one sense
        as it returns true when the record is not yet in
        the database.  Uses key of net,sta,loc,starttime
        and endtime.  All tests are simple equality.
        Should be ok for times as stationxml uses nearest
        day as in css3.0.

        originally tried to do the time interval tests with a
        query, but found it was a bit cumbersone to say the least.
        Because this particular query is never expected to return
        a large number of documents we resort to a linear
        search through all matches on net,sta,loc rather than
        using a confusing and ugly query construct.
        """
        dbsite = self.site
        queryrecord = {}
        queryrecord['net'] = record_to_test['net']
        queryrecord['sta'] = record_to_test['sta']
        queryrecord['loc'] = record_to_test['loc']
        matches = dbsite.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec = matches.count()
        if (nrec <= 0):
            return True
        else:
            # Now do the linear search on time for a match
            st0 = record_to_test['starttime']
            et0 = record_to_test['endtime']
            time_fudge_factor = 10.0
            stp = st0 + time_fudge_factor
            stm = st0 - time_fudge_factor
            etp = et0 + time_fudge_factor
            etm = et0 - time_fudge_factor
            for x in matches:
                sttest = x['starttime']
                ettest = x['endtime']
                if (sttest > stm and sttest < stp and ettest > etm and ettest < etp):
                    return False
            return True

    def _channel_is_not_in_db(self, record_to_test):
        """
        Small helper functoin for save_inventory.
        Tests if dict content of record_to_test is
        in the site collection.  Inverted logic in one sense
        as it returns true when the record is not yet in
        the database.  Uses key of net,sta,loc,starttime
        and endtime.  All tests are simple equality.
        Should be ok for times as stationxml uses nearest
        day as in css3.0.
        """
        dbchannel = self.channel
        queryrecord = {}
        queryrecord['net'] = record_to_test['net']
        queryrecord['sta'] = record_to_test['sta']
        queryrecord['loc'] = record_to_test['loc']
        queryrecord['chan'] = record_to_test['chan']
        matches = dbchannel.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec = matches.count()
        if (nrec <= 0):
            return True
        else:
            # Now do the linear search on time for a match
            st0 = record_to_test['starttime']
            et0 = record_to_test['endtime']
            time_fudge_factor = 10.0
            stp = st0 + time_fudge_factor
            stm = st0 - time_fudge_factor
            etp = et0 + time_fudge_factor
            etm = et0 - time_fudge_factor
            for x in matches:
                sttest = x['starttime']
                ettest = x['endtime']
                if (sttest > stm and sttest < stp and ettest > etm and ettest < etp):
                    return False
            return True

    def _handle_null_starttime(self, t):
        if t == None:
            return UTCDateTime(0.0)
        else:
            return t

    def _handle_null_endtime(self, t):
        # This constant is used below to set endtime to a time
        # in the far future if it is null
        DISTANTFUTURE = UTCDateTime(2051, 1, 1, 0, 0)
        if t == None:
            return DISTANTFUTURE
        else:
            return t

    def save_inventory(self, inv,
                       networks_to_exclude=['SY'],
                       verbose=False):
        """
        Saves contents of all components of an obspy inventory
        object to documents in the site and channel collections.
        The site collection is sufficient of Seismogram objects but
        TimeSeries data will often want to be connected to the
        channel collection.   The algorithm used will not add
        duplicates based on the following keys:

        For site:
            net
            sta
            chan
            loc
            starttime::endtime - this check is done cautiously with
              a 10 s fudge factor to avoid the issue of floating point
              equal tests.   Probably overly paranoid since these
              fields are normally rounded to a time at the beginning
              of a utc day, but small cost to pay for stabilty because
              this function is not expected to be run millions of times
              on a huge collection.

        for channels:
            net
            sta
            chan
            loc
            starttime::endtime - same approach as for site with same
               issues - note especially 10 s fudge factor.   This is
               necessary because channel metadata can change more
               frequently than site metadata (e.g. with a sensor
               orientation or sensor swap)

        Finally note the site collection contains full response data
        that can be obtained by extracting the data with the key
        "serialized_inventory" and running pickle loads on the returned
        string.

        :param inv: is the obspy Inventory object of station data to save.
        :networks_to_exclude: should contain a list (or tuple) of
            SEED 2 byte network codes that are to be ignored in
            processing.   Default is SY which is used for synthetics.
            Set to None if if all are to be loaded.
        :verbose:  print informational lines if true.  If false
        works silently)

        :return:  tuple with
          0 - integer number of site documents saved
          1 -integer number of channel documents saved
          2 - number of distinct site (net,sta,loc) items processed
          3 - number of distinct channel items processed
        :rtype: tuple
        """

        # site is a frozen name for the collection here.  Perhaps
        # should be a variable with a default
        # to do: need to change source_id to be a copy of the _id string.

        dbcol = self.site
        dbchannel = self.channel
        n_site_saved = 0
        n_chan_saved = 0
        n_site_processed = 0
        n_chan_processed = 0
        for x in inv:
            # Inventory object I got from webservice download
            # makes the sta variable here a net:sta combination
            # We can get the net code like this
            net = x.code
            # This adds feature to skip data for any net code
            # listed in networks_to_exclude
            if networks_to_exclude != None:
                if net in networks_to_exclude:
                    continue
            # Each x now has a station field, BUT tests I ran
            # say for my example that field has one entry per
            # x.  Hence, we can get sta name like this
            y = x.stations
            sta = y[0].code
            starttime = y[0].start_date
            endtime = y[0].end_date
            starttime = self._handle_null_starttime(starttime)
            endtime = self._handle_null_endtime(endtime)
            latitude = y[0].latitude
            longitude = y[0].longitude
            # stationxml files seen to put elevation in m. We
            # always use km so need to convert
            elevation = y[0].elevation / 1000.0
            # an obnoxious property of station xml files obspy is giving me
            # is that the start_dates and end_dates on the net:sta section
            # are not always consistent with the channel data.  In particular
            # loc codes are a problem. So we pull the required metadata from
            # the chans data and will override locations and time ranges
            # in station section with channel data
            chans = y[0].channels
            locdata = self._extract_locdata(chans)
            # Assume loc code of 0 is same as rest
            # loc=_extract_loc_code(chanlist[0])
            picklestr = pickle.dumps(x)
            all_locs = locdata.keys()
            for loc in all_locs:
                # If multiple loc codes are present on the second pass
                # rec will contain the objectid of the document inserted
                # in the previous pass - an obnoxious property of insert_one
                # This initialization guarantees an empty container
                rec = dict()
                rec['loc'] = loc
                rec['net'] = net
                rec['sta'] = sta
                lkey = loc
                loc_tuple = locdata[lkey]
                # We use these attributes linked to loc code rather than
                # the station data - experience shows they are not
                # consistent and we should use this set.
                loc_lat = loc_tuple[2]
                loc_lon = loc_tuple[3]
                loc_elev = loc_tuple[4]
                # for consistency convert this to km too
                loc_elev = loc_elev / 1000.0
                loc_edepth = loc_tuple[5]
                loc_stime = loc_tuple[0]
                loc_stime = self._handle_null_starttime(loc_stime)
                loc_etime = loc_tuple[1]
                loc_etime = self._handle_null_endtime(loc_etime)
                rec['latitude'] = loc_lat
                rec['longitude'] = loc_lon
                # This is MongoDBs way to set a geographic
                # point - allows spatial queries.  Note longitude
                # must be first of the pair
                rec['coords'] = [loc_lat, loc_lon]
                rec['elevation'] = loc_elev
                rec['edepth'] = loc_edepth
                rec['starttime'] = starttime.timestamp
                rec['endtime'] = endtime.timestamp
                if latitude != loc_lat or longitude != loc_lon or elevation != loc_elev:
                    print(net, ":", sta, ":", loc,
                          " (Warning):  station section position is not consistent with loc code position")
                    print("Data in loc code section overrides station section")
                    print("Station section coordinates:  ", latitude, longitude, elevation)
                    print("loc code section coordinates:  ", loc_lat, loc_lon, loc_elev)
                if self._site_is_not_in_db(rec):
                    result=dbcol.insert_one(rec)
                    # Note this sets site_id to an ObjectID for the insertion
                    # We use that to define a duplicate we tag as site_id
                    site_id=result.inserted_id
                    self.site.update_one({'_id':site_id},{'$set':{'site_id' : site_id}})
                    n_site_saved+=1
                    if verbose:
                        print("net:sta:loc=", net, ":", sta, ":", loc,
                              "for time span ", starttime, " to ", endtime,
                              " added to site collection")
                else:
                    if verbose:
                        print("net:sta:loc=", net, ":", sta, ":", loc,
                              "for time span ", starttime, " to ", endtime,
                              " is already in site collection - ignored")
                n_site_processed += 1
                # done with site now handle channel
                # Because many features are shared we can copy rec
                # note this has to be a deep copy
                chanrec = copy.deepcopy(rec)
                # We don't want this baggage in the channel documents
                # keep them only in the site collection
                # del chanrec['serialized_inventory']
                for chan in chans:
                    chanrec['chan'] = chan.code
                    chanrec['vang'] = chan.dip
                    chanrec['hang'] = chan.azimuth
                    chanrec['edepth'] = chan.depth
                    st = chan.start_date
                    et = chan.end_date
                    # as above be careful of null values for either end of the time range
                    st = self._handle_null_starttime(st)
                    et = self._handle_null_endtime(et)
                    chanrec['starttime'] = st.timestamp
                    chanrec['endtime'] = et.timestamp
                    n_chan_processed += 1
                    if (self._channel_is_not_in_db(chanrec)):
                        picklestr = pickle.dumps(chan)
                        chanrec['serialized_channel_data'] = picklestr
                        result = dbchannel.insert_one(chanrec)
                        # insert_one has an obnoxious behavior in that it
                        # inserts the ObjectId in chanrec.  In this loop
                        # we reuse chanrec so we have to delete the id field
                        # howeveer, we first want to update the record to
                        # have chan_id provide an  alternate key to that id
                        # object_id - that makes this consistent with site
                        # we actually use the return instead of pulling from
                        # chanrec
                        idobj=result.inserted_id
                        dbchannel.update_one({'_id':idobj},
                                             {'$set':{'chan_id' : idobj}})
                        del chanrec['_id']
                        n_chan_saved += 1
                        if verbose:
                            print("net:sta:loc:chan=",
                                  net, ":", sta, ":", loc, ":", chan.code,
                                  "for time span ", st, " to ", et,
                                  " added to channel collection")
                    else:
                        if verbose:
                            print('net:sta:loc:chan=',
                                  net, ":", sta, ":", loc, ":", chan.code,
                                  "for time span ", st, " to ", et,
                                  " already in channel collection - ignored")

        # Tried this to create a geospatial index.   Failing
        # in later debugging for unknown reason.   Decided it
        # should be a done externally anyway as we don't use
        # that feature now - thought of doing so but realized
        # was unnecessary baggage
        # dbcol.create_index(["coords",GEOSPHERE])
        #
        # For now we will always print this summary information
        # For expected use it would be essential information
        #
        print("Database.save_inventory processing summary:")
        print("Number of site records processed=", n_site_processed)
        print("number of site records saved=", n_site_saved)
        print("number of channel records processed=", n_chan_processed)
        print("number of channel records saved=", n_chan_saved)
        return tuple([n_site_saved, n_chan_saved, n_site_processed, n_chan_processed])

    def load_seed_station(self, net, sta, loc='NONE', time=-1.0):
        """
        The site collection is assumed to have a one to one
        mapping of net:sta:loc:starttime - endtime.
        This method uses a restricted query to match the
        keys given and returns a dict of coordinate data;
        lat, lon, elev, edepth.
        The (optional) time arg is used for a range match to find
        period between the site startime and endtime.
        Returns None if there is no match.

        The seed modifier in the name is to emphasize this method is
        for data originating as the SEED format that use net:sta:loc:chan
        as the primary index.

        :param net:  network name to match
        :param sta:  station name to match
        :param loc:   optional loc code to made (empty string ok and common)
        default ignores loc in query.
        :param time: epoch time for requested metadata

        :return: handle to query result
        :rtype:  MondoDB Cursor object of query result.
        """
        dbsite = self.site
        query = {}
        query['net'] = net
        query['sta'] = sta
        if (loc != 'NONE'):
            query['loc'] = loc
        if (time > 0.0):
            query['starttime'] = {"$lt": time}
            query['endtime'] = {"$gt": time}
        matchsize = dbsite.count_documents(query)
        if (matchsize == 0):
            return None
        else:
            stations = dbsite.find(query)
            if (matchsize > 1):
                print("load_seed_site (WARNING):  query=", query)
                print("Returned ", matchsize, " documents - should be exactly one")
            return stations

    def load_seed_channel(self, net, sta, chan, loc='NONE', time=-1.0):
        """
        The channel collection is assumed to have a one to one
        mapping of net:sta:loc:chan:starttime - endtime.
        This method uses a restricted query to match the
        keys given and returns a dict of the document contents
        associated with that key.
        The (optional) time arg is used for a range match to find
        period between the site startime and endtime.  If not used
        the first occurence will be returned (usually ill adivsed)
        Returns None if there is no match.

        :param net:  network name to match
        :param sta:  station name to match
        :param chan:  seed channel code to match
        :param loc:   optional loc code to made (empty string ok and common)
        default ignores loc in query.
        :param time: epoch time for requested metadata

        :return: handle to query return
        :rtype:  MondoDB Cursor object of query result.
        """
        dbchannel = self.channel
        query = {}
        query['net'] = net
        query['sta'] = sta
        if (loc == 'NONE'):
            query['loc'] = ""
        else:
            query['loc'] = loc
        query['chan'] = chan
        if (time > 0.0):
            query['starttime'] = {"$lt": time}
            query['endtime'] = {"$gt": time}
        matchsize = dbchannel.count_documents(query)
        if (matchsize == 0):
            return None
        else:
            channel = dbchannel.find(query)
            if (matchsize > 1):
                print("load_seed_channel (WARNING):  query=", query)
                print("Returned ", matchsize, " documents - should be exactly one")
            return channel

    def load_inventory(self, net=None, sta=None, loc=None, time=None):
        """
        Loads an obspy inventory object limited by one or more
        keys.   Default is to load the entire contents of the
        site collection.   Note the load creates an obspy
        inventory object that is returned.  Use load_stations
        to return the raw data used to construct an Inventory.

        :param net:  network name query string.  Can be a single
        unique net code or use MongoDB's expression query
        mechanism (e.g. "{'$gt' : 42}).  Default is all
        :param sta: statoin name query string.  Can be a single
        station name or a MongoDB query expression.
        :param loc:  loc code to select.  Can be a single unique
        location (e.g. '01') or a MongoDB expression query.
        :param time:   limit return to stations with
        startime<time<endtime.  Input is assumed an
        epoch time NOT an obspy UTCDateTime. Use a conversion
        to epoch time if necessary.
        :return:  obspy Inventory of all stations matching the
        query parameters
        :rtype:  obspy Inventory
        """
        dbsite = self.site
        query = {}
        if (net != None):
            query['net'] = net
        if (sta != None):
            query['sta'] = sta
        if (loc != None):
            query['loc'] = loc
        if (time != None):
            query['starttime'] = {"$lt": time}
            query['endtime'] = {"$gt": time}
        matchsize = dbsite.count_documents(query)
        result = Inventory()
        if (matchsize == 0):
            return None
        else:
            stations = dbsite.find(query)
            for s in stations:
                serialized = s['serialized_inventory']
                netw = pickle.loads(serialized)
                # It might be more efficient to build a list of
                # Network objects but here we add them one
                # station at a time.  Note the extend method
                # if poorly documented in obspy
                result.extend([netw])
        return result

    def save_catalog(self, cat, verbose=False):
        """
        Save the contents of an obspy Catalog object to MongoDB
        source collection.  All contents are saved even with
        no checking for existing sources with duplicate
        data.   Like the comparable save method for stations,
        save_inventory, the assumption is pre or post cleanup
        will be preformed if duplicates are a major issue.

        :param cat: is the Catalog object to be saved
        :param verbose: Print informational data if true.
        When false (default) it does it's work silently.

        :return: integer count of number of items saved
        """
        # perhaps should demand db is handle to the source collection
        # but I think the cost of this lookup is tiny
        # to do: need to change source_id to be a copy of the _id string.

        dbcol = self.source
        nevents = 0
        for event in cat:
            # event variable in loop is an Event object from cat
            o = event.preferred_origin()
            m = event.preferred_magnitude()
            picklestr = pickle.dumps(event)
            rec = {}
            # rec['source_id']=source_id
            rec['latitude'] = o.latitude
            rec['longitude'] = o.longitude
            # It appears quakeml puts source depths in meter
            # convert to km
            # also obspy's catalog object seesm to allow depth to be
            # a None so we have to test for that condition to avoid
            # aborts
            if o.depth == None:
                depth = 0.0
            else:
                depth = o.depth / 1000.0
            rec['depth'] = depth
            otime = o.time
            # This attribute of UTCDateTime is the epoch time
            # In mspass we only story time as epoch times
            rec['time']=otime.timestamp
            rec['magnitude']=m.mag
            rec['magnitude_type']=m.magnitude_type
            rec['serialized_event']=picklestr
            result=dbcol.insert_one(rec)
            # the return of an insert_one has the object id of the insertion
            # set as inserted_id.  We save taht as source_id as a more
            # intuitive key that _id
            idobj=result.inserted_id
            dbcol.update_one({'_id':idobj},
                        {'$set':{'source_id' : idobj}})
            nevents += 1
        return nevents

    def load_event(self, source_id):
        """
        Return a bson record of source data matching the unique id
        defined by source_id.   The idea is that magic string would
        be extraced from another document (e.g. in an arrival collection)
        and used to look up the event with which it is associated in
        the source collection.

        This function is a relic and may be depricated.  I originally
        had a different purpose.
        """
        dbsource = self.source
        x = dbsource.find_one({'source_id': source_id})
        return x

    # TODO: the following is not used when data is read from the database. We need
    #       link these metadata keys with the actual member variables just like the 
    #       npts or t0 in TimeSeries.
    @staticmethod
    def _sync_time_metadata(mspass_object):
        """
        MsPASS data objects are designed to cleanly handle what we call relative
        and UTC time.  This small helper function assures the Metadata of
        mspass_object are consistent with the internal contents.  That
        involves posting some special attributes seen below to handle this issue.
        Since Metadata is volatile we need to be sure these are consistent or
        timing can be destroyed on data.
        """
        # this adds a small overhead but it guarantees Metadata and internal t0
        # values are consistent.  Shouldn't happen unless the user messes with them
        # incorrectly, but this safety is prudent to reduce the odds of mysterious
        # timing errors in data
        t0 = mspass_object.t0
        mspass_object.set_t0(t0)
        # This will need to be modified if we ever expand time types beyond two
        if mspass_object.time_is_relative():
            if mspass_object.shifted():
                mspass_object['startime_shift'] = mspass_object.time_reference()
                mspass_object['utc_convertible'] = True
            else:
                mspass_object['utc_convertible'] = False
            mspass_object['time_standard'] = 'Relative'
        else:
            mspass_object['utc_convertible'] = True
            mspass_object['time_standard'] = 'UTC'

    @staticmethod
    def _sync_ensemble_metadata(ensemble, do_not_copy=None):
        """
        An ensemble has a Metadata object attached.  The assumption always is
        that every member of the ensemble is appropriate to associate with the
        ensemble Metadata attributes.  This function merges the ensembles
        Metadata to each member using the Metadata += operator.  An optional
        do_not_copy list of keys can  be supplied.  Keys listed found in the
        ensemble Metadata will not be pushed to the members.

        :param ensemble: ensemble object (requirement is only that it has
        Metadata as a parent and contains a members vector of data with
        their own Metadata container - TimeSeriesEnsemble or SeismogramEnsemble)
        with members to receive the ensemble attributes.
        :param do_not_copy: is a list of keys that should not be copied to
        members.  The default is none.  Keys listed but not in the ensemble
        Metadata will be silently ignored.
        """
        md_to_copy = Metadata(ensemble)
        if do_not_copy != None:
            for k in do_not_copy:
                if md_to_copy.is_defined(k):
                    md_to_copy.erase(k)
        for d in ensemble.member:
            # This needs to be replaced by a new method for
            # copying metadata.  Using operator += is problematic
            # for multiple reasons discussed in github issues.
            for k in md_to_copy.keys():
                val = md_to_copy[k]
                d.put(k, val)
