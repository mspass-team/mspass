
"""
Tools for connecting to MongoDB.
"""
import math
import os
import copy
import pickle
import struct
import sys
import traceback

import bson.errors
import bson.objectid
import gridfs
import pymongo
import numpy as np
from array import array

from mspasspy.ccore.seismic import (BasicTimeSeries,
                            TimeSeries,
                            Seismogram,
                            CoreSeismogram,
                            CoreTimeSeries,
                            TimeReferenceType,
                            TimeSeries,
                            DoubleVector)
from mspasspy.ccore.utility import (MetadataDefinitions,
                                    Metadata,
                                    dmatrix,
                                    ErrorLogger,
                                    ErrorSeverity, ProcessingHistory)


from obspy import Inventory
from obspy import UTCDateTime
from obspy import Catalog

class Database(pymongo.database.Database):
    """
    A MongoDB database handler.

    This is a wrapper around the :class:`~pymongo.database.Database` with
    methods added to handle MsPASS data.  The one and only constructor
    uses a database handle normally created with a variant of this pair
    of commands:
        client=MongoClient()
        db=client['databasename']
    where databasename is variable and the name of the database you
    wish to access with this handle.
    """
    def __init__(self, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.metadata_def = MetadataDefinitions()

    def set_metadata_definition(self, definition):
        self.metadata_def = definition

    def read_data(self, object_id, collection="wf", load_history=True, metadata_def=None):
        """

        :param metadata_def:
        :param load_history:
        :param object_id:
        :type object_id: ~bson.objectid.ObjectId
        :param collection:
        :return:
        """
        col = self[collection]
        object = col.find_one({'_id': object_id})
        if not object:
            return None

        # 1. build metadata as dict
        md = Metadata()
        for k in object:
            if k == '_id':
                md['wf_id'] = str(object[k])
                continue
            md[k] = object[k]

        # fixme assert not none?
        site = self['site'].find_one({'_id': bson.objectid.ObjectId(object['site_id'])})
        md['site_lat'] = site['lat']
        md['site_lon'] = site['lon']
        md['site_elev'] = site['elev']
        md['site_starttime'] = site['starttime']
        md['site_endtime'] = site['endtime']
        md['net'] = site['net']
        md['sta'] = site['sta']
        md['loc'] = site['loc']

        source = self['source'].find_one({'_id': bson.objectid.ObjectId(object['source_id'])})
        md['source_lat'] = source['lat']
        md['source_lon'] = source['lon']
        md['source_depth'] = source['depth']
        md['source_time'] = source['time']
        # md['source_magnitude'] = source['magnitude'] # fixme

        if object['dtype'] == 'TimeSeries':
            channel = self['channel'].find_one({'_id': bson.objectid.ObjectId(object['channel_id'])})
            md['chan'] = channel['chan']
            md['channel_hang'] = channel['hang']
            md['channel_vang'] = channel['vang']
            md['channel_lat'] = channel['lat']
            md['channel_lon'] = channel['lon']
            md['channel_elev'] = channel['elev']
            md['channel_edepth'] = channel['edepth']
            md['channel_starttime'] = channel['starttime']
            md['channel_endtime'] = channel['endtime']

        read_metadata_def = self.metadata_def if not metadata_def else metadata_def
        for k in md:
            if read_metadata_def.is_defined(k):
                if type(md[k]) != self._mspass_type_helper(read_metadata_def.type(k)):
                    raise TypeError('{} has type {}, forbidden by definition'.format(k, type(md[k])))

        if object['dtype'] == 'TimeSeries':
            mspass_object = TimeSeries({k:md[k] for k in md}, np.ndarray([0], dtype=np.float64))
            mspass_object.npts = md['npts']  # fixme
        elif object['dtype'] == 'Seismogram':
            mspass_object = Seismogram(CoreSeismogram(md, False))
        else:
            return None

        # 2.load data from different modes
        mode = mspass_object['storage_mode']
        if mode == "file":
            self._read_data_from_dfile(mspass_object)
        elif mode == "gridfs":
            self._read_data_from_gridfs(mspass_object)
        elif mode == "url":
            pass  # todo for future
        else:
            raise TypeError("Unknown storage mode: {}".format(mode))

        # 3.load history
        if load_history:
            self._load_history(mspass_object=mspass_object)

        mspass_object.live = True
        mspass_object.clear_modified()
        return mspass_object

    def save_data(self, mspass_object, storage_mode, update_all=False, dfile=None, dir=None,
                  exclude=None, collection='wf', metadata_def=None):

        # 1. save data
        if storage_mode == "file":
            if dfile: # new file
                mspass_object['dfile'] = dfile
                mspass_object['dir'] = dir
            self._save_data_to_dfile(mspass_object)
        elif storage_mode == "gridfs":
            self._save_data_to_gridfs(mspass_object)
        elif storage_mode == "url":
            pass
        else:
            raise TypeError("Unknown storage mode: {}".format(storage_mode))
        mspass_object['storage_mode'] = storage_mode

        # 2. save metadata
        self.update_metadata(mspass_object, update_all, exclude, collection, metadata_def)

    def update_metadata(self, mspass_object, update_all=False, exclude=None, collection='wf', metadata_def=None):
        # 1. save history
        self._save_history(mspass_object)

        # 2. save error logs
        new_insertion = False
        if 'wf_id' not in mspass_object:
            new_insertion = True
        self._save_elog(mspass_object)  # elog ids will be updated in the wf col when saving metadata

        # 3. save metadata in wf
        if exclude is None:
            exclude = []
        col = self[collection]
        insert_dict = {}

        self._sync_metadata_before_update(mspass_object)
        copied_metadata = Metadata(mspass_object)
        update_metadata_def = self.metadata_def if not metadata_def else metadata_def
        update_metadata_def.clear_aliases(copied_metadata)

        for k in copied_metadata:
            if not str(copied_metadata[k]).strip():
                copied_metadata.clear(k)

        # skip _id and attributes in other collections
        skip_list = ['wf_id', 'site_lat', 'site_lon', 'site_elev', 'site_starttime', 'site_endtime', 'source_lat',
                     'source_lon', 'source_depth', 'source_time', 'chan', 'hang', 'vang', 'chan_id', 'channel_hang',
                     'channel_vang', 'channel_lat', 'channel_lon', 'channel_elev', 'channel_edepth',
                     'channel_starttime', 'channel_endtime', 'net', 'sta', 'loc']
        for k in copied_metadata:
            if k in exclude or k in skip_list:
                continue
            if update_metadata_def.is_defined(k):
                if type(copied_metadata[k]) != self._mspass_type_helper(update_metadata_def.type(k)):
                    raise TypeError('{} has type {}, forbidden by definition'.format(k, type(copied_metadata[k])))
                if not update_metadata_def.readonly(k):
                    insert_dict[k] = copied_metadata[k]
            elif update_all:
                insert_dict[k] = copied_metadata[k]

        if 'wf_id' not in copied_metadata:
            wf_id = str(col.insert_one(insert_dict).inserted_id)
            mspass_object['wf_id'] = wf_id
        else:
            filter = {'_id': bson.objectid.ObjectId(copied_metadata['wf_id'])}
            col.update_one(filter, {'$set': insert_dict})

        # 4. need to save the wf_id if the mspass_object is saved at the first time
        if new_insertion:
            col = self['error_logs']
            for id in mspass_object['elog_ids']:
                filter = {'_id': bson.objectid.ObjectId(id)}
                col.update_one(filter, {'$set': {'wf_id': mspass_object['wf_id']}})

    def detele_wf(self, wf_id, collection='wf'):
        self[collection].delete_one({'_id': wf_id})

    def delete_gridfs(self, gridfs_id):
        gfsh = gridfs.GridFS(self)
        if gfsh.exists(gridfs_id):
            gfsh.delete(gridfs_id)

    def _mspass_type_helper(self, type):
        if str(type) == 'MDtype.Double':
            return float
        elif str(type) == 'MDtype.String':
            return str
        elif str(type) == 'MDtype.Boolean':
            return bool
        elif str(type) == 'MDtype.Double_Array':
            return list
        elif str(type) == 'MDtype.Int64' or str(type) == 'MDtype.Int32':
            return int
        else:
            return None

    def _sync_metadata_before_update(self, d):
        if d.tref == TimeReferenceType.Relative:
            d.put_string('time_standard', 'relative')
        else:
            d.put_string('time_standard', 'UTC')
        if 'dtype' not in d:
            if isinstance(d, TimeSeries):
                d['dtype'] = 'TimeSeries'
            elif isinstance(d, Seismogram):
                d['dtype'] = 'Seismogram'
            else:
                raise TypeError("only TimeSeries and Seismogram are supported")

    def _save_history(self, mspass_object, collection='history'):
        history_col = self[collection]
        history_binary = pickle.dumps(ProcessingHistory(mspass_object))
        if 'history_id' in mspass_object:
            # overwrite history
            filter = {'_id': bson.objectid.ObjectId(mspass_object['history_id'])}
            history_col.find_one_and_replace(filter, {'history_binary': history_binary})
        else:
            id = history_col.insert_one({'history_binary': history_binary}).inserted_id
            mspass_object['history_id'] = str(id)

    def _load_history(self, mspass_object, collection='history'):
        if 'history_id' not in mspass_object:
            raise KeyError("history_id not found")
        res = self[collection].find_one({'_id': bson.objectid.ObjectId(mspass_object['history_id'])})
        mspass_object.load_history(pickle.loads(res['history_binary']))

    def _save_elog(self, data, collection='error_logs'):
        """
        Save error log for a data object.

        Data objects in MsPASS contain an error log object used to post any
        errors handled by processing functions.   These have different levels of
        severity.   This method posts each log entry into the elog document with
        the Objectid of the parent posted as a tag.  We only post the object id
        as a simple string and make no assumptions it is valid.   To convert it
        to a valid ObjectID in Mongo would require calls to the ObjectID constuctor.
        We view that as postprocessing problem to handle.

        Args:
        oidstr is the ObjectID represented as a string.  It is normally pulled from
            Metadata with the key oid_string.
        elog is the error log object to be saved.
        Return:  List of ObjectID of inserted
        """
        if 'elog_ids' not in data:
            data['elog_ids'] = []
        oidstr = None
        if 'wf_id' in data:
            oidstr = data['wf_id']
        elog = data.elog
        n = elog.size()
        if n == 0:
            return
        errs = elog.get_error_log()
        jobid = elog.get_job_id()
        oidlst = []
        for i in range(n):
            x = errs[i]
            docentry = {'job_id': jobid, 'algorithm': x.algorithm, 'badness': str(x.badness),
                        'error_message': x.message, 'process_id': x.p_id}
            if oidstr:
                docentry['wf_id'] = oidstr
            oid = self[collection].insert_one(docentry).inserted_id
            oidlst.append(str(oid))
        data['elog_ids'].extend(oidlst)

    @staticmethod
    def _read_data_from_dfile(d):
        if ('dir' not in d) or ('dfile' not in d) or ('foff' not in d):
            raise KeyError("one of the keys: dir, dfile, foff is missing")
        di = d.get_string('dir')
        dfile = d.get_string('dfile')
        foff = d.get('foff')
        fname = os.path.join(di, dfile)
        with open(fname, mode='rb') as fh:
            fh.seek(foff)
            float_array = array('d')
            if isinstance(d, TimeSeries):
                if not d.is_defined('npts'):
                    raise KeyError("npts is not defined")
                float_array.frombytes(fh.read(d.get('npts') * 8))
                d.data = DoubleVector(float_array)
            elif isinstance(d, Seismogram):
                if not d.is_defined('npts'):
                    raise KeyError("npts is not defined")
                float_array.frombytes(fh.read(d.get('npts') * 8 * 3))
                print(len(float_array))
                d.data = dmatrix(3, d.get('npts'))
                for i in range(3):
                    for j in range(d.get('npts')):
                        d.data[i, j] = float_array[i * d.get('npts') + j]
            else:
                raise TypeError("only TimeSeries and Seismogram are supported")

    @staticmethod
    def _save_data_to_dfile(d):
        """
        Saves sample data as a binary dump of the sample data.

        Save a Seismogram object as a pure binary dump of the sample data
        in native (Fortran) order.   The file name to write is derived from
        dir and dfile in the usual way, but frozen to unix / separator.
        Opens the file and ALWAYS appends data to the end of the file.

        :param: d is a Seismogram object whose data is to be saved

        :returns:  -1 if failure.  Position of first data sample (foff) for success
        :raise:  None. Any io failures will be trapped and posted to the elog area of
        the object d.   Caller should test for negative return and post the error
        to the database to help debug data problems.
        """
        if ('dir' not in d) or ('dfile' not in d):
            raise KeyError("one of dir, dfile is missing")
        di = d.get_string('dir')
        dfile = d.get_string('dfile')
        fname = os.path.join(di, dfile)
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        with open(fname, mode='a+b') as fh:
            foff = fh.seek(0, 2)
            if isinstance(d, TimeSeries):
                ub = bytes(np.array(d.data))  # fixme DoubleVector
            elif isinstance(d, Seismogram):
                ub = bytes(d.data)
            else:
                raise TypeError("only TimeSeries and Seismogram are supported")
            fh.write(ub)
        di = os.path.dirname(os.path.realpath(fname))
        dfile = os.path.basename(os.path.realpath(fname))
        d.put('dir', di)
        d.put('dfile', dfile)
        d.put('foff', foff)

    def _save_data_to_gridfs(self, d):
        """
        Save a Seismogram object sample data to MongoDB gridfs_wf collection.

        Use this method for saving a Seismogram inside MongoDB.   This is
        the recommended mode for anything but data to be exported or data that
        is expected to remain static.   External files are subject to several
        issues to beware of before using them:  (1) they are subject to damage
        by other processes/program, (2) updates are nearly impossible without
        stranding (potentially large quantities) of data in the middle of files or
        corrupting a file with a careless insert, and (3) when the number of files
        gets large managing them becomes difficult.

        :param d: is the Seismogram to be saved
        :param fscol: is the gridfs collection name to save the data in
            (default is 'gridfs_wf')
        :return: object_id of the document used to store the data in gridfs
            -1 if something failed.  In that condition a generic error message
            is posted to elog.    Caller should dump elog only after
            trying to do this write to preserve the log
        :raise: Should never throw an exception, but caller should test and save
        error log if it is not empty.
        """
        gfsh = gridfs.GridFS(self)
        if d.is_defined('gridfs_id'):
            ids = d.get_string('gridfs_id')
            oid = bson.objectid.ObjectId(ids)
            if gfsh.exists(oid):
                gfsh.delete(oid)
        if isinstance(d, Seismogram):
            ub = bytes(d.data)
        else:
            ub = bytes(np.array(d.data))
        file_id = gfsh.put(pickle.dumps(ub))
        d.put_string('gridfs_id', str(file_id))

    def _read_data_from_gridfs(self, d):
        """
        Load a Seismogram object stored as a gridfs file.

        Constructs a Seismogram object from Metadata and sample data
        pulled from a MongoDB gridfs document.   The Metadata must contain
        a string representation of the ObjectId of the document with the
        key gridfs_wf_id.  That string is used to generate a unique ObjectId
        which is then used to find the unique document containing the sample
        data in the collection called gridfs_wf (optionally can be changed with
        argument fscol)

        This function was designed to never throw an exception but always
        return some form of Seismogram object. Caller should test the boolean
        'live" attribute of the return. If it is false, it means this function
        failed completely.  The error log may also contain various levels of
        warning errors posted to it's internal ErrorLogger (elog) object.

        :param md: is the Metadata object used to drive the construction.  This
            would normally be constructed from a parent document in the wf
            collection using dict2Metadata.  A critical key is the entry gridfs_wf_id
            as described above.   Several other key:value pairs are required or
            the function will abort with the result returned as invalid (live=false).
            These are:  npts, starttime, and delta.   time_reference is a special
            switch for handling UTC versus relative time.   Default is UTC
            but relative time can be handled with the attribure t0_shift.
            See User Manual for more about this feature.
        :param elogtmp: is an (optional) ErrorLogger object added to the Seismogram
            object during construction. It's primary use is to preserve any
            warning errors encountered during the construction of md passed
            to the function.
        :param fscol: is the collection name the function should use to find the
            gridfs data document
        :return: the Seismogram object requested
        :rtype: Seismogram
        """
        # First make sure we have a valid id string.  No reason to procede if
        # not the case
        if not d.is_defined('gridfs_id'):
            raise KeyError("gridfs_id is not defined")
        dataid = bson.objectid.ObjectId(d.get_string('gridfs_id'))
        gfsh = gridfs.GridFS(self)
        fh = gfsh.get(file_id=dataid)
        ub = pickle.load(fh)
        fmt = "@%dd" % int(len(ub)/8)
        x = struct.unpack(fmt, ub)
        if isinstance(d, TimeSeries):
            d.data = DoubleVector(x)
        elif isinstance(d, Seismogram):
            if not d.is_defined('npts'):
                raise KeyError("npts is not defined")
            if len(x) != (3 * d['npts']):
                emess = "Size mismatch in sample data. Number of points in gridfs file = %d but expected %d" \
                        % (len(x), (3 * d['npts']))
                raise ValueError(emess)
            d.data = dmatrix(3, d['npts'])
            for i in range(3):
                for j in range(d['npts']):
                    d.data[i, j] = x[i * d['npts'] + j]
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
        alllocs={}
        for chan in chanlist:
            alllocs[chan.location_code]=[
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
        queryrecord={}
        queryrecord['net']=record_to_test['net']
        queryrecord['sta']=record_to_test['sta']
        queryrecord['loc']=record_to_test['loc']
        matches=dbsite.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec=matches.count()
        if(nrec<=0):
            return True
        else:
            # Now do the linear search on time for a match
            st0=record_to_test['starttime']
            et0=record_to_test['endtime']
            time_fudge_factor=10.0
            stp=st0+time_fudge_factor
            stm=st0-time_fudge_factor
            etp=et0+time_fudge_factor
            etm=et0-time_fudge_factor
            for x in matches:
                sttest=x['starttime']
                ettest=x['endtime']
                if( sttest>stm and sttest<stp and ettest>etm and ettest<etp):
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
        queryrecord={}
        queryrecord['net']=record_to_test['net']
        queryrecord['sta']=record_to_test['sta']
        queryrecord['loc']=record_to_test['loc']
        queryrecord['chan']=record_to_test['chan']
        matches=dbchannel.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec=matches.count()
        if(nrec<=0):
            return True
        else:
            # Now do the linear search on time for a match
            st0=record_to_test['starttime']
            et0=record_to_test['endtime']
            time_fudge_factor=10.0
            stp=st0+time_fudge_factor
            stm=st0-time_fudge_factor
            etp=et0+time_fudge_factor
            etm=et0-time_fudge_factor
            for x in matches:
                sttest=x['starttime']
                ettest=x['endtime']
                if( sttest>stm and sttest<stp and ettest>etm and ettest<etp):
                    return False
            return True
    def _handle_null_starttime(self,t):
        if t==None:
            return UTCDateTime(0.0)
        else:
            return t
    def _handle_null_endtime(self,t):
        # This constant is used below to set endtime to a time
        # in the far future if it is null
        DISTANTFUTURE=UTCDateTime(2051,1,1,0,0)
        if t==None:
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
        n_site_saved=0
        n_chan_saved=0
        n_site_processed=0
        n_chan_processed=0
        for x in inv:
            # Inventory object I got from webservice download
            # makes the sta variable here a net:sta combination
            # We can get the net code like this
            net=x.code
            # This adds feature to skip data for any net code
            # listed in networks_to_exclude
            if networks_to_exclude != None:
                if net in networks_to_exclude:
                    continue
            # Each x now has a station field, BUT tests I ran
            # say for my example that field has one entry per
            # x.  Hence, we can get sta name like this
            y=x.stations
            sta=y[0].code
            starttime=y[0].start_date
            endtime=y[0].end_date
            starttime=self._handle_null_starttime(starttime)
            endtime=self._handle_null_endtime(endtime)
            latitude=y[0].latitude
            longitude=y[0].longitude
            # stationxml files seen to put elevation in m. We
            # always use km so need to convert
            elevation=y[0].elevation/1000.0
            # an obnoxious property of station xml files obspy is giving me
            # is that the start_dates and end_dates on the net:sta section
            # are not always consistent with the channel data.  In particular
            # loc codes are a problem. So we pull the required metadata from
            # the chans data and will override locations and time ranges
            # in station section with channel data
            chans=y[0].channels
            locdata = self._extract_locdata(chans)
            # Assume loc code of 0 is same as rest
            #loc=_extract_loc_code(chanlist[0])
            picklestr=pickle.dumps(x)
            all_locs=locdata.keys()
            for loc in all_locs:
		# If multiple loc codes are present on the second pass
		# rec will contain the objectid of the document inserted
		# in the previous pass - an obnoxious property of insert_one
		# This initialization guarantees an empty container
                rec=dict()
                rec['loc']=loc
                rec['net']=net
                rec['sta']=sta
                lkey=loc
                loc_tuple=locdata[lkey]
                # We use these attributes linked to loc code rather than
                # the station data - experience shows they are not
                # consistent and we should use this set.
                loc_lat=loc_tuple[2]
                loc_lon=loc_tuple[3]
                loc_elev=loc_tuple[4]
                # for consistency convert this to km too
                loc_elev = loc_elev/1000.0
                loc_edepth=loc_tuple[5]
                loc_stime=loc_tuple[0]
                loc_stime=self._handle_null_starttime(loc_stime)
                loc_etime=loc_tuple[1]
                loc_etime=self._handle_null_endtime(loc_etime)
                rec['latitude']=loc_lat
                rec['longitude']=loc_lon
                # This is MongoDBs way to set a geographic
                # point - allows spatial queries.  Note longitude
                # must be first of the pair
                rec['coords']=[loc_lat,loc_lon]
                rec['elevation']=loc_elev
                rec['edepth']=loc_edepth
                rec['starttime']=starttime.timestamp
                rec['endtime']=endtime.timestamp
                if latitude!=loc_lat or longitude!=loc_lon or elevation!=loc_elev:
                    print(net,":",sta,":",loc,
                            " (Warning):  station section position is not consistent with loc code position" )
                    print("Data in loc code section overrides station section")
                    print("Station section coordinates:  ",latitude,longitude,elevation)
                    print("loc code section coordinates:  ",loc_lat,loc_lon,loc_elev)
                if self._site_is_not_in_db(rec):
                    result=dbcol.insert_one(rec)
                    # we use the string representation of object_id of this document as site_id
                    # this gyration is required to do handle that
                    idobj=result.inserted_id
                    site_id=str(idobj)
                    self.site.update_one({'_id':idobj},{'$set':{'site_id' : site_id}})
                    n_site_saved+=1
                    if verbose:
                        print("net:sta:loc=",net,":",sta,":",loc,
                            "for time span ",starttime," to ",endtime,
                            " added to site collection")
                else:
                    if verbose:
                        print("net:sta:loc=",net,":",sta,":",loc,
                            "for time span ",starttime," to ",endtime,
                            " is already in site collection - ignored")
                n_site_processed += 1
                # done with site now handle channel
                # Because many features are shared we can copy rec
                # note this has to be a deep copy
                chanrec=copy.deepcopy(rec)
                # We don't want this baggage in the channel documents
                # keep them only in the site collection
                #del chanrec['serialized_inventory']
                for chan in chans:
                    chanrec['chan']=chan.code
                    chanrec['vang']=chan.dip
                    chanrec['hang']=chan.azimuth
                    chanrec['edepth']=chan.depth
                    st=chan.start_date
                    et=chan.end_date
                    # as above be careful of null values for either end of the time range
                    st=self._handle_null_starttime(st)
                    et=self._handle_null_endtime(et)
                    chanrec['starttime']=st.timestamp
                    chanrec['endtime']=et.timestamp
                    n_chan_processed += 1
                    if(self._channel_is_not_in_db(chanrec)):
                        picklestr=pickle.dumps(chan)
                        chanrec['serialized_channel_data']=picklestr
                        result=dbchannel.insert_one(chanrec)
                        # insert_one has an obnoxious behavior in that it
                        # inserts the ObjectId in chanrec.  In this loop
                        # we reuse chanrec so we have to delete the id file
                        # howeveer, we first want to update the record to
                        # have chan_id be the string representation of that
                        # object_id - that makes this consistent with site
                        # we actually use the return instead of pulling from
                        # chanrec
                        idobj=result.inserted_id
                        dbchannel.update_one({'_id':idobj},
                                             {'$set':{'chan_id' : str(idobj)}})
                        del chanrec['_id']
                        n_chan_saved += 1
                        if verbose:
                            print("net:sta:loc:chan=",
                              net,":",sta,":",loc,":",chan.code,
                              "for time span ",st," to ",et,
                              " added to channel collection")
                    else:
                        if verbose:
                            print('net:sta:loc:chan=',
                              net,":",sta,":",loc,":",chan.code,
                              "for time span ",st," to ",et,
                              " already in channel collection - ignored")

        # Tried this to create a geospatial index.   Failing
        # in later debugging for unknown reason.   Decided it
        # should be a done externally anyway as we don't use
        # that feature now - thought of doing so but realized
        # was unnecessary baggage
        #dbcol.create_index(["coords",GEOSPHERE])
        #
        # For now we will always print this summary information
        # For expected use it would be essential information
        #
        print("Database.save_inventory processing summary:")
        print("Number of site records processed=",n_site_processed)
        print("number of site records saved=",n_site_saved)
        print("number of channel records processed=",n_chan_processed)
        print("number of channel records saved=",n_chan_saved)
        return tuple([n_site_saved,n_chan_saved,n_site_processed,n_chan_processed])

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
        dbsite=self.site
        query={}
        query['net']=net
        query['sta']=sta
        if(loc!='NONE'):
            query['loc']=loc
        if(time>0.0):
            query['starttime']={"$lt" : time}
            query['endtime']={"$gt" : time}
        matchsize=dbsite.count_documents(query)
        if(matchsize==0):
            return None
        else:
            stations=dbsite.find(query)
            if(matchsize>1):
                print("load_seed_site (WARNING):  query=",query)
                print("Returned ",matchsize," documents - should be exactly one")
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
        dbchannel=self.channel
        query={}
        query['net']=net
        query['sta']=sta
        if(loc=='NONE'):
            query['loc']=""
        else:
            query['loc']=loc
        query['chan']=chan
        if(time>0.0):
            query['starttime']={"$lt" : time}
            query['endtime']={"$gt" : time}
        matchsize=dbchannel.count_documents(query)
        if(matchsize==0):
            return None
        else:
            channel=dbchannel.find(query)
            if(matchsize>1):
                print("load_seed_channel (WARNING):  query=",query)
                print("Returned ",matchsize," documents - should be exactly one")
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
        dbsite=self.site
        query={}
        if(net!=None):
            query['net']=net
        if(sta!=None):
            query['sta']=sta
        if(loc!=None):
            query['loc']=loc
        if(time!=None):
            query['starttime']={"$lt" : time}
            query['endtime']={"$gt" : time}
        matchsize=dbsite.count_documents(query)
        result=Inventory()
        if(matchsize==0):
            return None
        else:
            stations=dbsite.find(query)
            for s in stations:
                serialized=s['serialized_inventory']
                netw=pickle.loads(serialized)
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
        nevents=0
        for event in cat:
            # event variable in loop is an Event object from cat
            o=event.preferred_origin()
            m=event.preferred_magnitude()
            picklestr=pickle.dumps(event)
            rec={}
            #rec['source_id']=source_id
            rec['latitude']=o.latitude
            rec['longitude']=o.longitude
            # It appears quakeml puts source depths in meter
            # convert to km
            # also obspy's catalog object seesm to allow depth to be
            # a None so we have to test for that condition to avoid
            # aborts
            if o.depth == None:
                depth=0.0
            else:
                depth=o.depth/1000.0
            rec['depth']=depth
            otime=o.time
            # This attribute of UTCDateTime is the epoch time
            # In mspass we only story time as epoch times
            rec['time']=otime.timestamp
            rec['magnitude']=m.mag
            rec['magnitude_type']=m.magnitude_type
            rec['serialized_event']=picklestr
            result=dbcol.insert_one(rec)
            # We use the string representation of the objectid of this
            # document as a unique source id - same as site and channel
            idobj=result.inserted_id
            dbcol.update_one({'_id':idobj},
                        {'$set':{'source_id' : str(idobj)}})
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
        x=dbsource.find_one({'source_id' : source_id})
        return x