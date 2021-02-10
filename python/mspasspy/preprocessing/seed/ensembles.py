import os
from pathlib import Path
from obspy import (read,
                UTCDateTime)
import pandas as pd
import numpy as np
from mspasspy.ccore.utility import (Metadata,
                                    MsPASSError,
                                    ErrorSeverity,
                                    AtomicType,
                                    ProcessingHistory
                                    )
from mspasspy.ccore.seismic import TimeSeriesEnsemble
#from mspasspy.io.converter import Trace2TimeSeries
from mspasspy.util.converter import Trace2TimeSeries

def obspy_mseed_file_indexer(file):
    """
    Use obspy's miniseed reader to eat up a (potentially large) file and
    build an index as a table (returned) of data that can be written to
    a database.   Obspy's reader was written to accept the abomination of
    miniseed with random packets scattered through the file (or so it seems,
    since they have no foff concept in their reader).  Hence, what this reader
    does is make a table entry for each net:sta:chan:loc trace object their
    reader returns.   It does this with panda dataframes to build the
    table.  One required argument is the file name containing the miniseed data.
    """
    try:
        pr=Path(file)
        fullpath=pr.absolute()
        [dirself,dfileself]=os.path.split(fullpath)
        dseis=read(file,format='mseed')
        net=[]
        sta=[]
        chan=[]
        loc=[]
        stime=[]
        etime=[]
        samprate=[]
        delta=[]
        npts=[]
        calib=[]
        dfile=[]
        dir=[]
        mover=[]
        tref=[]
        format=[]
        mover_self='obspy_read'
        tref_self='UTC'
        format_self='miniseed'
        # Note obspy uses a more verbose name for net:sta:chan:loc
        # We change to mspass definition below that uses css3.0 names
        for x in dseis:
            net.append(x.stats['network'])
            sta.append(x.stats['station'])
            chan.append(x.stats['channel'])
            loc.append(x.stats['location'])
            sutc=x.stats['starttime']
            stime.append(sutc.timestamp)
            eutc=x.stats['endtime']
            etime.append(eutc.timestamp)
            samprate.append(x.stats['sampling_rate'])
            delta.append(x.stats['delta'])
            npts.append(x.stats['npts'])
            calib.append(x.stats['calib'])
            dfile.append(dfileself)
            dir.append(dirself)
            tref.append(tref_self)
            format.append(format_self)
            mover.append(mover_self)
        # Now convert the above to a pandas dataframe and return that
        # there may be a better way to do this than using this
        # intermediary dict object, but this should not be a hugely
        # compute or memory entensive operation even for large files
        ddict={'net':net,
               'sta':sta,
               'chan':chan,
               'loc':loc,
               'starttime':stime,
               'endtime':etime,
               'samprate':samprate,
               'delta':delta,
               'npts':npts,
               'calib':calib,
               'dfile':dfile,
               'dir':dir,
               'treftype':tref,
               'format':format,
               'mover':mover
               }
        return pd.DataFrame(ddict)
    except FileNotFoundError as err:
        print('mseed_file_indexer:  invalid file named received')
        print(err)
def dbsave_raw_index(db,pdframe,collection='import_miniseed_ensemble'):
    """
    Database save to db for a panda data frame pdframe.
    This crude version collection name is frozen as import_miniseed_ensemble.  db is
    assumed to be the client root for mongodb or the mspasspy Client
    that is a child of MongoClient.

    :param db:  MongoClient or mspass Client to save data desired
    :param pdframe:  panda data frame to be saved
    :param collection:  collection to which the data in pdframe is to be
      saved.  Default is 'import_miniseed_ensemble'
    """
    col=db[collection]
    # records is a keyword that makes rows of the dataframe docs for mongo
    dtmp=pdframe.to_dict('records')
    col.insert_many(dtmp)
def dbsave_seed_ensemble_file(db,file,gather_type="event",
                keys=None):
    """
    Indexer for SEED files that are already assembled in a
    "gather" meaning the data have some relation through one or more
    keys.   The association may be predefined by input though a
    keys array or left null for later association.   There is a large
    overhead in this function as it has to read the entire seed file
    to acquire the metadata it needs.  This version uses a bigger
    memory bloat than required because it uses obspy's seed reader
    that always eats up the whole file and returns a list of
    Trace object.  A less memory intensive approach would be to
    scan the seed blockettes to assemble the metadata, but
    that would be a future development.

    A KEY POINT about this function is that it ONLY builds an index
    for the data file it is given.  That index is loosely equivalent to
    the css3.0 wfdisc table, but organized completely differently.

    This function writes records into a import_miniseed_ensemble collection.
    The records written are a hierarchy expressed in json (bson) of
    how a Ensemble object is define: i.e. ensemble Metadata
    combined with a container of TimeSeries of Seismogram objects.
    Because SEED data always defines something that directly maps to
    TimeSeries this only works to create an index to build
    TimeSeriesEnsemble objects.

    This function was written to index ensembles that are passive
    array common event (source) gathers.  These are the passive array
    equivalent of a shot gather in reflection processing.   The
    format of the json(bson) document used for the index, however,
    is not limited to that case.  The gather type is defined by a
    metadata key (for this prototype the key is "gather_type").  The
    concept is the gather_type can be used as both a filter to select
    data and as a hint to readers on how to handle the data bundle.

    The gather (ensemble) metadata include a list of dict
    data that define a json/bson document defining the members of
    an ensemble.   Other documentation will be needed to define this
    concept more clearly with figures.

    A design constraint we impose for now is that one file generates
    one document in the import_miniseed_ensemble collection.   This means if
    the data for an ensemble is spread through several files the
    best approach is to convert all the data to TimeSeries objects and
    assemble them with a different algorithm

    A final point about this function is that it dogmatically always produces a
    unique uuid string using the ProcessingHistory newid method.   Be warned
    that that string CANNOT be converted to a MongoDB ObjectId as it is
    generated by a completely different algorithm.  The uuid is posted with
    the key "seed_file_id" to provide a unique name.  That id is used
    by the reader in this module to define a unique origin for a channel of
    data.  We note that usage is problematic for finding a particular
    waveform linked to a seed_file_id because the index storeds that
    attribute in subdocuments for each ensemble.

    :param db:  MongoDB database pointer - may also be a mspass Database
      class
    :param file:  seed file containing the data to be indexed.
    :param gather_type: character string defining a name that defines
      a particular ensemble type.  Default is "event", which is the
      only currently supported format.  (others keyword will cause an
      error to be thrown)  Anticipated alternatives are:  "common_receiver"
      or "station", "image_point", and "time_window".
     :return:  ObjectId of the document inserted that is the index for
      the file processed.
    """

    try:
        his=ProcessingHistory()  # used only to create uuids
        dbh=db['import_miniseed_ensemble']
        pr=Path(file)
        fullpath=pr.absolute()
        [dirself,dfileself]=os.path.split(fullpath)
        dseis=read(file,format='mseed')
        # This holds the ensemble metatdata
        ensemblemd={'dir':dirself}
        ensemblemd['dfile']=dfileself
        ensemblemd['format']='mseed'
        # this is a placeholder not really necessary for seed data \
        # as seed data by definition yield TimeSeries type data although
        # not necessarily seismic data (e.g. MT data are distributed as mseed
        ensemblemd['member_type']='TimeSeries'
        ensemblemd['mover']='obspy_seed_ensemble_reader'
        members=[]   # this list will contain one dict for each dseis Trace
        # we want to put time range of the data into enemblemd - we use these for that
        stimes=[]
        etimes=[]
        for d in dseis:
            mddict={}
            mddict['net']=d.stats['network']
            mddict['sta']=d.stats['station']
            mddict['chan']=d.stats['channel']
            mddict['loc']=d.stats['location']
            st=d.stats['starttime']
            et=d.stats['endtime']
            mddict['starttime']=st.timestamp
            mddict['endtime']=et.timestamp
            stimes.append(st.timestamp)
            etimes.append(et.timestamp)
            mddict['sampling_rate']=d.stats['sampling_rate']
            mddict['delta']=d.stats['delta']
            mddict['npts']=d.stats['npts']
            mddict['calib']=d.stats['calib']
            # this key name could change
            mddict['seed_file_id']=his.newid()
            members.append(mddict)
        ensemblemd['members'] = members
        tmin=np.median(stimes)
        tmax=np.median(etimes)
        ensemblemd['starttime']=tmin
        ensemblemd['endtime']=tmax
        result=dbh.insert_one(ensemblemd)
        return result.inserted_id
    except:
        print('something threw an exception - this needs detailed handlers')
def _load_md(rec,keys):
    """
    Helper for load ensemble.   Extracts metadata defined by keys list and
    posts to a Metadata container that is returned.
    """
    # do this stupid for now without error handlers
    md=Metadata()
    for k in keys:
        x=rec[k]
        md.put(k,x)
    return md
def load_one_ensemble(doc,
                  create_history=False,
                  jobname='Default job',
                  jobid='99999',
                  algid='99999',
                  ensemble_mdkeys=[],  # default is to load nothing for ensemble
		  apply_calib=False,
                  verbose=False):
    """
    This function can be used to load a full ensemble indexed in the
    collection import_miniseed_ensemble.  It uses a large memory model
    that eat up the entire file using obspy's miniseed reader.   It contains
    some relics of early ideas of potentially having the function
    utilize the history mechanism.  Those may not work, but were retained.

    :param doc: is one record in the import_miniseed_ensemble collection
    :param create_history:  if true each member of the ensemble will be
      defined in the history chain as an origin and jobname and jobid will be
      be used to construct the ProcessingHistory object.
    :param jobname: as used in ProcessingHistory (default "Default job")
    :param jobid: as used in processingHistory
    :param algid: as used in processingHistory
    :param ensemble_mdkeys:  list of keys to copy from first member to ensemble
       Metadata (no type checking is done)
    :param apply_calib:  if True tells obspy's reader to apply the calibration
      factor to convert the data to ground motion units.  Default is false.
    :param verbose:  write informational messages while processing
    """
    try:
        ensemblemd=Metadata()
        if create_history:
            his=ProcessingHistory(jobname,jobid)
        form=doc['format']
        mover=doc['mover']
        if form!='mseed':
                raise MsPASSError("Cannot handle this ensemble - ensemble format="+form+"\nCan only be mseed for this reader")
        if mover!='obspy_seed_ensemble_reader':
                raise MsPASSError("Cannot handle this ensemble - ensemble mover parameter="+mover+" which is not supported")
        dir=doc['dir']
        dfile=doc['dfile']
        fname=dir+"/"+dfile
        # Note this algorithm actually should work with any format
        # supported by obspy's read function - should generalize it for release
        dseis=read(fname,format='mseed',apply_calib=apply_calib)
        if len(ensemble_mdkeys)>0:
            ensemblemd=_load_md(doc,ensemble_mdkeys)
        else:
            # default is to load everything != members
            members_key='members'
            for k in doc:
                if k!=members_key:
                    x=doc[k]
                    ensemblemd[k]=x
        # There is a Stream2TimeSeriesEnsemble function
        # but we don't use it here because we need some functionality
        # not found in that simple function
        nseis=len(dseis)
        result=TimeSeriesEnsemble(ensemblemd,nseis)
        # Secondary files get handled almost the same except for
        # a warning.   The warning message (hopefully) explains the
        # problem but our documentation must warn about his if this
        # prototype algorithm becomes the release version
        count=0
        for d in dseis:
            #print('debug - working on data object number',count)
            count+=1
            dts=Trace2TimeSeries(d)
            if create_history:
                dts.load_history(his)  # This should just define jobname and jobid
                seedid=d['seed_file_id']
                dts.set_as_origin('load_ensemble',algid,seedid,
                          AtomicType.TIMESERIES,True)
            result.member.append(dts)
        return result
    except:
        print('something threw an exception - needs more complete error handlers')

def link_source_collection(db,dt=10.0,prefer_evid=False,verbose=False):
    """
    This prototype function uses a not at all generic method to link data
    indexed in a import_miniseed_ensemble collection to source data assumed stored
    in the source collection.   The algorithm is appropriate ONLY if the
    data are downloaded by obspy with a time window defined by a start time
    equal to the origin time of the event.   We use a generic test to check
    if the median ensemble start time (pulled from import_miniseed_ensemble record)
    is within +-dt of any origin time in source.   If found we extract the
    source_id of the maching event document and then update the record in
    import_miniseed_ensemble being handled.  Tha process is repeated for each
    document in the import_miniseed_ensemble collection.

    To handle css3.0 set the prefer_evid boolean True (default is False).
    When used the program will use a document with an evid set as a match
    if it finds multiple source matches.  This feature is used to match
    data with arrivals exported from a css3.0 database where the source
    data is embedded in the exported database view table.

    :param db:  MongoDB top level handle or a mspasspy.Database object to
      be accessed - this function is  pure database function talking to
      this db
    :param dt:  time range of match (search is + to - this value)
    :param prefer_evid:  As noted above if True select the source doc with
      evid set when there are multiple matches.
    :param verbose:  when true output will be more verbose.
    """
    dbwf=db['import_miniseed_ensemble']
    dbsource=db['source']
    try:
        ensrec=dbwf.find({})
        for ens in ensrec:
            #print('debug - at top of ensemble loop')
            t=ens['starttime']
            tlow=t-dt
            thigh=t+dt
            query={'time':{'$gte':tlow,'$lte':thigh}}
            matchid=ens['_id']
            ens_match_arg={'_id' : matchid}
            #print('debug - query:',query)
            #print('range between ',UTCDateTime(tlow),'->',UTCDateTime(thigh))
            n=dbsource.count_documents(query)
            #print('debug - found ',n,' documents')
            if n==0:
                if verbose:
                    print('link_source_collection:  no match in source for time=',
                        UTCDateTime(t))
                    print("This enemble cannot be processed")
            elif n==1:
                srcrec=dbsource.find_one(query)
                #print('debug - query returned:',srcrec)
                #only in this situation will we update the document
                source_id=srcrec['source_id']
                #print('debug - matchid and source_id=',matchid,source_id)
                if prefer_evid:
                    if 'evid' in srcrec:
                        evid=srcrec['evid']
                        update_record={'$set':{'source_id' :source_id,'evid':evid}}
                        if verbose:
                            print('Found evid=',evid,' for ensembled with start time=',UTCDateTime(t))
                    else:
                        print('link_source_collection(WARNING): unique match for source at time=',
                              UTCDateTime(t), ' does not have evid set but function was called with prefer_evid true')
                        update_record={'$set':{'source_id' :source_id}}
                else:
                    update_record={'$set':{'source_id' :source_id}}
                dbwf.update_one(ens_match_arg,update_record)
            else:
                cursor=dbsource.find(query)
                evid=-1   # Negative used as a test for search failure
                for srcrec in cursor:
                    # this will be set to last record if evid search fails but
                    # will match evid field if there is a match because of the break
                    # note this logic sets source_id to the last record found
                    # when prefer_evid is false.  That is inefficient but
                    # we don't expect long match lists
                    source_id=srcrec['source_id']
                    if prefer_evid and ('evid' in srcrec):
                        evid=srcrec['evid']
                        matchid=ens['_id']
                        break
                if evid>0:
                    update_record={'$set':{'source_id' :source_id,'evid':evid}}
                    if verbose:
                            print('Found evid=',evid,' for ensembled with start time=',UTCDateTime(t))
                else:
                    update_record={'$set':{'source_id' :source_id}}
                    if verbose:
                        print('Found ',n,' matches in source collection for ensemble start time=',
                              UTCDateTime(t))
                        print('Linking to document with source_id=',source_id)
                dbwf.update_one(ens_match_arg,update_record)
    except Exception as err:
        raise MsPASSError('Something threw an unexpected exception',
            ErrorSeverity.Invalid) from err

def load_source_data_by_id(db,mspass_object):
    """
    Prototype function to load source data to any MsPASS data object
    based on the normalization key.  That keys is frozen in this version
    as "source_id" but may be changed to force constraints by the mspasspy
    schema classes.

    Handling of Ensembles and atomic objects are different conceptually but
    in fact do exactly the same thing.   That is, in all cases the
    algorithm queries the input object for the key "source_id".  If that
    fails it returns an error.  Otherwise, it finds the associated document
    in the source collection.   It then posts a frozen set of metadata to
    mspass_object.   If that is an ensemble it is posted to the ensemble
    metadata area.  If it is an atomic object it gets posted to the atomic object's
    metadata area.
    """
    dbsource=db.source
    try:
        if not 'source_id' in mspass_object:
            raise MsPASSError('load_source_data_by_id',
                              'required attribute source_id not in ensemble metadata',
                              ErrorSeverity.Invalid)
        source_id=mspass_object['source_id']
        # The way we currently do this source_id eithe rmaches one documentn in
        # source or none.  Hence, we can jus use a find_one query
        srcrec=dbsource.find_one({'_id' : source_id})
        # note find_one returns a None if there is no match.  Point this out
        # because if we used find we would use test size of return and use
        # next to get the data. Find_one return is easier but depends upon
        # the uniqueness assumption
        if srcrec==None:
            raise MsPASSError("load_source_data",
                "no match found in source collection for source_id="+source_id,
                ErrorSeverity.Invalid)
        else:
            mspass_object['source_lat']=srcrec['lat']
            mspass_object['source_lon']=srcrec['lon']
            mspass_object['source_depth']=srcrec['depth']
            mspass_object['source_time']=srcrec['time']
        return mspass_object
    except:
        print("something threw an unexpected exception")
def load_hypocenter_data_by_time(db=None,
                                ens=None,
                                dbtime_key='time',
                                mdtime_key='time_P',
                                event_id_key='evid',
                                phase='P',
                                model='iasp91',
                                dt=10.0,
                                t0_definition='origin_time',
                                t0_offset=0.0,
                                kill_null=True):
    """
    Loads hypocenter data (space time coordinates) into an ensemble using
    an arrival time matching algorithm.  This is a generalization of earlier
    prototypes with the objective of a single interface to a common concept -
    that is, matching arrival documents to ensemble data using timing
    based on travel times.

    We frequently guide processing by the time of one or more seismic phases.
    Those times can be either measured times done by a human or an automated
    system or theoretical times from an earth model.   This function should
    work with either approach provided some earlier function created an
    arrival document that can be used for matching.  The matching algorithm uses
    three keys:  an exact match for net, an exact match for sta, and a time
    range travel time association.

    The algorithm is not a general associator.  It assumes we have access to
    an arrival collection that has been previously associated.  For those
    familiar with CSS3.0 the type example is the join of event->origin->assoc->arrival
    grouped by evid or orid.  We use a staged match to the ensemble to
    reduce compute time.  That is, we first run a db find on arrival to select
    only arrival documents within the time span of the ensemble with the
    defined arrival name key.  From each matching arrival we compute a theoretical
    origin time using obspy's taup calculator and a specified model and
    hypocenter coordinates in the arrival document that we assume was loaded
    previously from a css3.0 database (i.e. the event->origin->assoc->arrival
    view).   We then select and load source coordinates for the closest
    origin time match in the source collection.   This is much simpler than
    the general phase association (determine source coordinates from a random
    bad of arrival times) but still has one complication - if multiple
    events have arrivals within the time span of the ensemble the simple match
    described above is ambiguous.   We resolve that with a switch defined by
    the argument t0_definition.   Currently there are two options defining
    the only two options I know of for time selection of downloaded segments.
    (1) if set to 'origin_time' we assume member t0 values are near the origin
    time of the event.  (2) if set to 'phase_time' we assume the member t0
    values are relative to the phase used as a reference.  In both cases an
    optional t0_offset can be specified to offset each start time by a constant.
    The sign of the shift is to compare the data start time to the computed
    time MINUS the specified offset.  (e.g. if the reference is a P phase time
    and we expect the data to have been windowed with a start time 100 s before
    the theoretical P time, the offset would be 100.0)   Note if arrival windowing
    is selected the source information in arrival will not be referenced but
    that data is required if using origin time matching.  In any case when
    multiple sources are found to match the ensemble the one with the smallest
    rms misfit for the windowing is chosen.  A warning message is always
    posted in that situation.

    By default any ensemble members without a match in arrival will be
    killed. Note we use the kill method which only marks the data dead but
    does not clear the contents. Hence, on exit most ensembles will have
    at least some members marked dead.  The function returns the number of
    members set.  The caller should test and complain if there are no matches.
    """
    base_error_message='load_hypocenter_data_by_time:  '
    if db==None:
        raise MsPASSError(base_error_message+'Missing required argument db=MongoDB Database handle',
                    ErrorSeverity.Fatal)
    elif ens==None:
        raise MsPASSError(base_error_message+'Missing required argument ens=mspass Ensemble data',
                    ErrorSeverity.Fatal)
    # First we need to query the arrival table to find all arrivals within
    # the time range of this ensemble
    try:
        dbarrival=db.arrival
        stime=ens['startttime']
        etime=ens['endtime']
        query={{dbtime_key:{'$gte':stime,'$lte':etime}}}
        narr=dbarrival.count_documents(query)
        if narr == 0:
            print(base_error_message,'No arrivals found in data time range:')
            print(UTCDateTime(stime),' to ',UTCDateTime(etime))
            if kill_null:
                for d in ens.member:
                    d.kill()
            return 0
        else:
            # This else block isn't essential, but makes the logic clearer
            # first scan the data for unique events.  For now use an evid
            # test.  This could be generalized to coordinates
            arrivals=dbarrival.find(query)
            evids=dict()
            for doc in arrivals:
                evnow=doc[event_id_key]
                lat=doc['source.lat']
                lon=doc['source.lon']
                depth=doc['source.depth']
                otime=doc['source.time']
                evids[evnow]=[lat,lon,depth,otime]
            if len(evids)>1:
                # land if picks from multiple events are inside the data window
                #INCOMPLETE - will probably drop this function
                for k in evids.keys():
                    print(k)
            # Above block always sets lat,lon,depth, and otime to the
            # selected hypocenter data.  IMPORTANT is for the unambiguous
            # case where len(evids) is one we depend on the python property
            # that lat,lon,depth, and otime are set because they have not
            # gone out of scope
    except MsPASSError as err:
        print(err)
        raise err

def load_site_data(db,ens):
    """
    Loads site data into ens.  Similar to load_source_data but uses a diffrent
    match:  net,sta, time matching startdate->enddate.   Mark members dead and
    post an elog message if the site coordinates are not found.
    """
    dbsite=db.site
    try:

        for d in ens.member:
            if d.dead():
                continue
            t0=d['starttime']
            net=d['net']
            sta=d['sta']
            query={
                'net' : {'$eq' : net},
                'sta' : {'$eq' : sta},
                'starttime' : {'$lt' : t0},
                'endtime' : {'$gt' : t0}
                }
            n=dbsite.count_documents(query)
            if n==0:
                d.kill()
                d.elog.log_error('load_site_data',
                 'no match found in site collection for net='+net+' sta='+sta+' for this event',
                 ErrorSeverity.Invalid)
            else:
                siterec=dbsite.find_one(query)
                d['site_lat']=siterec['lat']
                d['site_lon']=siterec['lon']
                d['site_elev']=siterec['elev']
                d['site_id']=siterec['site_id']
                if n>1:
                    message = "Multiple ({n}) matches found for net={net} and sta={sta} with reference time {t0}".format(n=n,net=net,sta=sta,t0=t0)
                    d.elog.log_error('load_site_data',message,ErrorSeverity.Complaint)
        return ens
    except Exception as err:
        raise MsPASSError('Something threw an unexpected exception',
            ErrorSeverity.Invalid) from err
def load_channel_data(db,ens):
    """
    Loads channel data into ens.  Similar to load_source_data but uses a diffrent
    match:  net,sta,loc,time matching startdate->enddate.   Mark members dead and
    post an elog message if required metadata are not found.
    """
    dbchannel=db.channel
    try:
        for d in ens.member:
            if d.dead():
                continue
            t0=d['starttime']
            # this is a sanity check to avoid throwing exceptions
            if( d.is_defined('net')
              and d.is_defined('sta')
              and d.is_defined('loc')
              and d.is_defined('chan')):
                net=d['net']
                sta=d['sta']
                chan=d['chan']
                loc=d['loc']
                query={
                        'net' : {'$eq' : net},
                        'sta' : {'$eq' : sta},
                        'chan' : {'$eq' : chan},
                        'loc' : {'$eq' : loc},
                        'starttime' : {'$lt' : t0},
                        'endtime' : {'$gt' : t0}
                }
                n=dbchannel.count_documents(query)
                if n==0:
                    d.kill()
                    d.elog.log_error('load_channel_data',
                        'no match found in channel collection for net='+net+' sta='+sta+" chan="+chan+" loc="+loc+' for this event',
                            ErrorSeverity.Invalid)
                    continue
                if n==1:
                    chanrec=dbchannel.find_one(query)
                else:
                # In this case we just complain - and keep use the first record
                # that is what find_one returns.  We use the count to make
                # the eror message cleaer
                    chanrec=dbchannel.find_one(query)
                    message = "Multiple ({n}) matches found for net={net} and sta={sta} with reference time {t0}".format(n=n,net=net,sta=sta,t0=t0)
                    d.elog.log_error('load_site_data',message,ErrorSeverity.Complaint)
                d['site_lat']=chanrec['lat']
                d['site_lon']=chanrec['lon']
                d['site_elev']=chanrec['elev']
                d['vang']=chanrec['vang']
                d['hang']=chanrec['hang']
                d['site_id']=chanrec['_id']

        return ens
    except Exception as err:
        raise MsPASSError('Something threw an unexpected exception',
            ErrorSeverity.Invalid) from err

def load_arrivals_by_id(db,tsens,
        phase='P',
        required_key_map={'phase':'phase','time':'arrival_time'},
        optional_key_map={'iphase':'iphase','deltim':'deltim'},
        verbose=False):
    """
    Special prototype function to load arrival times in arrival collection
    to TimeSeries data in a TimeSeriesEnsemble. Match is a fixed query
    that uses source_id tht is assumed set for the ensemble AND previously
    defined for all arrival documents to be matched.  The correct record
    in arrival is found by the combination of matching source_id, net, and sta.
    This algorithm does a lot of queries to the arrival collection so an
    index on net and sta is essential.   Probably would be good to add source_id
    to the index as well.

    :param db:  MongoDB database handle or mspsspy.Database object
    :param tsens:  TimeSeriesEnsemble to be processed
    :param required_key_map:  a  dict of pairs defining how keys in
      arrival should map to metadata keys of ensemble members.  The key of
      each entry in the dict is used to fetch the required attribute from
      the MongoDB doc found in a query.  The value of that attribute is
      posted to each member TimeSeries object with the value of the key
      value pair associated with that entry.  e.g. for the default
      we fetch the data from the MongoDB document with the key time and
      write the value retrieved to Metadata with the key arrival.time.
      If a required key is not found in the arrival document a MsPASSError
      will be logged  and that member will be marked dead.  Users should
      always test for an empty ensemble after running this function.
    :param optional_key_map:  similar to required_key_map but missing attributes
      only geneate a complaint message and the data will be left live
    :param verbose:  print some messages otherwise only posted to elog

    :return: count of number of live members in the ensemble at completion
    """
    dbarrival=db.arrival
    algorithm='load_arrivals_by_id'
    if 'source_id' in tsens:
        source_id=tsens['source_id']
        for d in tsens.member:
            if d.dead():
                continue
            if ('net' in d) and ('sta' in d):
                net=d['net']
                sta=d['sta']
                query={
                        'source_id' : source_id,
                        'net' : net,
                        'sta' : sta,
                        'phase' : phase
                }
                n=dbarrival.count_documents(query)
                #print('debug  query=',query,' yield n=',n)
                if n==0:
                    d.elog.log_error(algorithm,
                      "No matching arrival for source_id="+source_id+" and net:sta ="+net+':'+sta,
                      ErrorSeverity.Invalid)
                    if verbose:
                        print("No matching arrival for source_id="+source_id+" and net:sta ="+net+':'+sta)
                    d.kill()
                else:
                    cursor=dbarrival.find(query)
                    if n==1:
                        rec=cursor.next()
                    elif n>1:
                        d.elog.log_error(algorithm,
                          "Multiple documents match source_id="+source_id+" and net:sta ="+net+':'+sta+"  Using first found",
                          ErrorSeverity.Complaint)
                        if verbose:
                            print('debug:  multiple docs match - printing full documents of all matches.  Will use first')
                            for rec in cursor:
                                print(rec)
                            cursor.rewind()
                        rec=cursor.next()
                    for k in required_key_map:
                        if k in rec:
                            x=rec[k]
                            dkey=required_key_map[k]
                            d[dkey]=x
                        else:
                            d.elog.log_error(algorithm,
                                "Required attribute with key="+k+" not found in matching arrival document - data killed",
                               ErrorSeverity.Invalid)
                            d.kill()
                            if verbose:
                                print("Required attribute with key="+k+" not found in matching arrival document - data killed")
                    for k in optional_key_map:
                        if k in rec:
                            x=rec[k]
                            dkey=optional_key_map[k]
                            d[dkey]=x
                        else:
                            d.elog.log_error(algorithm,
                                "Optional attribute with key="+k+" was not found in matching arrival document and cannot be loaded",
                                ErrorSeverity.Complaint)
                            if verbose:
                                print("Optional attribute with key="+k+" was not found in matching arrival document and cannot be loaded")

    else:
        message="Enemble metadata does not contain required source_id attribute - killing all data in this ensemble"
        if verbose:
            print(message)
        for d in tsens.member:
            d.elog.log_error('load_arrivals_by_id',message,ErrorSeverity.Invalid)
            d.kill()
    nlive=0
    for d in tsens.member:
        if d.live:
            nlive+=1
    return nlive
def erase_seed_metadata(d,keys=['mseed','_format']):
    """
    Erase a set of debris obspy's reader adds to stats array that get copied to data.  We
    don't need that debris once data are converted to MsPASS data objects and stored in
    MongoDB.  Use this function when importing seed data with obspy's reader to
    reduce junk in the database.

    :param d: is the data object to be cleaned (assumed only to be a child of Metadata so
      erase will work.
    :param keys:  list of keys to clear.  Default should be all that is needed for
      standard use.
    """
    for mem in d.member:
        for k in keys:
            mem.erase(k)
