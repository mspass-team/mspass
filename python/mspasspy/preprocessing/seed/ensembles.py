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
from mspasspy.io.converter import Trace2TimeSeries

def obspy_mseed_file_indexer(file):
    """
    Use obspy's miniseed reader to eat up a (potentially large) file and
    build an index as a table (returned) of data that can be written to
    a database.   Obspy's reader was written to accept the abomination of
    miniseed with random packets scattered through the file (or so it seems,
    since they have no foff concept in their reader).  Hence, what this reader
    does is make a table entry for each net:sta:chan:loc trace object their
    reader returns.   It does this with panda dataframes to build the
    table.
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
def dbsave_raw_index(db,pdframe):
    """
    Prototype database save to db for a panda data frame pdframe.
    This crude version collection name is frozen as raw.  db is
    assumed to be the client root for mongodb
    """
    col=db['seedwf']
    # records is a keyword that makes rows of the dataframe docs for mongo
    dtmp=pdframe.to_dict('records')
    col.insert_many(dtmp)
def dbsave_seed_ensemble_file(db,file,gather_type="event",
                keys=None):
    """
    Prototype reader for SEED files that are already assembled in a
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

    This function writes records into a seed_data.enemble collection.
    Be warned that the "." in the name is a common convention in
    MongoDB databases but really only defines a unique name and
    does not do any hierarchy of collections as the name might
    suggest (O'Reilly book on MongoDB by Shannon et al. 2019).
    It is a useful tag here, however, to distinguish it from seed_data
    that contains an index to individual channels that can be
    used to construct TimeSeries (or Trace) objects.

    The records written are a hierarchy expressed in json (bson) of
    how a Ensemble object is define: i.e. ensemble Metadata
    combined with a container of TimeSeries of Seismogram objects.
    Because SEED data always defines something that directly maps to
    TimeSeries this only works to create an index to build
    TimeSeriesEnsemble objects.

    This prototype was written to index ensembles that are passive
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
    on document in the seed_data.ensemble collection.   This means if
    the data for an ensemble is spread through several files it would
    have to be constructed in pieces.  That will require implementing
    a function that merges ensemble data.  That model should make this
    more generic as an end member is an ensembled created by merging
    files with one TimeSeries per file.

    :param db:  MongoDB database pointer - may also be a mspass Database
      class
    :param file:  seed file containing the data to be indexed.
    :param gather_type: character string defining a name that defines
      a particular ensemble type.  Default is "event", which is the
      only currently supported format.  (others keyword will cause an
      error to be thrown)  Anticipated alternatives are:  "common_receiver"
      or "station", "image_point", and "time_window".
    """

    try:
        dbh=db['seed_data.ensemble']
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
            mddict['sta']=d.stats['channel']
            mddict['chan']=d.stats['location']
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
def load_md(rec,keys):
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
def load_ensemble(db,idkey=None,idval=None,
                  query=None,
                  collection="seed_data.ensemble",
                  create_history=False,
                  jobname='Default job',
                  jobid='99999',
                  algid='99999',
                  ensemble_mdkeys=[],  # default is to load nothing for ensemble
                  verbose=False):
    """
    This is a prototype.  Ultimately this should probably be a method
    in the Database handle.  For now will do this as a function to
    see how it works.

    This example illustrates an issue in history we need to improve.
    At this point jobname and jobid are best set for a reader like
    this function, but what happens if jobname and jobid change
    within a workflow is not clear - it could break the history chain.
    Just putting this here now as a reminder to consider that later.

    prototype uses a idkey and value for a unique set of data or a
    query dict passed directly to mongo.

    This help string MUST discuss the problem of assuming ensemble md
    are consistent if reading multiple files.
    """
    try:
        dbh=db[collection]
        #This function needs some sanity check here eventually - skip
        #for now while prototyping
        dbquery={}
        if idkey!=None:
            if idval==None:
                raise MsPASSError("Illegal argument combination - idval must be specified idkey is set")
            else:
                dbquery[idkey]=idval
        elif query!=None:
            dbquery=query
        n=dbh.count_documents(dbquery)
        if n<=0:
            raise MsPASSError("query failed - no data match query")
        elif n>1 and verbose:
            print("Warning:  ensemble with the following query has ",n," entries")
            print("Normal expectation is a unique match - will attempt to merge data from multiple files")
            print(dbquery)
        curs=dbh.find(dbquery)
        nprocessed=0
        ensemblemd=Metadata()
        for rec in curs:
            if create_history:
                his=ProcessingHistory(jobname,jobid)
                # use the objectid string as the uuid for the origin definition
                # of all data in this ensemble
                history_uuid=str(rec['_id'])
                # all TimeSeries members of this ensemble will get a copy
                # of this top level history definition.  Correct since they
                # all come from a single file defined by the ObjecID
                his.set_as_origin('load_ensemble',algid,history_uuid,
                      AtomicType.TIMESERIES,True)
            form=rec['format']
            mover=rec['mover']
            if form!='mseed':
                raise MsPASSError("Cannot handle this ensemble - ensemble format="+form+"\nCan only be mseed for this reader")
            if mover!='obspy_seed_ensemble_reader':
                raise MsPASSError("Cannot handle this ensemble - ensemble mover parameter="+mover+" which is not supported")

            dir=rec['dir']
            dfile=rec['dfile']
            fname=dir+"/"+dfile
            # Note this algorithm actually should work with any format
            # supported by obspy's read function - should generalize it for release
            dseis=read(fname,format='mseed')
            if nprocessed==0 :
                if len(ensemble_mdkeys)>0:
                    ensemblemd=load_md(rec,ensemble_mdkeys)
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
                    dts.load_history(his)
                result.member.append(dts)
            nprocessed+=1
        if nprocessed>1 and len(ensemble_mdkeys)>0:
            print('load_ensemble (WARNING):  ensemble was loaded from ',nprocessed,' files')
            print('The algorithm assumes ensemble metadata are constant for each file and uses data from the first document it finds')
            print('Using these values:  ',ensemblemd)
        return result
    except:
        print('something threw an exception - needs more complete error handlers')

def link_source_collection(db,dt=10.0,verbose=False):
    """
    This prototype function uses a not at all generic method to link data
    indexed in a seed_data.ensmble collection to source data assumed stored
    in the source collection.   The algorithm is appropriate ONLY if the
    data are downloaded by obspy with at least one signal have a start time
    equal to the origin time of th event.   We use a generic test to check
    if the minimum ensemble start time (pulled from seed_data.ensemble record)
    is within +-dt of any origin time in source.   If found we extract the
    source_id of the maching event document and then update the record in
    seed_data.ensemble being handled.  Tha process is repeated for each
    document in the seed_data.ensemble collection.
    """
    dbwf=db['seed_data.ensemble']
    dbsource=db['source']
    try:
        ensrec=dbwf.find({})
        for ens in ensrec:
            #print('debug - at top of ensemble loop')
            t=ens['starttime']
            tlow=t-dt
            thigh=t+dt
            query={'source_time':{'$gte':tlow,'$lte':thigh}}
            #print('debug - query:',query)
            n=dbsource.count_documents(query)
            print('debug - found ',n,' documents')
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
                matchid=ens['_id']
                print('debug - matchid and source_id=',matchid,source_id)
                dbwf.update_one({'_id' : matchid},{'$set':{'source_id' :source_id}})
            else:
                print('link_source_collection (WARNING):  ambiguous time=',
                   UTCDateTime(t))
                print('source collection has the following matches with ',dt,
                   ' seconds of that time:')
                for src in srcrec:
                    lat=src['source_lat']
                    lon=src['source_lon']
                    depth=src['source_depth']
                    otime=src['source_time']
                    print(lat,lon,depth,UTCDateTime(otime))
                print('Cannot set source_id in seed_data.enemble collection document')
    except Exception as err:
        print('something threw an unexpected exception - this needs to be cleaned up')
        print(err)

def load_source_data(db,ens):
    """
    Prototype function to load source data from the source collection.
    We assume source_id is set in the ensemble's metadata.
    We use that id to query the source collection.   If a match is found
    source coordinates are loaded in all members of ens.   If a match is not
    found print an error message and do nothing.
    """
    dbsource=db.source
    try:
        source_id=ens['source_id']
        # The way we currently do this source_id eithe rmaches one documentn in
        # source or none.  Hence, we can jus use a find_one query
        srcrec=dbsource.find_one({'source_id' : source_id})
        # note find_one returns a None if there is no match.  Point this out
        # because if we used find we would use test size of return and use
        # next to get the data. Find_one return is easier but depends upon
        # the uniqueness assumption
        if srcrec==None:
            raise MsPASSError("load_source_data:  no match found in source collection for source_id="+source_id,
                ErrorSeverity.Invalid)
        else:
            ens['source_lat']=srcrec['source_lat']
            ens['source_lon']=srcrec['source_lon']
            ens['source_depth']=srcrec['source_depth']
            ens['source_time']=srcrec['source_time']
            ens['source_id']=source_id
        return ens
    except:
        print("something threw an unexpected excepion")
def load_site_data(db,ens):
    """
    Loads site data into ens.  Similar to load_source_data but uses a diffrent
    match:  net,sta, time matching startdate->enddate.   Mark members dead and
    post an elog message if the site coordinates are not found.
    """
    dbsite=db.site
    try:
        # We assume this got set on initialization - only works for sure
        # for seed ensemble reader linked to this prototype code
        t0=ens['starttime']
        for d in ens.members:
            if d.dead:
                continue
            net=d['net']
            sta=d['sta']
            query={
                'net' : {'$eq' : net},
                'sta' : {'$eq' : sta},
                'starttime' : {'$lt' : t0},
                'endtime' : {'gt' : t0}
                }
            n=dbsite.count_documents(query)
            if n==0:
                d.kill()
                d.elog.log_error('load_site_data',
                 'no match found in site collection for net='+net+' sta='+sta+' for this event',
                 ErrorSeverity.Invalid)
            if n==1:
                siterec=dbsite.find_one(query)
            else:
                # In this case we just complain - and keep use the first record
                # that is what find_one returns.  We use the count to make
                # the eror message cleaer
                siterec=dbsite.find_one(query)
                message = ('Muliple (%d) matches found for net %s sta %s with reference time %f'
                   % [n,net,sta,t0])
                d.elog.log_error('load_site_data',message,ErrorSeverity.Complain)
            d['site_lat']=siterec['site_lat']
            d['site_lon']=siterec['site_lon']
            d['site_depth']=siterec['site_depth']
            d['site_time']=siterec['site_time']
            d['site_id']=siterec['site_id']

        return ens
    except:
        # this nees to have exlicit handlers for a stable release version
        print('something threw an unexpected exception')
def load_channel_data(db,ens):
    """
    Loads channel data into ens.  Similar to load_source_data but uses a diffrent
    match:  net,sta,loc,time matching startdate->enddate.   Mark members dead and
    post an elog message if required metadata are not found.
    """
    dbchannel=db.channel
    try:
        # We assume this got set on initialization - only works for sure
        # for seed ensemble reader linked to this prototype code
        t0=ens['starttime']
        for d in ens.members:
            if d.dead:
                continue

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
                        'endtime' : {'gt' : t0}
                }
                n=dbchannel.count_documents(query)
                if n==0:
                    d.kill()
                    d.elog.log_error('load_channel_data',
                        'no match found in channel collection for net='+net+' sta='+sta+" chan="+chan+" loc="+loc+' for this event',
                            ErrorSeverity.Invalid)
                if n==1:
                    chanrec=dbchannel.find_one(query)
                else:
                # In this case we just complain - and keep use the first record
                # that is what find_one returns.  We use the count to make
                # the eror message cleaer
                    chanrec=dbchannel.find_one(query)
                    message = ('Muliple (%d) matches found for net=%s sta=% chan=%s loc=%s with reference time %s'
                   % [n,net,sta,chan,loc,t0])
                d.elog.log_error('load_site_data',message,ErrorSeverity.Complain)
                d['site_lat']=chanrec['site_lat']
                d['site_lon']=chanrec['site_lon']
                d['site_depth']=chanrec['site_depth']
                d['site_time']=chanrec['site_time']
                d['site_id']=chanrec['site_id']

        return ens
    except:
        # this nees to have exlicit handlers for a stable release version
        print('something threw an unexpected exception')

def load_arrivals(db,tsens,
        required_keys=['phase','arrival_time'],
        optional_keys=['iphase','deltim'],
        clear_undefined=True):
    """
    Special prototype function to load arrival times in arrival collection
    to TimeSeries data in a TimeSeriesEnsemble.   Match is a frozen query
    that works by matching source_id and net:sta in arrival with
    Metadata contents.  This means, of course, that source_id must be
    defined for the ensemble or the process will fail.  In that case, a
    message is posted on all members of ens before returning.
    """
    dbarrival=db.arrival
    algorithm='load_arrivals'
    if 'source_id' in tsens:
        source_id=tsens['source_id']
        for d in tsens.members:
            if d.dead:
                continue
            site_id=d['site_id']
            query={
              {'source_id' : source_id},
              {'site_id' : site_id}
            }
            n=dbarrival.count_documents(query)
            if n==0:
                d.elog.log_error(algorithm,
                    "Not matching arrival for source_id="+source_id+" and site_id="+site_id,
                    ErrorSeverity.Invalid)
                d.kill()
            else:
                rec=dbarrival.find_one(query)
            if n>1:
                d.elog.log_error(algorithm,
                    "Multiple documents match source_id="+source_id+" and site_id="+site_id+"  Using first found",
                    ErrorSeverity.Complaint)
            for k in required_keys:
                if k in rec:
                    x=rec[k]
                    d[k]=x
                else:
                    d.elog.log_error(algorithm,
                      "Required attribute with key="+k+" not found in matching arrival document - data killed",
                    ErrorSeverity.Invalid)
                d.kill()
            for k in optional_keys:
                # Here we silently skip any keys not defined.  Maybe should
                # post a warning to elog
                if k in rec:
                    x=rec[k]
                    d[k]=x

    else:
        message=""
        for d in tsens.members:
            d.elog.log_error('load_arrivals',message,ErrorSeverity.Invalid)
    return tsens
