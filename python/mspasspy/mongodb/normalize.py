import pymongo
from math import sqrt
from math import cos
from math import pi

def unique_receivers(wfcol,firstid,tolerance=0.0001,ztol=10.0):
    """
    Works through the return of a find with sort of the (potentially large) 
    list of dict containers of docs to be scanned.   Returns a subset of 
    input that have unique positions defined by the tolerance value.
    tolerance is a simple distance as hypot of the differences in degrees.
    ztol is a tolerance in elevation (default size assumes units of km)

    Args: 
      wfcol handle pointing at collection to be queried, sorted, and used for 
        as input the algorithm
      firstid is the initial site_id to use in this pass.  This should
        normally be created by scanning a previously created site collection
        but could be some arbitrarily large number (caution if you go there)
      tolerance - horizontal tolerance to define equalities
      ztol - elevation tolerance
    Return:
     tuple with two elements.  Element 0 will contain a deduced dict of 
     receiver attributes with unique locations.  After cross-checks it is 
     intended to be posted to MongoDB with update or insert.  Element 1 
     of a list of tuples with 0 containing the ObjectId of the parent 
     waveform (from wfcol) and 1 containing the unique site_id adetermined by 
     this algorithm.  
    """
    # this is a two key sort and find combined
    # WARNING:  google searches can yield comfusing exmaples.  pymongo 
    # syntax is deceptively different from mongo shell - don't follow a 
    # mongodb shell example
    # good source: https://kite.com/python/docs/pymongo.cursor.Cursor.sort
    allwf=wfcol.find().sort([('site_lat',pymongo.ASCENDING),
                     ('site_lon',pymongo.ASCENDING)])
    # note the cursor object allwf may need a call to the rewind method once
    # this loop finishes
    site_id=firstid
    count=0
    unique_coords={}
    idlist=[]
    firstflag=True
    for doc in allwf:
        # for unknown reasons even though doc is a dict object operator []
        # does not function and we need to use the get method
        lat=doc.get('site_lat')
        lon=doc.get('site_lon')
        elev=doc.get('site_elev')
        oid=doc.get('_id')
        if((lat==None) or (lon==None) or (elev==None)):
            print('coordinate key lookup failed for document number ',count)
            print('Document will be skipped')
        else:
            if(firstflag):
                idlist.append((oid,site_id))
                m0={}
                m0['site_id']=siteid
                m0['site_lat']=lat
                m0['site_lon']=lon
                m0['site_elev']=elev
                unique_coords[site_id]=m0
                lat0=lat
                lon0=lon
                elev0=elev
                firstflag=False
            else:
                dlat=lat-lat0
                dlon=lon-lon0
                delev=elev-elev0
                dr=sqrt(dlat*dlat+dlon*dlon)
                if( (dr>tolerance) or (delev>ztol)):
                    site_id+=1
                    m={}
                    m['site_id']=siteid
                    m['site_lat']=lat
                    m['site_lon']=lon
                    m['site_elev']=elev
                    unique_coords[site_id]=m
                    lat0=lat
                    lon0=lon
                    elev0=elev
                idlist.append((oid,site_id))
        count+=1
    return(idlist,unique_coords)
def load_site_wf(db,tolerance=0.0001,ztol=10.0,verbose=False):
    """
    Scans entire wf collection, extracts unique receiver locations, and 
    inserts the unique set into the site collection.   This function 
    is intended to be run on a wf collection of newly imported data 
    created by the antelope contrib program extract_events, run through 
    export_to_mspass, and then loaded to mongdb with dbsave_indexdata found
    in mspasspy.importer.   

    The algorithm is simple and should only be used in a context where 
    the wf collection matches the same assumptions made here.   That is:
    1.  Each wf document has receiver coordinates defined with the mspass
        names of: site_lon, site_lat, and site_elev.
    2.  The scaling defining if a station is 'close' to another is 
        frozen at 0.0001 degrees.  That comes from the highest precision
        digit for coordinates stored in Antelope.   If that is not 
        appropriate the function default can easily be changed for 
        a special applciation.

    The algorithm constructs a cross reference to site table using the 
    ObjectId of each entry it creates in site.  The string represention 
    of the matching doc in site to each wf entry is defined by the 
    key oid_site.  The cross-reference value for oid_site is placed 
    in each wf document.   

    Args:
      db - is the database handled returned from a call to MongoClient
           (the function will internally look up the wf and site collections)
      tolerance, ztol - see help(unique_receivers)
      verbose - when true will echo some information about number of entries
          found and added to each table (default is false).

    """
    # We have to first query the site collection to get the largest value 
    # of the simple integer staid.   A couple true incantations to pymongo
    # are needed to get that - follow
    sitecol=db.site
    nsite=sitecol.find().count() # Kosher way to get size of the collection
    if(nsite==0):
        startid=0
    else:
        maxcur=sitecol.find().sort([('site_id',pymongo.DESCENDING)]).limit(1)
        maxcur.rewind()   # may not be necessary but near zero cost
        maxdoc=maxcur[0]
        maxsid=maxdoc['site_id']
        startid=maxsid+1
        if(verbose):
            print('Existing site_id data found in wf collection\n'+\
                  'Setting initial site_id value to ',startid)
    wfcol=db.wf
    x=unique_receivers(wfcol,startid,tolerance,ztol)
    oidxref=x[0]
    idmap=x[1]
    if(verbose):
        print("load_site_wf:  Found ",len(idmap),\
            " unique receiver locations. Adding to site collection")
    sitexref={}
    for b in idmap:
        result=sitecol.insert_one(idmap[b])
        sid=result.inserted_id
        sitexref[b]=sid
    # Now we put sitid integer value and the ObjectId in sitecol into 
    # each wf collection document we match - should be all and assume that
    # Note for consistency and to allow the attribute to be passed to 
    # Metadata we convert to string representation
    count=1
    for a in oidxref:
        site_id=a[1]
        oid_site=sitexref[site_id]
        updict={'oid_site':oid_site}
        updict['site_id']=siteid
        wfcol.update_one({'_id':a[0]},{'$set':updict})
        count+=1
    if(verbose):
        print("load_site_wf:  updated site cross reference data in ",count,
            " documents of wf collection")
def unique_sources(wfcol,firstid,dttol=1250.0,degtol=1.0,drtol=25.0,Vsource=6.0):
    """
    Works through the return of a find with sort by time of the (potentially large) 
    list of dict containers of docs to be scanned.   Returns a subset of 
    input that have unique space-time positions defined by three tolerance 
    values that are used in a sequence most appropriate for earthquake 
    catalogs sorted in by time.   That sequence is:
        1.  If delta time is > dttol break and consider these distinct events
            (dttol should be of the order of P wave travel time through the earth
            to be safe).
        2.  Check if either the latitude or longitude are less than degtol. 
            If either are larger,  immediately define a new event.
        3.  Estimate a total distance in space-time units using a velocity
            multiplier defined by vsource (default 6.0 km/s).   Horizontal 
            distance of separation uses a local cartesian approximation with 
            delta longitude scaled by latitude.   Depth is used directly.  
            Time uses delta_otime*vsource.  L2 norm of that set of 4 delta 
            numbers are computed.   If the L2 norm is < drtol events 
            are considered equal.
    Note other than the difference of the above test this algorithm is 
    nearly identical to unique_receivers

    Args: 
      wfcol handle pointing at collection to be queried, sorted, and used for 
        as input the algorithm
      firstid is the initial source_id to use in this pass.  This should
        normally be created by scanning a previously created event collection
        but could be some arbitrarily large number (caution if you go there)
      dttol - initial time test tolerance (see above) (default 1250 s)
      degtol - lat,lon degree difference test tolerance (see above - default 
        1 degree for either coordinate)
      drtol - space-time distance test tolerance (see above - default of 25 km
        is appropriate for teleseismic data.  Local scales require a change 
        for certain)
      vsource is velocity used to convert origin time difference to a 
        distance for sum of square used for drtol test.
      
    Return:
     tuple with two elements.  Element 0 will contain a deduced dict of 
     source attributes with unique locations.  After cross-checks it is 
     intended to be posted to MongoDB with update or insert.  Element 1 
     of a list of tuples with 0 containing the ObjectId of the parent 
     waveform (from wfcol) and 1 containing the unique source_id adetermined by 
     this algorithm.  
    """
    degtor=pi/180.0   # degrees tp radians - needed below
    # this is a two key sort and find combined
    # WARNING:  google searches can yield comfusing exmaples.  pymongo 
    # syntax is deceptively different from mongo shell - don't follow a 
    # mongodb shell example
    # good source: https://kite.com/python/docs/pymongo.cursor.Cursor.sort
    allwf=wfcol.find().sort([('source_time',pymongo.ASCENDING)])
    # note the cursor object allwf may need a call to the rewind method once
    # this loop finishes
    source_id=firstid
    count=0
    unique_coords={}
    idlist=[]
    firstflag=True
    neweventflag=False
    for doc in allwf:
        # for unknown reasons even though doc is a dict object operator []
        # does not function and we need to use the get method
        lat=doc.get('source_lat')
        lon=doc.get('source_lon')
        depth=doc.get('source_depth')
        otime=doc.get('source_time')
        oid=doc.get('_id')
        if((lat==None) or (lon==None) or (depth==None) or (otime==None)):
            print('coordinate key lookup failed for document number ',count)
            print('Document will be skipped')
        else:
            if(firstflag):
                m={}
                m['source_id']=source_id
                m['source_lat']=lat
                m['source_lon']=lon
                m['source_depth']=depth
                m['source_time']=otime
                unique_coords[source_id]=m
                lat0=lat
                lon0=lon
                depth0=depth
                otime0=otime
                firstflag=False
                neweventflag=True
                idlist.append((oid,source_id))
            else:
                # first test dttol, then degtol, then drtol
                dotime=otime-otime0
                if(dotime>dttol):
                    neweventflag=True
                else:
                    if( (abs(lat-lat0)>degtol) or (abs(lon-lon0)>degtol)):
                        neweventflag=True
                    else:               
                        # crude radius of earth=6371 km
                        Rearth=6371.0
                        rlon=Rearth*cos(lat0*degtor)
                        dlatkm=Rearth*abs(lat-lat0)*degtor
                        dlonkm=rlon*abs(lon-lon0)*degtor
                        sumsq=dlatkm*dlatkm + dlonkm*dlonkm
                        dz=depth-depth0
                        sumsq += dz*dz
                        dotime=otime-otime0
                        sumsq += Vsource*Vsource*dotime*dotime
                        drst=sqrt(sumsq)
                        if(drst>drtol):
                            neweventflag=True
                        else:
                            neweventflag=False
                if(neweventflag):
                    source_id+=1
                    # A weird python thing is if the next line is missing
                    # unique_coords gets overwritten for each entry with 
                    # the last value.  reason, I think, is m is a pointer and
                    # without the next line the container is a fixed memory location
                    # that gets overwritten each pass
                    m={}
                    m['source_id']=source_id
                    m['source_lat']=lat
                    m['source_lon']=lon
                    m['source_depth']=depth
                    m['source_time']=otime
                    unique_coords[source_id]=m
                    #print(source_id,':  ',m)
                    lat0=lat
                    lon0=lon
                    depth0=depth
                    otime0=otime
                idlist.append((oid,source_id))
        count+=1
    return(idlist,unique_coords)
def load_source_wf(db,dttol=1250.0,degtol=1.0,drtol=25.0,verbose=False):
    """
    Scans entire wf collection, extracts unique source locations, and 
    inserts the unique set into the source collection.   This function 
    is intended to be run on a wf collection of newly imported data 
    created by the antelope contrib program extract_events, run through 
    export_to_mspass, and then loaded to mongdb with dbsave_indexdata found
    in mspasspy.importer. The algorithm is nearly identical to load_site_wf
    but for sources instead of receivers.  Perhaps could combine into \
    one function, but left for a different time.  
    
    The unique_sources function sorts out the unique source locations.  This
    function does mongodb interactions to insert the results in the source
    collection. 

    The algorithm constructs a cross reference to source collection using the 
    ObjectId of each entry it creates in source.  The string represention 
    of the matching doc in source to each wf entry is defined by the 
    key oid_source.  The cross-reference value for oid_source is placed 
    in each wf document.   

    Args:
      db - is the database handled returned from a call to MongoClient
           (the function will internally look up the wf and source collections)
      dttol, degtol, drtol - see help(unique_sources)
      verbose - when true will echo some information about number of entries
          found and added to each table (default is false).

    """
    # We have to first query the source collection to get the largest value 
    # of the simple integer staid.   A couple true incantations to pymongo
    # are needed to get that - follow
    sourcecol=db.source
    nsource=sourcecol.find().count() # Kosher way to get size of the collection
    if(nsource==0):
        startid=0
    else:
        # Docs claim this is faster than a full sort 
        maxcur=sourcecol.find().sort([('source_id',pymongo.DESCENDING)]).limit(1)
        maxcur.rewind()   # may not be necessary but near zero cost
        maxdoc=maxcur[0]
        maxsid=maxdoc['source_id']
        startid=maxsid+1
        if(verbose):
            print('Existing source_id data found in wf collection\n'+\
                  'Setting initial source_id value to ',startid)
    wfcol=db.wf
    x=unique_sources(wfcol,startid,dttol,degtol,drtol,verbose)
    oidxref=x[0]
    idmap=x[1]
    if(verbose):
        print("load_source_wf:  Found ",len(idmap),\
            " unique source locations. Adding to source collection")
    srcxref={}
    for b in idmap:
        result=sourcecol.insert_one(idmap[b])
        sid=result.inserted_id
        srcxref[b]=sid
        # debug - delete when working
    # Now we put sitid integer value and the ObjectId in sourcecol into 
    # each wf collection document we match - should be all and assume that
    # Note for consistency and to allow the attribute to be passed to 
    # Metadata we convert to string representation
    count=1
    updict={}
    for a in oidxref:
        #sm=idmap[a[1]]
        source_id=a[1]
        #oid_source=str(sm['_id'])
        updict['source_id']=source_id
        sid=srcxref[source_id]
        updict['source_oidstring']=str(sid)
        #print('updating:  source_id=',source_id,' setting oidstring=',str(sid))
        wfcol.update_one({'_id':a[0]},{'$set':updict})
        count+=1
    if(verbose):
        print("load_source_wf:  updated source cross reference data in ",count,
            " documents of wf collection")
