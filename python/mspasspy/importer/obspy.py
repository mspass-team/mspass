# Converter from obspy Trace object to mspass::TimeSeries
from mspasspy.ccore import CoreTimeSeries
from mspasspy.ccore import TimeSeries
from mspasspy.ccore import TimeReferenceType
from mspasspy.ccore import MetadataDefinitions
from mspasspy.ccore import MDDefFormat
from mspasspy.ccore import MDtype
# We need this for error ErrorLogger
from mspasspy.ccore import MsPASSError
from mspasspy.ccore import ErrorSeverity

from obspy import Trace
def obspy2mspass(d,mdef,mdother=[],aliases=[]):
    """
    Convert an obspy Trace object to a TimeSeries object.

    Arguments:
    d - obspy trace object to convert
    mdef - global metadata catalog.   Used to guarantee all names used are registered.
    mdother - This function always loads obspy required attributes and passes the directly to the mspass
        TimeSeries object..  For each entry in mdother the procedure tries to fetch that attribute.  It's type
        is then extracted from the MetadataDefinitions object and saved with the same key.
        (default is an empty list)
    aliases - is a list of attributes that should be posted to output with an alias name.  Aliases have
        a large overhead as the algorithm first looks for the unique key associated with the aliases and
        deletes it from the output before adding it back with the alias.   This is an essential feature to
        prevent duplicates that could easily get out of sync.  Aliases should generally be avoided but
        can be a useful feature. (default is an empty list)
    Returns a mspass TimeSeries object equivalent to obspy input but reorganized into mspass class structure
    """
# First get the data size to know use allocating constructor for TimeSeries
    ns=d.stats.npts
    dout=CoreTimeSeries(ns)
    # an api change is needed here.  Also TimeSeries needs to handle invalid objectid
    dout=TimeSeries(dout,"INVALID")
    dout.ns=ns
# Now extract TimeSeries attributes equivalent in d.stats and write to output
    dout.dt=d.stats.delta
# obspy only understands UTC time as a standard so we just set it
    dout.tref=TimeReferenceType.UTC
# This is a collision point between C and python.  In python a bool is a class
# so cannot just use a python syntax here. C treats bool as an int and anything
# not 0 is considered true
    dout.live=1
    t0utc=d.stats.starttime
    dout.t0=t0utc.timestamp
# These are required obspy attributes we pass to mspass::Metadata
    sta=d.stats.station
    net=d.stats.network
    chan=d.stats.channel
    calib=d.stats.calib
    dout.put("station",sta)
    dout.put("network",net)
    dout.put("channel",chan)
    dout.put("calib",calib)
# C++ library uses an stl vector container to contain the seismic samples
# pybind11 includes a wrapper for append which is equivalent to the C++ method
# push_back.   Hence, this loop is a copy of samples to the C++ container.
    for i in range(ns):
        dout.s.append(d.data[i])
# Things below here can throw exceptions.  We capture them with a feature
# of mspass using the ErrorLogger object that is a component of the TimeSeries
# object. Here we initialize the error logger to post obspy2mspass as the algorithm
    dout.elog.set_algorithm("obspy2mspass")

# Mow load and copy all mdother data. Cannot assume the set of attributes
# requested are actually with the data being imported for cxx
    for key in mdother:
        try:
            val=d.stats[key]
            try:
                typ = mdef.type(key)
                if(typ==MDtype.Int64 or typ==MDtype.Long or typ==MDtype.Integer) :
                    # assumes python casting does nothing if val already of that type
                    # this will fail if we try to convert strings to numbers so
                    # we use an error handler here
                    try :
                        val_to_save=int(val)
                        dout.put(key,val_to_save)
                    except ValueError:
                        error_message="obspy2mspass: Error handling attribute with key="+key+"\n"
                        error_message=error_message+"Data in trace attribute with this key cannot be converted to an Int64 as required by schema\n"
                        error_message=error_message+"Content stored with this key=" + str(val)
                        dout.elog.log_error(error_messag3e,ErrorSeverity.Complaint)
                        # testing - delete for release
                        print("Saved message=",error_message," to log")
                        #print("obspy2mspass: Error handling attribute with key="+key)
                        #print("Data in trace attribute with this key cannot be converted to an Int64 as required by schema")
                        #print("Content stored with this key=",val)
                elif(typ==MDtype.Real or typ==MDtype.Double or typ==MDtype.Real64) :
                    # identical to above but for floats = double in python
                    try :
                        val_to_save=float(val)
                        dout.put(key,val_to_save)
                    except ValueError:
                        error_message="obspy2mspass: Error handling attribute with key="+key+"\n"
                        error_message=error_message + "Data in trace attribute with this key cannot be converted to Real64 as required by schema\n"
                        error_message=error_message + "Content stored this key="+str(val)+"\n"
                        dout.elog.log_error(error_message,ErrorSeverity.Complaint)
                # Section for short versions of numeric types.   For now we promote
                # all int32 to int64 and real32 (C float) to real64(C double)
                # This code repeats above intentionally in case these cases
                # require later tweeks
                # Note for present we don't worry about unsigned int but that
                # might be useful
                elif(typ==MDtype.Int32) :
                    try:
                        val_to_save=int(val)
                        dout.put(key,val_to_save)
                        if(val>2147483648 or val<-2147483648) :
                            mess="obspy2mspass:  Attribute with key="+key+"\n"
                            mess=mess + "Valued stored="+str(val)+" will overflow Int32 required by schema\n"
                            mess=mess + "Converted for now to Int64, but may be wrong in output if saved\n"
                            dout.elog.log_error(mess,ErrorSeverity.Complaint)
                    except ValueError:
                        mess="obspy2mspass: Error handling attribute with key="+key+"\n"
                        mess=mess + "Data in trace attribute with this key cannot be converted to an integer as required by schema\n"
                        mess=mess + "Content stored with this key="+str(val)+"\n"
                        dout.elog.log_error(mess,ErrorSeverity.Complaint)
                elif(typ==MDtype.Real32):
                    try :
                        val_to_save=float(val)
                        dout.put(key,val_to_save)
                    except ValueError:
                        mess="obspy2mspass: Error handling attribute with key="+key+"\n"
                        mess=mess + "Data in trace attribute with this key cannot be converted to Real64 as required by schema\n"
                        mess=mess + "Note it is defines as a Real32, but is always promoted to Real64 inside mspass\n"
                        mess=mess + "Content stored this key="+str(val)+"\n"
                        dout.elog.log_error(mess,ErrorSeverity.Complaint)
                elif(typ==MDtype.String) :
                    # As far as I can tell the python str function is bombproof
                    # and will always return a result for any numeric typeself
                    # including np numeric types.   Hence, there is no try
                    # block for conversion errors there
                    val_to_save=str(val)
                    dout.put(key,val_to_save)
                elif(typ==MDtype.Boolean) :
                    # bool function also seems bombproof and a bool of a logical
                    # (e.g. bool(True)) does nothing.  Hence, we also don't need
                    # an error handler here
                    val_to_save=bool(val)
                    dout.put(key,val_to_save)
                else :
                    mess="obspy2mspass:  Cannot handle attribute with key =" + "\n"
                    + "MetadataDefinitions has an error and contains an unsupported type definition\n"
                    mess=mess + "Check data used to generate MetadataDefinitions for errors"
                    dout.elog.log_error(mess,ErrorSeverity.Complaint)
            except RuntimeError:
                mess="obspy2mspass:  optional key="+key+" is undefined in master schema\n"
                + "attribute linked to that key will not be copied to mspass TimeSeries\n"
                dout.elog.log_error(mess,ErrorSeverity.Complaint)
        except KeyError:
            mess="obspy2mspass:  optional key="+key+" not found in obspy stats\n"
            mess=mess + "attribute linked to that key will not be copied to mspass TimeSeries\n"
            dout.elog.log_error(mess,ErrorSeverity.Complaint)
    # Now handle entries that are to be set as aliases.  As docstring says the approach
    # is to delete contents of a master name associated with the requested alias to
    # avoid stale duplicates.  An additional complication is that we also have to clear
    # the entry that says the original attribute was saved.   The clear method does that
    for a in aliases:
        # The C function returns an std::pair.  pybind11 wrappers convert this to
        # a 2 element tuple with 0 as the unique key name and the 1 having the type
        # Error handler is necessary in case the alias is not undefined
        try:
            p=mdef.unique_name(a)
            ukey=p[0]
            typ=p[1]
            if(dout.is_defined(ukey)) :
                # these work because 32s were cast to 64s above we don't worry if that happens
                if(typ==MDtype.Int64 or typ==MDtype.Long or typ==MDtype.Integer \
                                or typ==MDtype.Int32):
                    val=dout.get_int(ukey)
                    dout.put(a,val)
                    dout.clear(ukey)
                elif(typ==MDtype.Real or typ==MDtype.Double or typ==MDtype.Real64 \
                            or typ==MDtype.Real32) :
                    val=dout.get_double(ukey)
                    dout.put(a,val)
                    dout.clear(ukey)
                elif(typ==MDtype.String) :
                    val=dout.get_string(ukey)
                    dout.put(a,val)
                    dout.clear(ukey)
                elif(typ==MDtype.Boolean) :
                    val=dout.get_bool(ukey)
                    dout.put(a,val)
                    dout.clear(ukey)
                else :
                    # This block should never be executed, but good for stability
                    mess="obspy2mspass (WARNING): cannot set attribute alias="+a+"\n"
                    mess=mess + "unique key="+ukey+" has illegal type\n"
                    mess=mess + "Check aliases section data used to create MetadataDefinitions for errors"
                    dout.elog.log_error(mess,ErrorSeverity.Complaint)
            else :
                mess="obspy2mspass (WARNING):  cannot rename attribute with key="+ukey+"\n"
                mess=mess + "Requested change to alias name="+a+" failed because "+ukey+" was not previously set\n"
                dout.elog.log_error(mess,ErrorSeverity.Complaint)
        except RuntimeError:
            mess="obspy2mspass:  Error during processing aliases list for alias key="+a+"\n"
            dout.elog.log_error(mess,ErrorSeverity.Complaint)
    return dout

# comparable function to take a Stream object with 3 members and return
# a Seismogram object
def obspy2mspass_3c(st,mdef,mdother=[],aliases=[],master=0,cardinal=bool(0),
                azimuth='azimuth',dip='dip'):
    """
    Convert an obspy Stream object with 3 components to a mspass::Seismogram
    (three-component data) object.  This implementation actually converts
    each component first to a TimeSeries and then calls a C++ function to
    assemble the complete Seismogram.   This has some inefficiencies, but
    the assumption is this function is called early on in a processing chain
    to build a raw data set.

    st - input obspy Stream object.  The object MUST have exactly 3 components
        or the function will throw a AssertionError exception.  The program is
        less dogmatic about start times and number of samples as these are
        handled by the C++ function this python script calls.  Be warned,
        however, that the C++ function can throw a MsPASSrror exception that
        should be handled separately.
    mdef - global metadata catalog.   Used to guarantee all names used are registered.
    mdother - This function always loads obspy required attributes and passes the directly to the mspass
        TimeSeries object..  For each entry in mdother the procedure tries to fetch that attribute.  It's type
        is then extracted from the MetadataDefinitions object and saved with the same key.
        (default is an empty list)   Must contain azimuth and dip keys (as passed)
        unless cardinal is true
    aliases - is a list of attributes that should be posted to output with an alias name.  Aliases have
        a large overhead as the algorithm first looks for the unique key associated with the aliases and
        deletes it from the output before adding it back with the alias.   This is an essential feature to
        prevent duplicates that could easily get out of sync.  Aliases should generally be avoided but
        can be a useful feature.  (default is an empty list)
    master - a Seismogram is an assembly of three channels composed created from
        three TimeSeries/Trace objects.   Each component may have different
        metadata (e.g. orientation data) and common metadata (e.g. station
        coordinates).   To assemble a Seismogram a decision has to be made on
        which component has the definitive common metadata.   We use a simple
        algorithm and clone the data from one component defined by this index.
        Must be 0,1, or 2 or the function wil throw a RuntimeError.  Default is 0.
    cardinal - boolean used to define one of two algorithms used to assemble the
        bundle.  When true the three input components are assumed to be in
        cardinal directions (x1=positive east, x2=positive north, and x3=positive up)
        AND in a fixed order of E,N,Z. Otherwise the Metadata fetched with
        the azimuth and dip keys are used for orientation.
    azimuth - defines the Metadata key used to fetch the azimuth angle
       used to define the orientation of each component Trace object.
       Default is 'azimuth' used by obspy.   Note azimuth=hang in css3.0.
       Cannot be aliased - must be present in obspy Stats unless cardinal is true
    dip - defines the Metadata key used to fetch the vertical angle orientation
        of each data component.  Vertical angle (vang in css3.0) is exactly
        the same as theta in spherical coordinates.  Default is obspy 'dip'
        key. Cannot be aliased - must be defined in obspy Stats unless
        cardinal is true

    Can throw either an AssertionError or MsPASSrror(currently defaulted to
    pybind11's default RuntimeError.  Error message can be obtained by
    calling the what method of RuntimeError).
    """
    # First make sure we have exactly 3 components
    assert len(st)==3, "Stream length must be EXACTLY 3 for 3-components"
    assert (master>=0 and master<3), "master argument must be 0, 1, or 2"
    # Complicated logic here, but the point is to make sure the azimuth
    # attribute is set. The cardinal part is to override the test if
    # we can assume he components are ENZ
    if(not cardinal):
        if( not (azimuth in mdother)) :
            raise RuntimeError("obspy2mspass_3c:  Required attribute "+azimuth+" must be in mdother list")
    if(not cardinal):
        if( not (dip in mdother)) :
            raise RuntimeError("obspy2mspass_3c:  Required attribute "+dip+" must be in mdother list")
    # Outer exception handler to handle range of possible errors in
    # converting each component.  Note we pass an empty list for mdother
    # and aliases except the master
    bundle=[]
    if(cardinal):
        mdo2=[]
    else:
        mdo2=[azimuth,dip]
    try:
        for i in range(3):
            tr=st[i]
            if(i==master):
                # Assume azimuth and dip variables are in mdother
                # perhaps should test that for stability
                bundle.append(obspy2mspass(tr,mdef,mdother,aliases))
            else:
                #We could depend on defaults here, but being explicit is safer
                bundle.append(obspy2mspass(tr,mdef,mdo2,[]))
    except RuntimeError:
        print("obspy2mspass_3c Seismogram converter - unrecoverable error in assembling components")
        # I believe this rethrows the exception
        raise
    # The constructor we use below has frozen names hang for azimuth and
    # vang for what obspy calls dip.   Copy to those names - should work
    # even if the hang and vang are the names although with some inefficiency
    # assume that would not be normal so avoid unnecessary code
    if(cardinal):
        for i in range(3):
            if(i<3):
                bundle[i].put("vang",90.0)
            else:
                bundle[i].put("vang",0.0)
        bundle[0].put("hang",90.0)
        bundle[1].put("hang",0.0)
        bundle[2].put("hang",0.0)
    else:
        for i in range(3):
            hang=bundle[i].get_double(azimuth)
            bundle[i].put('hang',hang)
            vang=bundle[i].get_double(dip)
            bundle[i].put('vang',vang)
    # Assume now bundle contains all the pieces we need.   This constructor
    # for CoreSeismogram should then do the job
    # This may throw an exception, but we require the caller to handle it
    # All errors returned by this constructor currenlty leave the data INVALID
    # so handler should discard anything with an error
    from mspasspy.ccore import CoreSeismogram
    from mspasspy.ccore import Seismogram
    dout=CoreSeismogram(bundle,master)
    return(Seismogram(dout,'INVALID'))
