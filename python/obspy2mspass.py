# Converter from obspy Trace object to mspass::TimeSeries
from mspasspy import CoreTimeSeries
from mspasspy import TimeSeries
from mspasspy import TimeReferenceType
from mspasspy import MetadataDefinitions
from mspasspy import MDDefFormat
from mspasspy import MDtype
# We need this for error ErrorLogger
from mspasspy import MsPASSError
from mspasspy import ErrorSeverity

from obspy import Trace
def obspy2mspass(d,mdef,mdother,aliases):
    """
    Convert an obspy Trace object to a TimeSeries object.

    Arguments:
    d - obspy trace object to convert
    mdef - global metadata catalog.   Used to guarantee all names used are registered.
    mdother - This function always loads obspy required attributes and passes the directly to the mspass
        TimeSeries object..  For each entry in mdother the procedure tries to fetch that attribute.  It's type
        is then extracted from the MetadataDefinitions object and saved with the same key.
    aliases - is a list of attributes that should be posted to output with an alias name.  Aliases have
        a large overhead as the algorithm first looks for the unique key associated with the aliases and
        deletes it from the output before adding it back with the alias.   This is an essential feature to
        prevent duplicates that could easily get out of sync.  Aliases should generally be avoided but
        can be a useful feature.
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
