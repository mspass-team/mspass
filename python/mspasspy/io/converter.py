
"""
Functions for converting to and from MsPASS data types.
"""
import numpy as np
import obspy.core

from mspasspy.ccore import (CoreSeismogram,
                            CoreTimeSeries,
                            ExtractComponent,
                            ErrorSeverity,
                            Metadata,
                            MDtype,
                            Seismogram,
                            TimeReferenceType,
                            TimeSeries)


def dict2Metadata(d, mdef, elog):
  """
  Function to convert python dict data returned by MongoDB find to mspass::Metadata.

  pymongo returns a python dict container from find queries to any collection.
  Simple type in returned documents can be converted to mspass::Metadata
  that are used as headers in the C++ components of mspass.  This is a core,
  bombproof routine to do that.  It handles supported types and will issue
  warning if asked to convert an unsupported type.

  :param d: is the dict created by MongoDB query
  :param mdef: is the MetadataDefinitions object used to validate types against
        keys and types expected by mspass.  If a mismatch occurs the
        function attempts to convert to the type defined in mdef.  If
        a key is missing the data are still converted and a warning is issued.
  :param elog: is expected to be an ErrorLogger object.   It should normally be
        already be a component of the Seismogram or TimeSeries with which it
        is or will be (read) associated.   Any conversions problems in
        this function will create log messages of different severity
        posted to elog.   If elog is not already linked to a Seismogram or
        TimeSeries an empty container must be created and passed AND after
        running if it is not empty it should be added to the data object
        being created.
  :return: Metadata object translated from d
  :rtype: Metadata object (ccore)
  :raise:  Should never throw an exception, but can leave messages on elog.
  """
  md=Metadata()
  for x in d.keys():
      if(x=='_id'):
        # We assume this is always a MongoDB ObjectId
        y=d[x]
        ys=str(y)
        md.put_string("wfid_string",ys)  # Frozen name in C++ constructors
      else:
        y=d[x]
        # For some strange reason bool must appear before int.  A bool will
        # pass an isinstance test for int a return a false true. Because of
        # the chain progression of if,elseif constructs this can work.
        # Note that is possible because the reverse does not misbehave
        # isinstance of a bool will not return a true if the arg is int
        if(isinstance(y,bool)):
          try:
              mdt=mdef.type(x)
              if(mdt==MDtype.Boolean):
                  md.put_bool(x,y)
              else:
                  elog.log_error("dict2Metadata","Mismatched attribute types\n"
                    + "MongoDB value is a boolean, but schema defined by MetadataDefintions requires something else\n"
                    + "Attribute not copied - no recovery possible\n",
                    ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2Metadata","key="+x+" is not defined in schema defined by MetadataDefintions\n"
              + "Copying to Metadata as a boolean as was stored in MongoDB\n",
              ErrorSeverity.Complaint)
            md.put_bool(x,y)
        elif(isinstance(y,int)):
          try:
              mdt=mdef.type(x)
              # All classes of  int can be directly converted
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                yi=int(y)
                md.put_long(x,yi)
              # output to float is harmless, but will create a warning
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                elog.log_error("dict2Metadata","dict2Metadata (Warning):  Mismatched attribute types\n"
                  + "Attribute returned for key="+x+" is an integer but schema demands a float\n"
                  + "Converting internally to a float\n",ErrorSeverity.Complaint)
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                elog.log_error("dict2Metadata","Mismatched attribute types\n"
                  + "Attribute returned for key="
                  +x
                  +" is an integer but schema demands a string\n"
                  + "Converting internally to a string\n",
                  ErrorSeverity.Complaint)
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                yi=int(y)  #  Probably unnecessary but safer
                md.put_bool(x,yi)  #  C will make 0 false and anything else true
              else:
                elog.log_error("dict2Metadata","MetadataDefinition returned undefined type",
                        " for key="+x+"\nAttribute dropped\n",ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2Metadata","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as an integer as was stored in MongoDB",
             ErrorSeverity.Complaint)
            yi=int(y)
            md.put_long(x,yi)
        elif(isinstance(y,float)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                elog.log_error("dict2Metadata","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is float but schema demands an int\n"
                 + "Converting internally to a integer",ErrorSeverity.Complaint)
                yi=int(y)
                md.put_long(x,yi)
              # output to float is harmless, but will create a warning
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                elog.log_error("dict2Metadata","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is a float but schema demands a string\n"
                 + "Converting internally to a string",ErrorSeverity.Complaint)
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                elog.log_error("dict2Metadata","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is a float but schema demands a boolean\n"
                 + "Attribute will not be copied - no clear conversion is possible",
                 ErrorSeverity.Suspect)
              else:
                elog.log_error("dict2Metadata","MetadataDefinition returned undefined type",
                        " for key="+x+"Attribute ignored\n",ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2Metadata","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as a float as was stored in MongoDB\n",
             ErrorSeverity.Complaint)
            yd=float(y)
            md.put(x,yd)
        elif(isinstance(y,str)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                elog.log_error("dict2Metadata","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is string but schema demands an int\n"
                 + "Attempting to converting to a integer\n",
                 ErrorSeverity.Complaint)
                try:
                    yi=int(y)
                    md.put_long(x,yi)
                except ValueError:
                    elog.log_error("dict2Metadata","Conversion to int for key ="+x+" failed for string="
                       +y+"found in input dict\n"
                     + "Skipping this attribute",ErrorSeverity.Suspect)
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                emess="Mismatched attribute types\n"+\
                  "Attribute returned for key="+x+\
                  " is string but schema demands an int\n"+\
                  "Attempting to converting to a integer\n"
                sev=ErrorSeverity.Complaint
                try:
                    yf=float(y)
                    md.put_double(x,yi)  # use default for sev set above if it worked
                except ValueError:
                    emess+="Conversion to float failed for string="+y\
                     + "Skipping this attribute"
                    sev=ErrorSeverity.Suspect
                finally:
                    elog.log_error("dict2Metadata",emess,sev)
              elif(mdt==MDtype.String):
                md.put_string(x,y)
              elif(mdt==MDtype.Boolean):
                emess="dict2Metadata (Warning):  Mismatched attribute types\n"\
                 + "Attribute returned for key="+x+" is a string="+y+" but schema demands a boolean\n"\
                 + "Attempting conversion\n"
                if((y=='false') or (y=='FALSE') or (y=='0') ):
                    md.put_bool(x,0)
                    emess+="Parsed as a false and saving as such\n"
                else:
                    md.put_bool(x,1)
                    emess+="Assumed true"
                elog.log_error("dict2Metadata",emess,ErrorSeverity.Complaint)
              else:
                elog.log_error("dict2Metadata","dict2Metadata(Warning):  MetadataDefinition returned undefined type",
                        " for key="+x+"\nAttribute dropped\n",
                        ErrorSeverity.Complaint)
          except RuntimeError:
            elog.log_error("dict2Metadata","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as a string as was stored in MongoDB",
             ErrorSeverity.Complaint)
            md.put_string(x,y)
        else:
          # python equivalent of sprintf
          emess="data with key="+x+" is unsupported type=%s\n" % type(y)
          emess+="attribute will not be loaded\n"
          elog.log_error("dict2Metadata",emess,ErrorSeverity.Suspect)
  return md
#dict.toMetadata = dict2Metadata

def Metadata2dict(md):
    """
    Converts a Metadata object to a python dict.
    
    This is the inverse of dict2Metadata.  It converts a Metadata object to 
    a python dict, usually for interaction with MongoDB.  Only supports
    basic types of int, double, bool, and string.  
    
    :param md:  Metadata object to convert.
    :type md:  Metadata
    :return:  python dict equivalent to md.
    :raise:  AssertionError will be thrown if md contains anything but int, double, string, or bool
    """
    result={}
    keys=md.keys()
    for k in keys:
        typ=md.type(k)
        if(typ=='int'):
            ival=md.get_int(k)
            result[k]=ival
        elif(typ=='double'):
            dval=md.get_double(k)
            result[k]=dval
        elif(typ=='bool'):
            bval=md.get_bool(k)
            result[k]=bval
        else:
            # should be a string, but we need a sanity check as 
            # C code could insert nonstandard Metadata
            assert("string" in typ), "Metadata2dict: Unsupported type for key %s=%s" % (k,typ)
            sval=md.get_string(k)
            result[k]=sval
    return result
Metadata.todict = Metadata2dict

def TimeSeries2Trace(d, mdef):
    """
    Converts a TimeSeries object to an obspy Trace object.
    
    MsPASS can handle scalar data either as an obspy Trace object or
    as with the mspass TimeSeries object.  The capture nearly the same 
    concepts.  The main difference is that TimeSeries support the 
    error logging and history features of mspass while obspy, which is
    a separate package, does not.  Obspy has a number of useful 
    algorithms that operate on scalar data, however, so it is frequently 
    useful to switch between Trace and TimeSeries formats.  The user is
    warned, however, that converting a TimeSeries to a Trace object 
    with this function will result in the loss of any error log information.
    For production runs unless the data set is huge, we recommend saving
    the intermediate result AFTER calling this function if there is any
    possibility there are errors posted on any data.  We say after because
    some warning errors from this function may be posted in elog.  Since 
    python uses call by reference d may thus be altered.
    
    :param d: is the TimeSeries object to be converted
    :param mdef: is a MetadataDefinitions object defining the types 
       of expected Metadata in the TimeSeries object and any alias 
       information needed to translate names.  
       
    :return: an obspy Trace object from conversion of d.  An empty Trace
        object will be returned if d was marked dead
    :rtype:  obspy Trace object
    """
    myname='TimeSeries2Trace'
    dresult=obspy.core.Trace()
    # Silently return an empty trace object if the data are marked dead now
    if(not d.live):
        return dresult
     # We first deal with attributes in BasicTimeSeries that have to 
     # be translated into an obspy stats dictionary like object
    dresult.stats['delta']=d.dt
    dresult.stats['sampling_rate']=1.0/d.dt
    dresult.stats['npts']=d.ns
    dresult.stats['starttime']=obspy.core.UTCDateTime(d.t0)
    #dobspy.stats['endtime']=obspy.core.UTCDateTime(d.endtime())
     # These are required by obspy but optional in mspass.  Hence, we have
     # to extract them with caution.  Note defaults are identical to 
     # Trace constructor
    if(d.is_defined('net')):
        dresult.stats['network']=d.get_string('net')
    else:
        dresult.stats['network']=''
    if(d.is_defined('sta')):
        dresult.stats['station']=d.get_string('sta')
    else:
        dresult.stats['station']=''
    if(d.is_defined('channel')):
        dresult.stats['channel']=d.get_string('chan')
    else:
        dresult.stats['channel']=''
    if(d.is_defined('loc')):
        dresult.stats['location']=d.get_string('loc')
    else:
        dresult.stats['location']=''
    if(d.is_defined('calib')):
        dresult.stats['calib']=d.get_double('calib')
    else:
        dresult.stats['calib']=1.0   
         
    # It might be possible to replace the following loop with 
    # calling an (nonexistent) function called md2dict that would
    # generalize this loop.   May need that for serialization and it 
    # may be written in C.  If so, we might want to replace the loop
    # below with a call to the function for speed.  On the other hand,
    # creating a dict would require a copy loop or perhaps the 
    # function could be simplified with a call to Trace(dict) where
    # dict is the translation of md - as I read Trace i think that might
    # work as an alternative algorithm.  Make it work before you make
    # it fast!
    keys=d.keys()
    for k in keys:
        if(mdef.is_defined(k)):
            typ=mdef.type(k)
            # All options may not be necessary, but safer this way
            # intentionally do not use an exception handler as these should
            # never fail with this algorithm - if they do there is a bug
            if(typ==MDtype.Int64 or typ==MDtype.Long or typ==MDtype.Integer):
                dresult.stats[k]=d.get_long(k)
            elif(typ==MDtype.Real or typ==MDtype.Double or typ==MDtype.Real64):
                dresult.stats[k]=d.get_double(k)
            elif(typ==MDtype.String):
                dresult.stats[k]=d.get_string(k)
            elif(typ==MDtype.Boolean):
                #Not sure how obpsy would actually handle a boolean.   For 
                #now we just copy the boolean object python uses to stats
                dresult.stats[k]=d.get_bool(k)
            else:
                # We don't abort here but post to d's error log. 
                emess=myname+":  Metadata for key ="+k+" has an invalid MDtype\nNot copied"
                d.elog.log_error(myname,emess,ErrorSeverity.Complaint)
        else:
            emess=myname+":  No type is defined for Metadata with key="+k+"\nAttribute not copied"
            d.elog.log_error(myname,emess,ErrorSeverity.Complaint)
    # now we copy the data - there might be a faster way to do this with numpy
    # vector notation or with the buffer arg.  Here we do a brutal python loop
    dresult.data=np.ndarray(d.ns)
    for i in range(d.ns):
        dresult.data[i]=d.s[i]
    return dresult
TimeSeries.toTrace = TimeSeries2Trace

def Seismogram2Stream(d, mdef, chanmap=['E','N','Z'], hang=[90.0,0.0,0.0], vang=[90.0,90.0,0.0]):
    """
    Convert a mspass::Seismogram object to an obspy::Stream with 3 components split apart.
    
    mspass and obspy have completely incompatible approaches to handling three
    component data.  obspy uses a Stream object that is a wrapper around and
    a list of Trace objects.  mspass stores 3C data bundled into a matrix
    container.   This function takes the matrix container apart and produces
    the three Trace objects obspy want to define 3C data.   The caller is 
    responsible for how they handle bundling the output.    
    
    A very dark side of this function is any error log entries in the part 
    mspass Seismogram object will be lost in this conversion as obspy 
    does not implement that concept.  If you need to save the error log
    you will need to save the input of this function to MongoDB to preserve 
    the errorlog it may contain.  
    
    :param d: is the Seismogram object to be converted
    :param mdef: MetadataDefinitions object required to enforce schema type
       restrictions. 
    :param chanmap:  3 element list of channel names to be assigned components
    :param hang:  3 element list of horizontal angle attributes (azimuth in degrees) 
      to be set in Stats array of output for each component.  (default is
      for cardinal directions)
    :param vang:  3 element list of vertical angle (theta of spherical coordinates)
      to be set in Stats array of output for each component.  (default is
      for cardinal directions)
    :return: obspy Stream object containing a list of 3 Trace objects in 
       mspass component order.   Presently the data are ALWAYS returned to 
       cardinal directions.   (see above)
    :rtype:  obspy Stream object - will be empty if d was marked dead
    """
    dresult=obspy.core.Stream()
    # Note this logic will silently return an empty Stream object if the
    # data are marked dead
    if(d.live):
        oids=d.get_id()
        logstuff=d.elog
        for i in range(3):
            ts=ExtractComponent(d,i)
            ts.put_string('chan',chanmap[i])
            ts.put_double('hang',hang[i])
            ts.put_double('vang',vang[i])
            # ts is a CoreTimeSeries but we need to add a few things to 
            # make it mesh with TimeSeries2Trace
            tsex=TimeSeries(ts,oids)
            tsex.elog=logstuff
            dobspy=TimeSeries2Trace(tsex,mdef)
            dresult.append(dobspy)
    return dresult
Seismogram.toStream = Seismogram2Stream

def Trace2TimeSeries(d, mdef, mdother=[], aliases=[]):
    """
    Convert an obspy Trace object to a TimeSeries object.

    :param d: obspy trace object to convert
    :param mdef: global metadata catalog.   Used to guarantee all names used are registered.
    :param mdother: This function always loads obspy required attributes and passes the directly to the mspass
        TimeSeries object..  For each entry in mdother the procedure tries to fetch that attribute.  It's type
        is then extracted from the MetadataDefinitions object and saved with the same key.
        (default is an empty list)
    :param aliases: is a list of attributes that should be posted to output with an alias name.  Aliases have
        a large overhead as the algorithm first looks for the unique key associated with the aliases and
        deletes it from the output before adding it back with the alias.   This is an essential feature to
        prevent duplicates that could easily get out of sync.  Aliases should generally be avoided but
        can be a useful feature. (default is an empty list)
    Returns a mspass TimeSeries object equivalent to obspy input but reorganized into mspass class structure
    :return:  TimeSeries object derived from obpsy input Trace object
    :rtype: TimeSeries (ccore)
    :raise:  Should never throw an exception, but the elog area of the 
       returned TimeSeries should be tested for errors.  If the errors make
       the input invalid the live boolean in TimeSeries will be set false. 
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
    # object.  This name is posted to all error messages.
    algorithm="Trace2TimeSeries"

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
                        error_message="Error handling attribute with key="+key+"\n"
                        error_message=error_message + "Data in trace attribute with this key cannot be converted to an Int64 as required by schema\n"
                        error_message=error_message + "Content stored with this key=" + str(val)
                        dout.elog.log_error(algorithm,error_message,ErrorSeverity.Complaint)
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
                        error_message="Error handling attribute with key="+key+"\n"
                        error_message=error_message + "Data in trace attribute with this key cannot be converted to Real64 as required by schema\n"
                        error_message=error_message + "Content stored this key="+str(val)+"\n"
                        dout.elog.log_error(algorithm,error_message,ErrorSeverity.Complaint)
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
                            mess="Attribute with key="+key+"\n"
                            mess=mess + "Valued stored="+str(val)+" will overflow Int32 required by schema\n"
                            mess=mess + "Converted for now to Int64, but may be wrong in output if saved\n"
                            dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
                    except ValueError:
                        mess="Error handling attribute with key="+key+"\n"
                        mess=mess + "Data in trace attribute with this key cannot be converted to an integer as required by schema\n"
                        mess=mess + "Content stored with this key="+str(val)+"\n"
                        dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
                elif(typ==MDtype.Real32):
                    try :
                        val_to_save=float(val)
                        dout.put(key,val_to_save)
                    except ValueError:
                        mess="Error handling attribute with key="+key+"\n"
                        mess=mess + "Data in trace attribute with this key cannot be converted to Real64 as required by schema\n"
                        mess=mess + "Note it is defines as a Real32, but is always promoted to Real64 inside mspass\n"
                        mess=mess + "Content stored this key="+str(val)+"\n"
                        dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
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
                    mess="Cannot handle attribute with key =" + "\n" + "MetadataDefinitions has an error and contains an unsupported type definition\n"
                    mess=mess + "Check data used to generate MetadataDefinitions for errors"
                    dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
            except RuntimeError:
                mess="Optional key="+key+" is undefined in master schema\n" + "attribute linked to that key will not be copied to mspass TimeSeries\n"
                dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
        except KeyError:
            mess="Optional key="+key+" not found in obspy stats\n"
            mess=mess + "attribute linked to that key will not be copied to mspass TimeSeries\n"
            dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
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
                    mess="Cannot set attribute alias="+a+"\n"
                    mess=mess + "unique key="+ukey+" has illegal type\n"
                    mess=mess + "Check aliases section data used to create MetadataDefinitions for errors"
                    dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
            else :
                mess="Cannot rename attribute with key="+ukey+"\n"
                mess=mess + "Requested change to alias name="+a+" failed because "+ukey+" was not previously set\n"
                dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
        except RuntimeError:
            mess="Error during processing aliases list for alias key="+a+"\n"
            dout.elog.log_error(algorithm,mess,ErrorSeverity.Complaint)
    return dout
obspy.core.Trace.toTimeSeries = Trace2TimeSeries

def Stream2Seismogram(st, mdef, mdother=[], aliases=[], master=0, cardinal=bool(0),
                azimuth='azimuth', dip='dip'):
    """
    Convert obspy Stream to a Seismogram.

    Convert an obspy Stream object with 3 components to a mspass::Seismogram
    (three-component data) object.  This implementation actually converts
    each component first to a TimeSeries and then calls a C++ function to
    assemble the complete Seismogram.   This has some inefficiencies, but
    the assumption is this function is called early on in a processing chain
    to build a raw data set.

    :param st: input obspy Stream object.  The object MUST have exactly 3 components
        or the function will throw a AssertionError exception.  The program is
        less dogmatic about start times and number of samples as these are
        handled by the C++ function this python script calls.  Be warned,
        however, that the C++ function can throw a MsPASSrror exception that
        should be handled separately.
    :param mdef: global metadata catalog.   Used to guarantee all names used are registered.
    :param mdother: This function always loads obspy required attributes and passes the directly to the mspass
        TimeSeries object..  For each entry in mdother the procedure tries to fetch that attribute.  It's type
        is then extracted from the MetadataDefinitions object and saved with the same key.
        (default is an empty list)   Must contain azimuth and dip keys (as passed)
        unless cardinal is true
    :param aliases: is a list of attributes that should be posted to output with an alias name.  Aliases have
        a large overhead as the algorithm first looks for the unique key associated with the aliases and
        deletes it from the output before adding it back with the alias.   This is an essential feature to
        prevent duplicates that could easily get out of sync.  Aliases should generally be avoided but
        can be a useful feature.  (default is an empty list)
    :param master: a Seismogram is an assembly of three channels composed created from
        three TimeSeries/Trace objects.   Each component may have different
        metadata (e.g. orientation data) and common metadata (e.g. station
        coordinates).   To assemble a Seismogram a decision has to be made on
        which component has the definitive common metadata.   We use a simple
        algorithm and clone the data from one component defined by this index.
        Must be 0,1, or 2 or the function wil throw a RuntimeError.  Default is 0.
    :param cardinal: boolean used to define one of two algorithms used to assemble the
        bundle.  When true the three input components are assumed to be in
        cardinal directions (x1=positive east, x2=positive north, and x3=positive up)
        AND in a fixed order of E,N,Z. Otherwise the Metadata fetched with
        the azimuth and dip keys are used for orientation.
    :param azimuth: defines the Metadata key used to fetch the azimuth angle
       used to define the orientation of each component Trace object.
       Default is 'azimuth' used by obspy.   Note azimuth=hang in css3.0.
       Cannot be aliased - must be present in obspy Stats unless cardinal is true
    :param dip:  defines the Metadata key used to fetch the vertical angle orientation
        of each data component.  Vertical angle (vang in css3.0) is exactly
        the same as theta in spherical coordinates.  Default is obspy 'dip'
        key. Cannot be aliased - must be defined in obspy Stats unless
        cardinal is true

    :raise: Can throw either an AssertionError or MsPASSrror(currently defaulted to
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
            raise RuntimeError("Stream2Seismogram:  Required attribute "+azimuth+" must be in mdother list")
    if(not cardinal):
        if( not (dip in mdother)) :
            raise RuntimeError("Stream2Seismogram:  Required attribute "+dip+" must be in mdother list")
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
                bundle.append(Trace2TimeSeries(tr,mdef,mdother,aliases))
            else:
                #We could depend on defaults here, but being explicit is safer
                bundle.append(Trace2TimeSeries(tr,mdef,mdo2,[]))
    except RuntimeError:
        print("Stream2Seismogram: Unrecoverable error in assembling components")
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
    dout=CoreSeismogram(bundle,master)
    return(Seismogram(dout,'INVALID'))
obspy.core.Stream.toSeismogram = Stream2Seismogram
