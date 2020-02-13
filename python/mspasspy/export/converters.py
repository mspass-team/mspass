from mspasspy.ccore import Seismogram
from mspasspy.ccore import TimeSeries
from mspasspy.ccore import ExtractComponent
from mspasspy.ccore import TimeReferenceType
from mspasspy.ccore import MetadataDefinitions
from mspasspy.ccore import MDDefFormat
from mspasspy.ccore import MDtype
from mspasspy.ccore import MsPASSError
from mspasspy.ccore import ErrorSeverity
from obspy.core import Trace
from obspy.core import Stream
from obspy.core import UTCDateTime
import numpy as np
def mspass2obspy(d,mdef):
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
    myname='mspass2obspy'
    dobspy=Trace()
    # Silently return an empty trace object if the data are marked dead now
    if(not d.live):
        return dbospy
     # We first deal with attributes in BasicTimeSeries that have to 
     # be translated into an obspy stats dictionary like object
    dobspy.stats['delta']=d.dt
    dobspy.stats['sampling_rate']=1.0/d.dt
    dobspy.stats['npts']=d.ns
    dobspy.stats['starttime']=UTCDateTime(d.t0)
    #dobspy.stats['endtime']=UTCDateTime(d.endtime())
     # These are required by obspy but optional in mspass.  Hence, we have
     # to extract them with caution.  Note defaults are identical to 
     # Trace constructor
    if(d.is_defined('net')):
        dobspy.stats['network']=d.get_string('net')
    else:
        dobspy.stats['network']=''
    if(d.is_defined('sta')):
        dobspy.stats['station']=d.get_string('sta')
    else:
        dobspy.stats['station']=''
    if(d.is_defined('channel')):
        dobspy.stats['channel']=d.get_string('chan')
    else:
        dobspy.stats['channel']=''
    if(d.is_defined('loc')):
        dobspy.stats['location']=d.get_string('loc')
    else:
        dobspy.stats['location']=''
    if(d.is_defined('calib')):
        dobspy.stats['calib']=d.get_double('calib')
    else:
        dobspy.stats['calib']=1.0   
         
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
                dobspy.stats[k]=d.get_long(k)
            elif(typ==MDtype.Real or typ==MDtype.Double or typ==MDtype.Real64):
                dobspy.stats[k]=d.get_double(k)
            elif(typ==MDtype.String):
                dobspy.stats[k]=d.get_string(k)
            elif(typ==MDtype.Boolean):
                #Not sure how obpsy would actually handle a boolean.   For 
                #now we just copy the boolean object python uses to stats
                dobspy.stats[k]=d.get_bool(k)
            else:
                # We don't abort here but post to d's error log. 
                emess=myname+":  Metadata for key ="+k+" has an invalid MDtype\nNot copied"
                d.elog.log_error(myname,emess,ErrorSeverity.Complaint)
        else:
            emess=myname+":  No type is defined for Metadata with key="+k+"\nAttribute not copied"
            d.elog.log_error(myname,emess,ErrorSeverity.Complaint)
    # now we copy the data - there might be a faster way to do this with numpy
    # vector notation or with the buffer arg.  Here we do a brutal python loop
    dobspy.data=np.ndarray(d.ns)
    for i in range(d.ns):
        dobspy.data[i]=d.s[i]
    return dobspy
def mspass2obspy_3C(d,mdef,chanmap=['E','N','Z'],hang=[90.0,0.0,0.0],vang=[90.0,90.0,0.0]):
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
    dresult=Stream()
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
            # make it mesh with mspass2obspy
            tsex=TimeSeries(ts,oids)
            tsex.elog=logstuff
            dobspy=mspass2obspy(tsex,mdef)
            dresult.append(dobspy)
    return dresult
       

