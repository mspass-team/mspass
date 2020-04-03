
"""
Functions for converting to and from MsPASS data types.
"""

from mspasspy.ccore import (Metadata,
                            MDtype,
                            ErrorSeverity)


def dict2Metadata(d,mdef,elog):
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