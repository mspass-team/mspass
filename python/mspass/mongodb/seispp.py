#!/usr/bin/env python3
from mspasspy import MetadataDefinitions
from mspasspy import Metadata
from mspasspy import MDtype
def dict2md(d,mdef):
  """
  Function to convert python dict data returned by MongoDB find to mspass::Metadata.
  
  pymongo returns a python dict container from find queries to any collection.
  Simple type in returned documents can be converted to mspass::Metadata 
  that are used as headers in the C++ components of mspass.  This is a core,
  bombproof routine to do that.  It handles supported types and will issue 
  warning if asked to convert an unsupported type.
  
  Args:
      d is the dict created by MongoDB query
      mdef is the MetadataDefinitions object used to validate types against 
        keys and types expected by mspass.  If a mismatch occurs the 
        function attempts to convert to the type defined in mdef.  If 
        a key is missing the data are still converted and a warning is issued.
  Returns:
      Metadata object
  """
  md=Metadata()
  for x in d.keys():
      if(x=='_id'):
        # We assume this is always a MongoDB ObjectId
        y=d[x]
        ys=str(y)
        md.put_string("oid_string",ys)  # Frozen name in C++ constructors
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
                  print("dict2md (Warning):   Mismatched attribute types")
                  print("MongoDB value is a boolean, but schema defined by MetadataDefintions requires something else")
                  print("Attribute not copied - no recovery possible")
          except RuntimeError:
            print("dict2md(Warning): key=",x," is not defined in schema defined by MetadataDefintions")
            print("Copying to Metadata as a boolean as was stored in MongoDB")
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
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is an integer but schema demands a float")
                print("Converting internally to a float")
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is an integer but schema demands a string")
                print("Converting internally to a string")
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                yi=int(y)  #  Probably unnecessary but safer
                md.put_bool(x,yi)  #  C will make 0 false and anything else true
              else:
                print("dict2md(Warning):  MetadataDefinition returned undefined type",
                        " for key=",x)
                print("Ignored")
          except RuntimeError:
            print("dict2md(Warning): key=",x," is not defined in schema defined by MetadataDefintions")
            print("Copying to Metadata as an integer as was stored in MongoDB")
            yi=int(y)
            md.put_int(x,yi)
        elif(isinstance(y,float)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long) 
                      or (mdt==MDtype.Int64) ):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is float but schema demands an int")
                print("Converting internally to a integer")
                yi=int(y)
                md.put_long(x,yi)
              # output to float is harmless, but will create a warning
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is a float but schema demands a string")
                print("Converting internally to a string")
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is a float but schema demands a boolean")
                print("Attribute will not be copied - no clear conversion is possible")
              else:
                print("dict2md(Warning):  MetadataDefinition returned undefined type",
                        " for key=",x)
                print("Ignored")
          except RuntimeError:
            print("dict2md(Warning): key=",x," is not defined in schema defined by MetadataDefintions")
            print("Copying to Metadata as a float as was stored in MongoDB")
            yd=float(y)
            md.put(x,yd)
        elif(isinstance(y,str)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long) 
                      or (mdt==MDtype.Int64) ):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is string but schema demands an int")
                print("Attempting to converting to a integer")
                try:
                    yi=int(y)
                    md.put_long(x,yi)
                except ValueError:
                    print("Conversion to int failed for string=",y)
                    print("Skipping this attribute")
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is string but schema demands an int")
                print("Attempting to converting to a integer")
                try:
                    yf=float(y)
                    md.put_double(x,yi)
                except ValueError:
                    print("Conversion to float failed for string=",y)
                    print("Skipping this attribute")
              elif(mdt==MDtype.String): 
                md.put_string(x,y)
              elif(mdt==MDtype.Boolean):
                print("dict2md (Warning):  Mismatched attribute types")
                print("Attribute returned for key=",x," is a string but schema demands a boolean")
                print("Attempting conversion")
                if((y=='false') or (y=='FALSE') or (y=='0') ):
                    md.put_bool(x,0) 
                    print("Parsed as a false and saving as such")
                else:
                    md.put_bool(x,1)
                    print("Assumed true")
              else:
                print("dict2md(Warning):  MetadataDefinition returned undefined type",
                        " for key=",x)
                print("Ignored")
          except RuntimeError:
            print("dict2md(Warning): key=",x," is not defined in schema defined by MetadataDefintions")
            print("Copying to Metadata as a string as was stored in MongoDB")
            md.put_string(x,y)
        else:
          print('dict2md:  data with key=',x,' is unsupported type=',type(y))
          print('attribute will not be loaded')
  return md
