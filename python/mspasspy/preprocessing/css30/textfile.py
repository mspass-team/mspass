#!/usr/bin/env python3
from mspasspy.ccore.utility import (AntelopePf,
                                    MsPASSError)
import pandas as pd
import dask.dataframe as daskdf



def parse_attribute_name_tbl(pf,tag='attributes'):
    """
    This function will parse a pf file to extract a tbl with a specific 
    key and return a data structure that defines the names and types of 
    each column in the input file.
    
    The structure returned is a tuple with three components:
        1 (index 0) python array of attribute names in the original tbl order
          This is used to parse the text file so the order matters a lot.
        2 (index 1) parallel array of type names for each attribute. 
          These are actual python type objects that can be used as the 
          second arg of isinstance.  
        3 (index 2) python dictionary keyed by name field that defines 
          what a null value is for this attribute.

    :param pf:  AntelopePf object to be parsed
    :param tag:  &Tbl tag for section of pf to be parsed.
   
    """
    tbl = pf.get_tbl(tag)
    names=[]
    dtypes=[]
    nullvalues=[]

    i=0
    for line in tbl:
        temp = line.split()
        names.append(temp[0])
        typenamein = temp[1].lower()  # allows upper or lower case in names
        if typenamein=='string' or typenamein=='str':
            typ = type('foobar')
            nullval = temp[2]
        elif typenamein=='integer' or typenamein=='int' or typenamein=='long':
            typ = type(1)
            nullval = int(temp[2])
        elif typenamein=='float' or typenamein=='double' or typenamein=='real' or typenamein=='epochtime':
            typ = type(1.0)
            nullval = float(temp[2])
        elif typenamein=='bool' or typenamein=='boolean':
            typ = type(True)
            nullval = bool(temp[2])
        else:
            raise MsPASSError('parse_attribute_name_tbl:  unsupported data type file='+typenamein,'Fatal')
        dtypes.append(typ)
        nullvalues.append(nullval)
        
        i += 1
        nulls=dict()
        i = 0
        for k in names:
            nulls[k] = nullvalues[i]
            i += 1

    return tuple([names,dtypes,nulls])

def import_table(db,collection,filename,
                format='unixtext',header_line=0,attribute_names=None,
                rename_attributes=None, attributes_to_use=None,
                null_values=None,
                one_to_one=True,parallel=False):
    """
    Import a text file representation of a table and store its 
    representation as a set of documents in a specified collection. 
    In one_to_one mode ever row in the input file will be saved 
    as a document in the specified collection with no tests for 
    duplications. That is feasible in MongoDB because the objectid 
    is used as the unique index.  When one_to_one is false 
    the panda (parallel set False) or dask (parallel set True) 
    drop_duplicates method is applied to the (optionally decimated)
    table before saving.  Use the attributes_to_use list in 
    combination with one_to_one=False to decimate a joined view
    to retrieve unique tuples duplicated in a join.  
    (e.g. for Antelope uses if you created a view by joining site to 
    wfdisc populate attributes_to_use with names like sta, lat, lon, elev
    ondate, and offdate and run this function with one_to_one set False.)
    
    :param db:  MongoDB database handle.  Normally a MsPASS Database, 
      but the base class raw MongoDB handle can also work in this context.
    :param collection:  MongoDB collection name to be used to save the 
      (often subsetted) tuples of filename as documents in this collection. 
    :param filename:  path to text file that is to be read to create the 
      table object that is to be processed (internally we use pandas or 
      dask dataframes)
    :param format:  expected format of the text in this file. Current 
      supported options are:
          'unixtext' (default) is a text file with the standard unix 
            white space definition between tokens.   This format would 
            be the norm, for example, from an Antelope raw table or view
            IF you exclude comment attributes that may contain spaces
        'csv' - standard csv table representation.  
    :param attribute_names: This argument must be either a list of (unique)  
      string names to define the attribute name tags for each column of the 
      input table.   The length of the array must match the number of 
      columns in the input table or this function will throw a MsPASSError 
      exception.   This argument is None by default which means the 
      function will assume the line specified by the "header_line" argument as 
      column headers defining the attribute name.  If header_line is less 
      than 0 this argument will be required.  When header_line is >= 0 
      and this argument (attribute_names) is defined all the names in 
      this list will override those stored in the file at the specified 
      line number.  
    :param  rename_attributes:   This is expected to be a python dict 
      keyed by names matching those defined in the file or attribute_names 
      array (i.e. the panda/dataframe column index names) and values defining 
      strings to use to override the original names.   That usage, of course, 
      is most common to override names in a file.  If you want to change all 
      the name use a custom attributes_name array as noted above.  This 
      argument is mostly to rename a small number of anomalous names. 
    :param attributes_to_use:  If used this argument must define a list of 
      attribute names that define the subset of the dataframe dataframe 
      attributes that are to be saved.  For relational db users this is 
      effectively a "select" list of attribute names.  The default is 
      None which is taken to mean no selection is to be done. 
    :param null_values:  is an optional dict defining null field values. 
      When used an == test is applied to each attribute with a key 
      defined in the null_vlaues python dict.  If == returns True 
      the field is not copied to the MongoDB doc saved for that tuple. 
      If your table has a lot of null fields this option can save 
      space, but readers must not require the null field.  The default 
      is None which it taken to mean there are no null fields defined. 
    :param one_to_one: is an important boolean use to control if the 
      output is or is not filtered by rows.  The default is True 
      which means every tuple in the input file will create a single 
      MongoDB document.  (Useful, for example, to construct an wf_miniseed 
      collection css3.0 attributes.)  If False the (normally reduced) set 
      of attributes defined by attributes_to_use will be filtered with the 
      panda/dask dataframe drop_duplicates method before converting the 
      dataframe to documents and saving them to MongoDB.  That approach 
      is important, for example, to filter things like Antelope "site" or
      "sitechan" attributes created by a join to something like wfdisc and 
      saved as a text file to be processed by this function. 
    :param parallel:  When true we use the dask dataframe operation.  
      The default is false meaning the simpler, identical api panda 
      operators are used. 
    :return:  integer count of number of documents added to collection
    """
    dbcol = db[collection]
    # Because of the flexibility of read_csv defining the separator is 
    # enough for the two currently supported formats.  If other added 
    # may need some changes here
    if format=='unixtext':
        separator='\s+'
    elif format=='csv':
        separator=','
    else:
        raise MsPASSError("import_table:  do not know how to handle format="+format,'Fatal')
    # We use the regular pandas dataframe in serial and dask when running 
    # parallel.   We only have to do this for the creation with read_csv 
    # because of the parallel api for dask dataframes. That approach will  
    # probably break if we want to run this with spark 
    if header_line<0:
        if parallel:
            df = daskdf.read_csv(filename, sep=separator, names=attribute_names)
        else:
            df = pd.read_csv(filename, sep=separator, names=attribute_names)
    else:
        if parallel:
            if len(attribute_names)>0:
                df = daskdf.read_csv(filename, sep=separator, names=attribute_names,header=header_line)
            else:
                df = daskdf.read_csv(filename, sep=separator, header=header_line)
        else:
            if len(attribute_names)>0:
                df = pd.read_csv(filename, sep=separator, names=attribute_names,header=header_line)
            else:
                df = pd.read_csv(filename, sep=separator, header=header_line)

    if attributes_to_use != None:
        df = df[attributes_to_use]
    if not one_to_one:
        df = df.drop_duplicates()
    # Intentionally left to last as the above can reduce the size of df
    if rename_attributes != None:
        df = df.rename(columns=rename_attributes)
    df_column_names = list(df.columns)
    n = 0
    for index, row in df.iterrows():
        doc=dict()
        for key in df_column_names:
            val = row[key]
            if key in null_values:
                if val != null_values[key]:
                    doc[key] = val
            else:
                doc[key] = val
        dbcol.insert_one(doc)
        #print(json_util.dumps(doc,indent=2))
        n += 1
    if parallel:
        df.compute()
    return n
    
