#!/usr/bin/env python3
import collections
import dask
from mspasspy.ccore.utility import AntelopePf, MsPASSError
import pandas as pd
import dask.dataframe as daskdf
import dask.bag as daskbg
import json

def parse_attribute_name_tbl(pf, tag="attributes"):
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
    names = []
    dtypes = []
    nullvalues = []

    i = 0
    for line in tbl:
        temp = line.split()
        names.append(temp[0])
        typenamein = temp[1].lower()  # allows upper or lower case in names
        if typenamein == "string" or typenamein == "str":
            typ = type("foobar")
            nullval = temp[2]
        elif typenamein == "integer" or typenamein == "int" or typenamein == "long":
            typ = type(1)
            nullval = int(temp[2])
        elif (
            typenamein == "float"
            or typenamein == "double"
            or typenamein == "real"
            or typenamein == "epochtime"
        ):
            typ = type(1.0)
            nullval = float(temp[2])
        elif typenamein == "bool" or typenamein == "boolean":
            typ = type(True)
            nullval = bool(temp[2])
        else:
            raise MsPASSError(
                "parse_attribute_name_tbl:  unsupported data type file=" + typenamein,
                "Fatal",
            )
        dtypes.append(typ)
        nullvalues.append(nullval)

        i += 1
        nulls = dict()
        i = 0
        for k in names:
            nulls[k] = nullvalues[i]
            i += 1

    return tuple([names, dtypes, nulls])


def textfile_to_df(
    filename,
    separator="\s+",
    type_dict=None,
    header_line=0,
    attribute_names=None,
    rename_attributes=None,
    attributes_to_use=None,
    null_values=None,
    one_to_one=True,
    parallel=False,
    insert_column=None,
):
    """
    Import a text file representation of a table and store its
    representation as a pandas dataframe.
    Note that even in the parallel environment, a dask dataframe will be
    transfered back to a pandas dataframe for the consistency.

    :param filename:  path to text file that is to be read to create the
      table object that is to be processed (internally we use pandas or
      dask dataframes)
    :param separator:   the delimiter used for seperating fields,
      for plaintext format file, its value should be space;
      for csv file, its value should be ','
    :param type_dict: pairs of each attribute and its type, usedd to validate
      the type of each input item
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
      defined in the null_vlaues python dict.  If == returns True, the 
      value will be set as None in dataframe. If your table has a lot of null
      fields this option can save space, but readers must not require the null 
      field.  The default is None which it taken to mean there are no null 
      fields defined.
    :param one_to_one: is an important boolean use to control if the
      output is or is not filtered by rows.  The default is True
      which means every tuple in the input file will create a single row in 
      dataframe. (Useful, for example, to construct an wf_miniseed
      collection css3.0 attributes.)  If False the (normally reduced) set
      of attributes defined by attributes_to_use will be filtered with the
      panda/dask dataframe drop_duplicates method.  That approach
      is important, for example, to filter things like Antelope "site" or
      "sitechan" attributes created by a join to something like wfdisc and
      saved as a text file to be processed by this function.
    :param parallel:  When true we use the dask dataframe operation.
      The default is false meaning the simpler, identical api panda
      operators are used.
    :param insert_column: a dictionary of new columns to add, and their value(s).
    If the content is a single value, it can be passedto define a constant value
    for the entire column of data. The content can also be a list, in that case,
    the list should contain values that are to be set, and it must be the same
    length as the number of tuples in the table.
    """

    if header_line < 0:
        if parallel:
            df = daskdf.read_csv(filename, sep=separator, names=attribute_names)
        else:
            df = pd.read_csv(filename, sep=separator, names=attribute_names)
    else:
        if parallel:
            if attribute_names is not None and len(attribute_names) > 0:
                df = daskdf.read_csv(
                    filename, sep=separator, names=attribute_names, header=header_line
                )
            else:
                df = daskdf.read_csv(filename, sep=separator, header=header_line)
        else:
            if attribute_names is not None and len(attribute_names) > 0:
                df = pd.read_csv(
                    filename, sep=separator, names=attribute_names, header=header_line
                )
            else:
                df = pd.read_csv(filename, sep=separator, header=header_line)

    #   Convert data in each column to the type given in type_dict
    if type_dict is not None:
        for field, type in type_dict.items():
            if field in df:
                df[field].astype(type)

    if attributes_to_use is not None:
        df = df[attributes_to_use]

    if not one_to_one:
        df = df.drop_duplicates()

    #   For those null values, set them to None
    if null_values is not None:
        for key, val in null_values.items():
            if key not in df:
                continue
            else:
                if parallel:
                    df = df.astype(object)
                    df[key] = df[key].mask(df[key] == val, None)
                else:
                    df[key] = df[key].apply(lambda a: None if (a == val) else a)  

    #   Intentionally left to last as the above can reduce the size of df
    if rename_attributes is not None:
        df = df.rename(columns=rename_attributes)

    #   Add new columns to the dataframe
    if insert_column is not None:
        for key, val in insert_column.items():
            df[key] = val
  
    if parallel:
        df = df.compute()

    return df


def df_to_mongo(db, collection, df, parallel, one_to_one=True):
    """
    Tansfer a dataframe into a set of documents, and store them
      in a specified collection. In one_to_one mode every row in the
      dataframe will be saved into the mongodb, otherwise duplicates
      would be discarded.
    :param db:  MongoDB database handle.  Normally a MsPASS Database,
      but the base class raw MongoDB handle can also work in this context.
    :param collection:  MongoDB collection name to be used to save the
      (often subsetted) tuples of filename as documents in this collection.
    :param one_to_one: a boolean to control if the set should be filtered by
      rows.  The default is True which means every row in the dataframe will
      create a single MongoDB document. If False the (normally reduced) set
      of attributes defined by attributes_to_use will be filtered with the
      panda/dask dataframe drop_duplicates method before converting the
      dataframe to documents and saving them to MongoDB.  That approach
      is important, for example, to filter things like Antelope "site" or
      "sitechan" attributes created by a join to something like wfdisc and
      saved as a text file to be processed by this function.
    :df: Pandas.Dataframe object, the input to be transfered into mongodb 
    documents
    :parallel:  a boolean that determine if dask api will be used for operation
    on the dataframe
    :param one_to_one: a boolean to control if the duplicate should be deleted
    :return:  integer count of number of documents added to collection
    """
    dbcol = db[collection]

    if not one_to_one:
        df = df.drop_duplicates()
    
    if parallel:
        df = daskdf.from_pandas(df, chunksize=1, sort=False)
        df.apply(lambda x: dbcol.insert_one(x.dropna().to_dict()), axis=1, meta=(None, 'object')).compute()
        return df.shape[0].compute()
    else:
        df = df.apply(pd.Series.dropna, axis=1)
        doc_list = df.to_dict(orient='records')
        if len(doc_list):
            dbcol.insert_many(doc_list)
        return len(doc_list)

def import_table(
    db,
    collection,
    filename,
    type_dict=None,
    separator = "\s+",
    header_line=0,
    attribute_names=None,
    rename_attributes=None,
    attributes_to_use=None,
    null_values=None,
    one_to_one=True,
    parallel=False,
    insert_column=None,
):
    """
    Import and parse a textfile into set of documents, and store them
    into a mongodb collection. This function consists of two steps:
    1. textfile_to_df: Convert the input textfile into a Pandas dataframe
    2. df_to_mongo: Insert the documents in that dataframe into a mongodb 
    collection
    The definition of the arguments can be found in these two functions.
    """
    df = textfile_to_df(
        filename=filename,
        separator=separator,
        type_dict=type_dict,
        header_line=header_line,
        attribute_names=attribute_names,
        rename_attributes=rename_attributes,
        attributes_to_use=attributes_to_use,
        null_values=null_values,
        one_to_one=one_to_one,
        parallel=parallel,
        insert_column=insert_column
    )
    return df_to_mongo(db, collection, df, parallel, one_to_one)
