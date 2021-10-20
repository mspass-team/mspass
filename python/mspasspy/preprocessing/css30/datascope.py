#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains an AntelopeDatabase class that can be used to 
interaction with an Antelope(Datascope) flat file database.   It is not 
intended to be a fully functional database handle.  It is largely a 
translator that takes the flat files used to hold the data in an Antelope 
database and converts them to dask or panda dataframes.   Because a dataframe
has methods that implement some relational database concepts (e.g. sort and join)
the idea is subsequent code can manipulate dataframes to assembled 
sorted, joined, and/or grouped dataframes that can be used to drive 
waveform processing in MsPASS.   That is, this object is intended as a 
handle that can be used to build a dataframe that can be used to 
drive a workflow with each tuple linked to one waveform object. 

In current form the handle is fairly limited.   It uses a parameter file 
to define attributes in tables.  For any table defined in the pf file 
the get_table method can be used to retrieve a dataframe representation 
of that table.   The only other real functionality is join.  The join 
method uses pandas/dask merge and works only with exact string key matches.
It may be possible to do time interval matches as used in Antelope's 
database, but I left this behind as a low priority.

If you have a complicated database view build from multiple tables using 
more complicated join operations, I recommend using Antelope or some other 
database engine (relational or not) to construct such a view and saving 
the result to a white space delimited text file or a csv file.  Then 
you can use the import_table function to convert that (potentially large) 
file to a dataframe.  

Created on Mon Oct 18 05:29:42 2021

@author: pavlis
"""
import os
from mspasspy.ccore.utility import (AntelopePf,
                                    MsPASSError)
import pandas as pd
import dask.dataframe as dd
from mspasspy.preprocessing.css30.textfile import parse_attribute_name_tbl


class AntelopeDatabase:
    def __init__(self,dbname,pffile='master',parallel=True):
        """
        Constructor for this class.
        
        :param dbname:  dbname is the root name for the 
        flat files used by antelope.  (e.g. if we had a file usarray.wfdisc, 
        the dbname would be usarray and wfdisc is a table name).  
        
        :param pffile:  parameter file name used to create the data structure 
        used internally by this class.   If set to 'master' (the default) 
        the constructor looks for a master file called AntelopeDatabase.pf 
        that it expects to find in $MsPASS_HOME/data/pf.  
        
        :param parallel:  if True (default) the handle expects to be 
        interacting with and creating dask dataframes.  If set False 
        it will do everything with pandas.  Note to switch between modes 
        just set self.parallel.   dask dataframes and pandas are supposed to 
        be compatible, but be conscious of which is left and which is right 
        in a join.  Documentation claims a small table as a pandas joining to 
        a large table is very efficient. 

        """
        self.dbname = dbname
        if pffile == 'master':
            home = os.getenv('MSPASS_HOME')
            if home == None:
                raise MsPASSError('AntelopeDatabase constructor: '
                    + 'MSPASS_HOME not defined.  Needed for default constructor\n'
                    + 'Specify a full path for pf file name or set MsPASS_HOME',
                    'Fatal')
            else:
                path = os.path.join(home,'data/pf','AntelopeDatabase.pf')
        else:
            path = pffile
        self.pf = AntelopePf(path)
        self.parallel = parallel
        
    def get_table(self,table='wfdisc',
                    rename_attributes=None, 
                    attributes_to_use=None,
                    attributes_to_drop=None):
        """
        This method converts a specified table to a dataframe.   
        
        :param rename_attributes:  optional python dictionary used to 
        change the names of one or more columns of the output dataframe.  
        This argument is passed directly to the dataframe rename method.  
        Default is None which cause the rename call to be bypassed.
        
        :param attributes_to_use:  optional python list of attribute 
        names to extract from the larger table.  If used only the attributes
        defined in the list will be returned in the dataframe. Default is 
        None which cause all attributes in the table to be returned.  
        
        :param attributes_to_drop:  list of attribute names to be 
        removed from the dataframe before returning.   This list is
        sort of a complement to attributes_to_use.  Use one or the 
        other depending, usually, on which is smaller. 
        
        :return:  dataframe representation of requested table with optional 
        edits applied.  A pandas dataframe if parallel is false, but a 
        dask dataframe if parallel is true.
        
        Note although the condition is not trapped it is intrinsically 
        dangerous to define both attributes_to_use and attributes_to_drop.  
        Probably ok if you recognize that attributes_to_use is tested 
        first and applied if the list is not empty.
        """
        tblret = self._parse_attribute_name_tbl(table)
        # this will be an array of the keys in order
        names = tblret[0]
        # this is a python dictionary keyed by names strings of null values
        # for each attribute 
        nullvals = tblret[2]
        # Antelope specific syntax.  The flat files in antelope are 
        # constructed in the form dbname.table.  We construct that file 
        # name here.  Note support for antelope's alias mechanism to 
        # put tables in multiple directories is not supported here.
        # if someone else needs that they can write it - it will be tedious.
        filename = self.dbname +'.'+table
        if self.parallel:
            df = dd.read_csv(filename,sep='\s+',names=names)
        else:
            df = pd.read_csv(filename,sep='\s+',names=names)
        if rename_attributes != None:
            df = df.rename(columns=rename_attributes)
        if attributes_to_use != None:
            df = df[attributes_to_use]
        if attributes_to_drop != None:
            df = df.drop(attributes_to_drop,axis=1)
        return df
    
    def get_keys(self,table):
        """
        Returns a list of the primary keys defined for this table 
        in the parameter file used to create this handle.   These 
        keys are used to produce a form of "natural join" in the 
        join method.   
        """
        pf = self.pf.get_branch(table)
        tbl = pf.get_tbl('primary_keys')
        # This is necessary to convert a C++ std::list defined by pybind11 
        # to a python list 
        result = list()
        for k in tbl:
            result.append(k)
        return result
    def join(self,df_left,table,join_keys='right_primary',right_suffix=None,
             how='right'):
        """
        This method provides a limited join functionality between an 
        input dataframe and a specified table.  It works with multiple 
        join keys but the only matches supported are exact matches using 
        string or integer keys.  It is really just a front end on 
        dataframe merge that loads and joins a named table to the 
        input dataframe passed through arg 0 (df_left):
            
        :param df_left:  dataframe to which table (arg1) is to be joined. 
        
        :param table:  antelope database table name to be joined with df_left.
        
        :param join_keys:  If not specified the keys returned by the 
        get_keys method will be used for left and right tables in the join. 
        Can be a list of attribute names and if so left and right join 
        keys will be the same.  Can also be a python dictionary.  Use 
        that form if you want to use different keys for the left and 
        right tables in the join.  The keys of the dict are used for 
        the left and the values are used for the right. 
        
        :param right_suffix:  Duplicate attribute names in a merge 
        need a way to be identified. Default uses the table name with 
        a leading underscore. (e.g. joining site would produce an lddate_site 
        attribute in the output dataframe).   If any other string is used 
        it is passed directly to the dataframe merge method and you will 
        get different names for ambiguous column names.  
        
        :param how:  defines the type of join operation to use.  
        Passed directly to the merge dataframe method.  See documentation 
        for dataframe merge to see options allowed for how.
        
        :return:  dataframe resulting from the join
        """
        if right_suffix==None:
            right_suffix = '_'+table
        df_right = self.get_table(table)
        if join_keys == 'right_primary':
            right_jkeys = self.get_keys(table)
            left_jkeys=right_jkeys
        elif isinstance(join_keys,list):
            right_jkeys=join_keys
            left_jkeys=join_keys
        elif isinstance(join_keys,dict):
            right_jkeys=[]
            left_jkeys=[]
            for k in join_keys:
                left_jkeys.append(k)
                right_jkeys.append(join_keys[k])
        else:
            raise MsPASSError('AntelopeDatabase.join:  Illegal type='
               + str(type(join_keys))
               +' for join_key arg.\nMust be string, list, or dict',
               'Fatal')
        # merge allows variations for left, right, inner, outter, and cross 
        # for the how clause.  Default users the merge default of 'inner'
        df = df_left.merge(df_right,how=how,
                           left_on=left_jkeys,
                           right_on=right_jkeys,
                           suffixes=('',right_suffix))

        # online sources suggest merge can clobber the index for the
        # merged dataframe but I don't think that matters for this 
        # intended application - beware though
        return df
    
    def _parse_attribute_name_tbl(self,table):
        """
        This function is a variant of a function in MsPASS with the same 
        name without the leading underscore.  In fact, the algorithm is 
        basically one that first extracts the branch name of the specified 
        table and then call the parse_attribute_name procedure.  One key 
        difference is the tbl tag here is frozen as "attributes"
    
        The structure returned is a tuple with three components returned by 
        parse_attributes_name_tbl. See docstring for that function.
        """

        pf = self.pf.get_branch
        pf = self.pf.get_branch(table)
        return parse_attribute_name_tbl(pf,tag='attributes')            
