#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a DatascopeDatabase class that can be used to 
interaction with an Antelope(Datascope) flat file database.   It is not 
intended to be a fully functional database handle.  I can, however, 
bu used as a base class to add additional functionality.

The main point of this implementation is a mean to translate Antelope 
tables that can be used for related seismic data processing.   The 
two main known uses are:  (1) translating a table to a MongoDB 
collection and (2) loadng a small table to create a normalizing 
operator via a subclass of the generic :class:`mspasspy.db.normalize.BasicMatcher`
class.  Although not currently implementd it could also, for example, 
but used to create a reader driven by a wfdisc table.  

The "translation" model this class uses is to create a DataFrame 
image of the a single table or what Antelope developers call a 
database view.  The class only supports pandas DataFrame assuming that 
tables are small enough to fit in memory.   An extremely good assumption 
since Datascope uses that model.  Note, however, that if one needs to 
create a giant table from many fragmented databases as is common with 
large network operations with Antelope, the  dask DataFrame `merge`
method can be used to combine a string of multiple, common tables. 

The inverse of writing a DataFrame to an Datascope table is also 
supported via the `df2table` method.   The forward an inverse 
translations can be used as the basis for workflows that 
utilize both MsPASS an Antelope.   e.g. if you have a license 
for Antelope you could use their database-driven event detection 
and association algorithms to create a catalog and use MsPASS 
for waveform processing that utilizes the catalog data.   

The class constructor uses an Antelope pf file to define the 
tables it knows how to parse.  A master pf for most tables is 
distributed with mspass.   To parse uncommon tables not defined in 
the master pf file you will need a license for antelope to run the 
python script found in Antelope contrib and in mspass called 
TO DO NEED TO BUILD THIS AN DUPDATE THIS DOCSTTRNG.
It is VERY IMPORTANT to realize that if you build that pf by hand 
you must list attributes in the "attributes Tbl" in the 
left to right table order of the Datascope schema definition.  
Readers will work if you violate that rule, but the writer will 
scramble the output if you do and the result will almost certainly 
by unreadable by Datascope.   


@author: Gary L. Pavlis
"""
import os
from mspasspy.ccore.utility import AntelopePf, MsPASSError, ErrorSeverity
import pandas as pd
import numpy as np


class DatascopeDatabase:
    def __init__(self, dbname, pffile=None):
        """
        Constructor for this class.  It parses either the master
        file describing the CSS3.0 schema or the (optional) alternative
        defined with the pffile argument.  The schema properties are loaded
        into memory and stored as an internal data structure used to
        efficiently deal with the ugly formatting issues. The
        database name, which is passed by the arg dbname, is not used until
        one of the methods is called.  Only the name is stored in the
        instance of the object this constructor creates.   Hence, you
        won't know if the reader works until you try reading a table
        with the `get_table` method,.

        :param dbname:  dbname is the root name for the
        flat files used by antelope.  (e.g. if we had a file usarray.wfdisc,
        the dbname would be usarray and wfdisc is a table name).

        :param pffile:  parameter file name used to create the data structure
        used internally by this class.   If None (the default)
        the constructor looks for a master file called DatascopeDatabase.pf
        that it will attempt to read from $MsPASS_HOME/data/pf.

        """
        self.dbname = dbname
        if pffile == "master":
            home = os.getenv("MSPASS_HOME")
            if home == None:
                raise MsPASSError(
                    "AntelopeDatabase constructor: "
                    + "MSPASS_HOME not defined.  Needed for default constructor\n"
                    + "Specify a full path for pf file name or set MsPASS_HOME",
                    "Fatal",
                )
            else:
                path = os.path.join(home, "data/pf", "AntelopeDatabase.pf")
        else:
            path = pffile
        self.pf = AntelopePf(path)

    def get_table(
        self,
        table="wfdisc",
        attributes_to_use=None,
    ) -> pd.DataFrame:
        """
        This method converts a specified table to a dataframe.   Default
        will load all attributes of the specified table.  The equivalent of
        an SQL select clause can be done by passing a list of attribute
        names through the `attributes_to_use` argument.  Note that list
        does not need to be in the same order as the attributes are stored
        in the Datascope text files from which the requested data will be
        retrieved.

        Note if you want to change the names of the attributes of the
        DataFrame this method returns, use the pandas `rename` method
        on the return.   See numerous online sources for use of
        the `rename` method.

        :param rename_attributes:  optional python dictionary used to
        change the names of one or more columns of the output dataframe.
        This argument is passed directly to the dataframe rename method.
        Default is None which cause the rename call to be bypassed.

        :param attributes_to_use:  optional python list of attribute
        names to extract from the larger table.  If used only the attributes
        defined in the list will be returned in the dataframe. Default is
        None which cause all attributes in the table to be returned.

        :return:  DataFrame representation of requested table with optional
        edits applied.

        """

        cols, alldtypes, nullvalues, fmt = self._parse_attribute_name_tbl(table)
        if attributes_to_use:
            load_list = attributes_to_use
        else:
            # Don't use the keys of the returned dictionaries as they
            # won't be in table order.  More efficient, i assume, to read
            # the file wthout scrampling the columns
            temp = self._get_line_format_pf(table)
            load_list = list()
            for t in temp:
                load_list.append(t[0])
        colspecs = []
        dtypes = dict()
        for key in load_list:
            colspecs.append(cols[key])
            dtypes[key] = alldtypes[key]

        # Antelope specific syntax.  The flat files in antelope are
        # constructed in the form dbname.table.  We construct that file
        # name here.  Note support for antelope's alias mechanism to
        # put tables in multiple directories is not supported here.
        # if someone else needs that they can write it - it will be tedious.
        filename = self.dbname + "." + table
        df = pd.read_fwf(filename, colspecs=colspecs, names=load_list, dtype=dtypes)

        return df

    def get_primary_keys(self, table) -> list:
        """
        Returns a list of the primary keys defined for this table
        in the parameter file used to create this handle.   These
        keys are used to produce a form of "natural join" in the
        join method.
        """
        pf = self.pf.get_branch(table)
        tbl = pf.get_tbl("primary_keys")
        # This is necessary to convert a C++ std::list defined by pybind11
        # to a python list
        result = list()
        for k in tbl:
            result.append(k)
        return result

    def get_nulls(self, table) -> dict:
        """
        Retrieve dictionary of null values for attributes in a table.

        Unlike MongoDB a relational database requires a Null value to
        define when a table cell is not defined.   This method
        returns the Null value defined for Datascope tables in their
        schema.   This method returns all the null values for a
        table in a python dictionary keyed by the attibute
        names defined for the schema for that table. That algorithm
        is used instead of a function retrievng a value given table
        and attribute as arguments for two reasons.  (1) Most
        conceivale applications would need most if not all of the
        Null values to use the data anyway and (2) the difference in
        time to fetch one value versus all is near zero because a
        linear search through a list is required to fetch a single
        value.

        :param table:  name of the table for which Null values is to
        be retrieved.
        :type table:  string
        :return:  python dictionary key by attribute names with Nulls
        as the value associated with that attribute.
        """
        allproperties = self._parse_attribute_name_tbl(table)
        return allproperties[2]

    def join(
        self, df_left, table, join_keys="right_primary", right_suffix=None, how="right"
    ):
        """
        This method provides a limited join functionality between an
        input dataframe and a specified table.  It works with multiple
        join keys but the only matches supported are exact matches using
        string or integer keys.  It is really just a front end on
        dataframe merge that loads and joins a named table to the
        input dataframe passed through arg 0 (df_left).   If more
        sophisticated join operators are needed there are two options:
            1.  Do the join operator externally in Datascaope,
                save the result as a Datascope view in a csv format, and then
                inport the csv file with the pandas read_csv method.
            2.  Save the larger of the two tables you want to join to
                MongoDB and use one of the generic matchers that are
                subclasses of :class:`mspasspy.db.normalize.DataFrameCacheMatcher`.
                If the matching operation you need is not already defined you
                may need to develop a custom subclass of that matcher.

        :param df_left:  dataframe to which table (arg1) is to be joined.

        :param table:  antelope database table name to be joined with df_left.

        :param join_keys:  If not specified the keys returned by the
        get_primary_keys method will be used for left and right tables in the join.
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
        if right_suffix == None:
            right_suffix = "_" + table
        df_right = self.get_table(table)
        if join_keys == "right_primary":
            right_jkeys = self.get_keys(table)
            left_jkeys = right_jkeys
        elif isinstance(join_keys, list):
            right_jkeys = join_keys
            left_jkeys = join_keys
        elif isinstance(join_keys, dict):
            right_jkeys = []
            left_jkeys = []
            for k in join_keys:
                left_jkeys.append(k)
                right_jkeys.append(join_keys[k])
        else:
            raise MsPASSError(
                "AntelopeDatabase.join:  Illegal type="
                + str(type(join_keys))
                + " for join_key arg.\nMust be string, list, or dict",
                "Fatal",
            )
        # merge allows variations for left, right, inner, outter, and cross
        # for the how clause.  Default users the merge default of 'inner'
        df = df_left.merge(
            df_right,
            how=how,
            left_on=left_jkeys,
            right_on=right_jkeys,
            suffixes=("", right_suffix),
        )

        # online sources suggest merge can clobber the index for the
        # merged dataframe but I don't think that matters for this
        # intended application - beware though
        return df

    def df2table(self, df, db, table, dir=None, append=True) -> pd.DataFrame:
        """
        Inverse of get_table method.   Writes contents of DataFrame
        `df` to Datascope table inferred from the `table` argument.
        That is, the method attempts to write the contents of df to
        a file "db.table" with an optional diretory (dir argument).
        It will immediately throw an exception if any of the df column
        keys do not match an attribute name for the schema defined for
        the specified table.  Missing keys will be written as the
        null value defined for the schema using the pf file loaded
        with the class constructor.

        :param df: pandas DataFrame containing data to be written.
        :type df:  pandas DataFrame
        :param db:  output database root name
        :type db: string
        :param table:  Datascope table to which the data should be
        written.
        :type table:  string
        :param dir: optional director name where the table data should be
        saved.   Default is None which is taken to mean the current director.
        If the directory does not exist it will be created.
        :type:  string or None
        :param append: boolean that when set causes the data to be appended
        to a file if it already exist.  Default is True.  When set False
        if the file exists it will be overwritten.

        :return:  possibly edited copy of input dataframe with null
        values inserted and columns rearrange to match Datascope table
        order,
        """
        if dir:
            outdir = dir
            # make sure dir ends with a / for this way we create path here
            n = len(outdir)
            if outdir[n - 1] != "/":
                outdir += "/"
        else:
            outdir = "./"
        fmtlist = self._get_line_format_pf(table)
        # Datascope always adds a space between attributes
        # this creates the format string for numpy's savetxt
        # function that way because the format string is as in C
        fmt = str()
        keys = list()  # used to define required output order
        n = len(fmtlist)
        for i in range(len(fmtlist)):
            fmt += fmtlist[i][1]  # fmtlist is a list of lists - returns str
            # drop the blank from last entry or line length gets botched
            if i < n - 1:
                fmt += " "
            keys.append(fmtlist[i][0])
            i += 1
        # only rearrange columns if necessary - an expensive operation
        dfkeys = df.columns
        need_to_rearrange = False
        if len(keys) == len(dfkeys):
            for i in range(len(keys)):
                if keys[i] != dfkeys[i]:
                    need_to_rearrange = True
                    break
        else:
            need_to_rearrange = True

        if need_to_rearrange:
            # note we can use the keys list as is for input to dataframe's
            # reindex method.  However, to do that we have to add nulls
            # for any dfkeys that don't have values for an attribute defines
            # in keys.  First, however, we have to delete any dfkey
            # columns not define in keys
            for k in dfkeys:
                dropped_keys = list()
                if k not in keys:
                    dropped_keys.append(k)
                if len(dropped_keys) > 0:
                    # intentionally do not throw an exception here but
                    # just post a warning because this method is expected
                    # to only be run interactively
                    message = "Warning:   The following attributes in the "
                    message += (
                        "input DataFrame are not defined in the schema for table "
                    )
                    message += table + "\n"
                    for k in dropped_keys:
                        message += k + " "
                    print(message)
                    dfout = df.drop(axis=1, labels=dropped_keys)
                else:
                    # we need this copy if we don't have any key issues
                    dfout = pd.DataFrame(df)
            # since they could change we have to reset this list
            dfkeys = dfout.columns
            # now get alist of missing attributes and add them using
            # the null value for that attribute defined by the table schema
            null_columns = list()
            for k in keys:
                if k not in dfkeys:
                    null_columns.append(k)
            if len(null_columns) > 0:
                attributes = self._parse_attribute_name_tbl(table)
                nulls = attributes[2]
                for k in null_columns:
                    nullvalue = nulls[k]
                    # a bit obscure python syntax to full array with null values and
                    # insert in one line
                    dfout[k] = pd.Series([nullvalue for x in range(len(dfout.index))])

            # Now we rearrange - simple with reindex method of pandas
            dfout = dfout.reindex(columns=keys)
        else:
            # in this case we just set the symbol and don't even cpy it
            dfout = df
        fname = outdir + db + "." + table
        if append:
            mode = "a"
        else:
            mode = "w"
        with open(fname, mode) as ofile:
            np.savetxt(ofile, dfout.values, fmt=fmt)
        return dfout

    def _parse_attribute_name_tbl(self, table) -> tuple:
        """
        Private method to parse special parameter file format for defining
        Antelope flat file table data for reading.   The algorithm parses the
        parameter file image defied by the constructor (self.pf) returning
        a tuple that contains data that pandas (or dask) read_fwf can use
        to load the content of the specified table.   The return is
        a tuple with three dictionaries keyed by the attribute names
        defined for table.   The order is:
            0 - column specification tuples that can be passed to
                read_fwf (note read_fwf does not reuire these to be sequential)
                list of tuples with column range
            1 - python type expected for the attribute (supports only
                float int, bool, and string as intent is to only use this
                on table not antelope views).  dict keyed by attribute name
            3 - null values dictionary.  key is attribute name
            4 - format string dictionary keyed by attribute name

        """

        pf = self.pf.get_branch(table)
        # magic name for this pf format
        tbl = pf.get_tbl("attributes")
        cols = dict()
        dtypes = dict()
        nullvalues = dict()
        fmt = dict()

        i = 0
        for line in tbl:
            # assumed order of items in each tbl line is:
            # name, type, first_column, eidth, null_value
            temp = line.split()
            name = temp[0]
            colwidth = int(temp[1])
            colstart = int(temp[2])
            typenamein = temp[3].lower()  # allows upper or lower case in names
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
                    "parse_attribute_name_tbl:  unsupported data type file="
                    + typenamein,
                    "Fatal",
                )
            dtypes[name] = typ
            # this works becasue typ is now a python "type" class which
            # allows type casting like this via its constructor
            nullvalues[name] = typ(temp[4])
            fmt[name] = temp[4]
            cols[name] = [colstart, colstart + colwidth]
        return tuple([cols, dtypes, nullvalues, fmt])

    def _get_line_format_pf(self, table) -> list:
        pf = self.pf.get_branch(table)
        # magic name for this pf format
        tbl = pf.get_tbl("attributes")
        attribute_list = list()
        for line in tbl:
            temp = line.split()
            # tuples are name and format string
            # IMPORTANT ASSUMPTION:  pf items are in table order
            attribute_list.append([temp[0], temp[5]])
        return attribute_list

    def CSS30Catalog2df(self) -> pd.DataFrame:
        """
        Forms the standard catalog view of CSS3.0 sans the orid==prefor
        condition to return a DataFrame formed by using
        the DatascopeDatabase join method in sequence to produce:
            event->origin->assoc->arrival
        noting the the assoc->arrival join is done via arid.
        The returned dataframe will have some attributes like the
        "time" attributes of `arrival` and `origin` altered to
        clarify which is which using the pandas stock method of
        appending a suffix.  The suffix is the parent table name.
        Hence, for the "time" example the output will have columns
        with the keys `time_origin` and `time_arrival`.

        Because this method is expected to normally be run interactively
        it will throw exceptions for a whole range of problems
        for which the authors of mspass have no control.
        In the usual python way the posted exception stack should
        define the problem.  Exceptions could come from methods of
        this class called by the function or pandas.

        :return:  Pandad DataFrame with the standard css3.0 catalog view.
        Attribute names are css3.0 names.

        """

        df = self.get_table("event")
        df = self.join(df, "origin", join_keys=["evid"])
        df = self.join(df, "assoc", join_keys=["orid"])
        df = self.join(df, "arrival", join_keys=["arid"])

        return df

    def wfdisc2doclist(
        self,
        snetsta_xref=None,
        default_net="XX",
        test_existence=False,
        verbose=True,
    ) -> list:
        """
        Special function to convert a wfdisc table to a list of docs that can
        be written to MongoDB.  Alternatively the output can be passed directly to
        :py:func:`mspasspy.io.distributed.read_distributed_data`
        to initiate a parallel workflow or passed as a constructor to
        create a parallel container (i.e bag or RDD) to passed to
        the :py:meth:`mspasspy.db.database.read_data` method.
        This method ONLY works for wfdiscs that index
        a collection of miniseed files.  It will drop any data not
        marked as miniseed.  The conversions process is not trivial
        for several reasons:

        1.  We need to add some computed attributes to match the attributes
            required in wf_miniseed in the mspass reader.
        2.  Handling Null values.
        3.  Filtering out rows not defining miniseed data.
        4.  the net code mismatch with css3.0 (see below)
        5.  The equally obnoxious loc code problem

        1. simply involves always posting some constants.
        2.  is handled by silently deleting any attribute defined by the
        Null value for that attribute in the wfdisc schema.
        3. is handled by dropping any tuples for which the "datatype"
        attribute on "sd" (normal mseed) or "S1" miniseed in an older
        compression format.   When verbose is set true any entries
        dropped will geneate a warning print mesage.

        Items 4 and 5 are a bigger complication.   The developers of
        Antelope created a solution to this problem by utilizing
        two tables called `snetsta` and `schanloc`.   The trick they
        used was whenever a station name was not unique, they
        create an alias merging the net and sta codes with a fixed
        separator of "_".  e.g. if their miniseed indexing program
        detected station "HELL" with net codes "66" and "33"  it
        would create two aliases called "66_HELL" and "77_HELL".
        The data defining that alias is stored in the "snetsta"
        table.   The problem is unique sta code values are not
        change to the merged net_sta form but only the sta name
        is used in processing.  This code deals with this issue
        assuming the "snetsta" table exists and defines the
        net code for every station it finds.  When it encounters
        names like "66_HELL" it automatically drops the name
        in wfdisc and sets the net and sta codes using the
        cross-reference defined in snetsta.   To handle the
        very common situation where snetsta is not complete
        (i.e. there are sta codes without an snetsta entry)
        when a wfdisc sta key has not entry in snetsta the value of
        the `default_net` optional argument is used to define the
        net name in the output.   The way Amtelope loc is easier to
        deal with because of a dogmatic naming convention for FDSN.
        That is, valid SEED channel codes are required to be 3
        characters in length and follow a rigid definition of
        what each of the 3 character imply about the data.  Antelope
        handles loc codes, which were not conceived as needed when
        the CSS3.0 schema was designed, by simply appending the
        loc code to the channel code to produce an extended channel
        code tag. (e.g. channel BHZ with loc code 00 would be set to BHZ_00)
        Antelope defines a schanloc table that is similar to snetsta but
        we don't actually need to use it in this method because the syntax
        rules make splitting compound channel names unambiguous.   Hence,
        a reference to schanloc is not needed as it is to handle the
        net-sta problem.

        The net-sta problem is handled by the argument snetsta_xref
        argument.   If the database this object references has a
        complete snetsta table you can use the default and it will
        load and utilize the snetsta channel data to sort out
        net-sta ambiguities.  Unless you are 100% certain your
        snetsta table has no missing entries (wfdisc sta values not in snetsta)
        you should be sure to set the `default_net` optional argument value
        to what is appropriate for your data set.

        :param snetsta_xref:   image of the snetsta table used as
        described above.  If set None (the default) this method will
        call another method of this class called `parse_snetsa` that
        reads the snetsta table and creates an instance of the
        data structure.   See the docstring of that method below for
        an explanation of the data structure of this object if you need to
        generate one by some other means.
        :type snetsta_xref:  python dict or None
        :param test_existence:   Boolean that when set
        True (default is False) enables a file existence check.
        This operation is expensive on a large wfdisc as it has to
        run an existence check on every tuple in the wfdisc.
        :param verbose:  When True prints a warning for each tuple
        it drops.  If False it will drop problem tuples silently.  Note
        tuples can be dropped for two reasons:  (1) `datatype` values that
        do not define miniseed and (2) tuples failing the existence
        check (if enabled)
        """
        alg = "DatascopeDatabase.wfdisc2doclist"
        if verbose:
            base_warning = "DatascopeDatabase.wfdisc2doclist (WARNING):  "
        if snetsta_xref is None:
            snetsta_xref = self.parse_snetsta()
        if isinstance(snetsta_xref, dict):
            nets = dict()
            seedsta = dict()
            for k in snetsta_xref:
                nets[k] = snetsta_xref[k][0]
                seedsta[k] = snetsta_xref[k][1]
        else:
            message = "snetsta_xref parameter is invalid type={}\n".format(
                type(snetsta_xref)
            )
            message += (
                "Expected to be a python dictionary created by parse_snetsta method"
            )
            raise MsPASSError(alg, message, ErrorSeverity.Fatal)
        nets["default"] = default_net

        df = self.get_table("wfdisc")
        nulls = self.get_nulls("wfdisc")
        keys = df.columns
        olist = list()
        # loop over the tuples
        count = 0
        for i in df.index:
            doc = dict()
            for k in keys:
                x = df[k][i]
                nval = nulls[k]
                # this drops null values
                if x != nval:
                    doc[k] = x
            # drop data not defined as mseed
            # print a warning in verbose mode
            # we let this throw an exception if
            # datatype is set Null as that should not happen if wfdisc
            # is intact
            if doc["datatype"] in ["sd", "S1"]:
                save_me = True
                if test_existence:
                    path = doc["dir"] + "/" + doc["dfile"]
                    if os.path.isfile(path):
                        save_me = True
                    else:
                        save_me = False
                        if verbose:
                            # weirdness needed because we can't just assign
                            # message=base_warning or base_warning will be altered
                            message = "" + base_warning
                            message += "file {} not found - this tuple dropped".format(
                                path
                            )
                            print(message)
                if save_me:
                    # these are requirements for wf_miniseed
                    doc["storage_mode"] = "file"
                    doc["time_standard"] = "UTC"
                    doc["format"] = "mseed"
                    # handle net-sta issue
                    wfsta = doc["sta"]
                    if wfsta in seedsta:
                        sta = seedsta[wfsta]
                        net = nets[wfsta]
                    else:
                        sta = wfsta
                        net = default_net
                    doc["sta"] = sta
                    doc["net"] = net
                    # now handle loc issue
                    # note we do nothing unless the code has an "_"
                    chan = doc["chan"]
                    if "_" in chan:
                        chanloc = chan.split("_")
                        doc["chan"] = chanloc[0]
                        doc["loc"] = chanloc[1]
                    olist.append(doc)

            elif verbose:
                message = "" + base_warning
                message += "datatype attribute is {} in tuple {}.  ".format(
                    doc["datatype"], i
                )
                message += "Only sd or S1 define miniseed data"
                print(message)

        return olist

    def parse_snetsta(self) -> dict:
        """
        Datascope is (mostly) linked to the CSS3.0 schema originally
        developed ine 1970s before SEED was adopted as a standard.  At the
        time the problem of duplicate station codes was not recognized and
        the SEED concept of a network code (also location code for channel)
        was not considered.   The developers of Datascope created a workaround
        for this problem in a special table (I am not sure if it was part
        of the original css3.0 schema or not) with the name `snetsta'.
        They use snetsta to handle duplicate station names in multiple networks.
        A common example is that more than one operator has used the
        colorful station code "HELL" so if we see an entry for HELL it can
        be ambiguous which level of HELL it refers to.  Antelope
        handles this by creating compound keys like "66_HELL" for net "66"
        and station code "HELL".   A complication is the compound keys
        are only used when duplicate sta codes a detected, which is not
        always easy to know.  In any case, the purpose of this method
        is to parse the snetsta table to return the data need to
        translate any station code (compound or not) to a unique
        combination of "net" and "sta" codes It does that by returning
        a dictionary keyed by expected station codes in other Datascope
        tables with a value containing a pair of string.  The 0 component
        is the net code associated with the key value and the 1 component
        the station code that would match what is expected in a parent
        miniseed file.
        """
        df = self.get_table("snetsta")
        xref = dict()
        for i in df.index:
            sta = df["sta"][i]
            net = df["snet"][i]
            seed_sta = df["fsta"][i]
            xref[sta] = tuple([net, seed_sta])
        return xref
