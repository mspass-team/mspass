#!/usr/bin/env python3
"""
******
dbverify - MsPASS database verify command line tool
******

.. topic:: Usage

    dbverify dbname [-t testname] [-c col ... ]
            [-n n1 ...] [-r k1 ...][-e n] [-v] [-h | --help]

.. topic:: Description

    *dbverify* is a command line tool to run a set of essential tests on
    a dataset stored in the MsPASS database.   This program or the test
    functions it uses should always be run on a dataset before starting
    a long job to process data with MsPASS.  It performs essential validation
    tests that are needed to reduce the chances of a large workflow aborting
    during processing.  The command line tool, *dbverify*, should be viewed as a
    front-end to test functions in the python library of MsPASS.  This tool
    was written largely to provide a way simply the output of the test
    functions and produce output that can be more easily understood. Note the
    tool was intentionally designed to run only one test at a time to
    simplify output.  Because MongoDB supports many simultaneous clients to
    test large data sets it may be useful to run each test as a separate job.

The following tests are currently supported.
They can only be run one at a time and are controlled by the *testname*
argument attached to the -t flag:

- **normalization**:  This test validates cross-references to collections that
  MongoDB calls normalized data.  Normalized data are smaller collections of
  common data that are more efficient to store together and link to the
  waveform data through a query with a cross-referencing key.   In MsPASS
  current normalization collections in the standard schema are called
  *site*, *channel*, and *source*.  When this test is run the -n
  option can be used to control what normalization collections are tested.
  The default is equivalent to "-n site".   The collection on which the
  test is to be run can be set with the -c option.  For this test -c should
  reference either wf_TimeSeries or wf_Seismogram.  It can only be run on
  one collection at a time.

- **required**:   Certain attributes are always required for processing.
  Use the -r option to provide a list of keys that should be
  checked for existence in the specified collection.   If -r is not give
  an internal default is used.   We recommend always providing a list of
  what your workflow considers required.  Only one collection can be
  processed at a time to keep the -r list manageable.

- **schema_check**:   Although MongoDB is completely promiscuous about
  types of attributes associated with any key, in MsPASS we imposed some order on the
  system through a schema.   Some attributes can have type requirements that
  may cause a job to abort if they are improperly defined.  The idea of this
  test is to flag all possible key-value pairs in a collection that
  are inconsistent with the schema.  Some mismatches are mostly harmless
  while others are nearly guaranteed to cause processing problems.
  What defines "mostly harmless" is discussed in more detail below in the
  section titled "Interpreting Test Results".   This is currently the only
  test that can be run on multiple collections in a single run.   The reason
  is it depends completely on the schema and needs no other direction
  other than a list of collections to scan.

.. topic:: Arguments

    Argument 1 is required to be a valid database name.   The program will attempt
    to connect the MongoDB server and access the data with that name tag.  The
    program will abort immediately if MongoDB is not running.   If you give an
    invalid name you can expect mysterious errors - most likely complaints that
    the collection you tried to test has no data.

* -t:
    Control the testname as described above.  Can also use --test.

* -c:
    This flag is used to mark the list of one or more collections.  Only the
    *schema_check* test supports multiple collections in one run. The other tests have
    argument dependencies that do not map well to the command line argument
    method of control parameter inputs.  For those tests if multiple arguments
    follow -c only the first will be used and a warning error will be printed.
    Can also use --collection as this flag.

* -n:
    This is used to mark the start of a list of secondary collections to that
    defined by -c that are expected to resolve with cross reference ids
    in the *normalization* test.   Any -n arguments for
    tests other than *normalization* will be ignored.  Can also use --normalize.
    .IP -r
    This is used to mark a list of required key attributes for the required
    test.   Any arguments after -r will be ignored by other tests.
    Can also use --required.

* -v:
    Runs in verbose mode.  Default is to print only a summary of the test results.
    Verbose mode is most useful as a followup when errors are detected in one
    or more collections and you need more information about what is wrong.
    Can also use --verbose

* -e:
    Sets a limit on the number of errors found before stopping the test.
    A large dataset with a common error in most or all the documents in
    a collection can generate voluminous output in verbose mode and pass the
    point of diminishing returns for the standard summary mode. The default is
    1000.  Smaller numbers are often useful in verbose mode and larger numbers
    may be needed for data sets assembled from multiple sources with different
    problems.

.. topic:: EXAMPLES

    * Run the normalization test on the wf_Seismogram collection:

    dbverify mydb -c wf_Seismogram -t normalization -n site source

    * Run the required test on the channel collection

    dbverify mydb -c channel -t required -r lat lon elev hang vang starttime endtime
    
    * Run the schema_check test on all standard collections fpr TimeSeries data
    dbverify mydb -c wf_TimeSeries channel source -t schema_check
    
    * Rerun schema test on wf_TimeSeries to clarify errors
    
    * impose a limit of only 100 documents to keep the output reasonable.

    dbverify mydb -c wf_TimeSeries -t schema_check -v -e 100

.. topic:: INTERPRETING TEST RESULTS

    We recommend this program should usually be run in the default nonverbose mode.
    In that mode all the tests attempt to simply summarize the errors the test
    found.  In most cases that is list of offending keys and the number of documents
    found with the particular error.

    The *normalization* test print an all is well message if it finds
    no errors for a given collection.  If there are problems it will simply
    list the number of documents it found with an error.  If that number is
    the same as the error limit or the number of waveforms in the database it
    probably means all links to that collection are broken.

    In normal mode the *schema_check* and *required* test both produce
    an output of offending keys and the number of documents they found with
    that error.  The types of errors are defined in the print statements.

    In all cases, the way you should use this program is to first always run
    it in the default (not verbose) mode.  If any of the test fail look
    at the output carefully.  Some errors may not require rerunning the
    program.  For example, keys that are not defined in the schema will
    generate errors in multiple tests. Undefined keys are usually harmless
    but they do waste storage if they aren't really needed.  Type collisions
    can be the hardest to sort out.  If you have type mismatch errors
    we recommend the first step is to rerun the same test in verbose mode
    to look at the full set of attributes stored in the offending collection
    for a few errors.  As noted earlier when running in verbose mode it is
    usually prudent to reduce the error limit size with the -e option.
    Many type errors are recoverable and some are not.  For example, it is
    relatively easy in python to unintentionally post a real number as an integer.
    That is particularly so when data are entered as literals in a python script.
    (e.g. in python the statements x=11.0 and y=11 do not the same things even though
    the numbers might seem the same.  x is a float and y is an integer.)
    The MsPASS readers will convert type collisions like the example above
    automatically.  What is not repairable are string data appearing in a field
    the schema requires to be a number.  e.g. if one posted lat='37N22.40' it might
    look right but cannot be automatically converted.  The point is type
    mismatches can be hard to detect without this tool and even after it is
    detected it can be a challenge to see the problem from the output.
    Fortunately, such errors are rare unless a function has a bug or the input
    is created by some simulation method that doesn't set some attributes
    correctly. 

.. seealso::
    The tests this program runs are python functions found in the module
    mspasspy.db.verify.   See the sphynx documentation for more about
    the individual functions.
    "BUGS AND CAVEATS"
    The function currently does not support any query mechanism on a
    collection it processes.  It will only work on an entire collection or
    set of collections you direct the program to process.  All the current
    tests are reasonably fast and should be feasible as a serial process
    for any currently conceivable dataset, but for very large data sets
    some of them could take some time to complete.   If you need to
    query a smaller data volume use the python functions directly.

.. topic:: Author

    Gary L. Pavlis

    Department of Earth and Atmospheric Sciences

    Indiana University
"""

import sys
import argparse
from bson import json_util
from bson.objectid import ObjectId
from mspasspy.ccore.utility import MsPASSError
from mspasspy.db.database import Database
from mspasspy.db.client import Client as DBClient

def print_id_keyed_dict(d):
    """
    Prints a python dict d keyed by ObjectId in a format with 
    beginning and end of each dump marked by an obvious human readable 
    mark.   Part of the reason this is necesary is json_util.dumps like
    json.dumps does not like dict keys to be anything but a simple type.
    
    :param d:  python dict with id keys to be printed
    """
    newdoc_string="=========================================================="
    for key,value in d.items():
        print(newdoc_string)
        if isinstance(key,ObjectId):
            print('ObjectId string of document=',str(key))
        else:
            print('WARNING:  key is not object id as it shoudl be. It is->',
                  key,' of type ',type(key))
        print(newdoc_string)
        if type(value)==dict:
            print(json_util.dumps(value,indent=2))
        else:
            print(value)
    
def print_bad_wf_docs(dbcol,idlist):
    """
    Print values from a doc defined in dbcol matching a list of ids.  
    Print is the dump of the dict form from the aux_list subset.   
    
    :param dbcol:  database collection ids with errors were pulled from
    :param idlist:  python list of ObjectIds to print

    """
    n=1
    for id in idlist:
        print('////////////////Doc number ',n,' with error///////////////')
        query={'_id' : id}
        doc=dbcol.find_one(query)
        print(json_util.dumps(doc,indent=2))
        print('////////////////////////////////////////////////////////')
                
def run_check_links(db,wfcollection,nrmlist,elimit,verbose):
    """
    Run the verify function called check_links and produces a reasonable
    summary or readable (verbose) printed output of the results.  
    The check_links function verifies if normalization ids are 
    set and resolve correctly.   It treats the two problems together
    but returns containers that link to each problem separately.  
    This function prints a summary of the result of the function 
    when not verbose.  When verbose a json dump of all attributes
    in offending documetions are written to stdout - this can get 
    quite huge but use the elimit arg to reduce the size.
    
    :param db:  mspass Database handle
    :param wfcollection:  waveform collection name to be scanned for 
      normalization links.
    :param nrmlist:  is a list of collection names that are to 
      be tested.  Note in MsPASS we assume the cross reference is an
      ObjectId of the document in a give collection name. e.g. for 
      a link to site the test will search for site_id and verify 
      a document with that id is in site.  
    :param elimit:  number of errors allowed before the test is 
      terminated. For large data sets with many problems this test
      could take a long time just to return a summary.  Keep this 
      parameter small until you find common errors to reduce effort.
    :verbose:  when true each document with an error will have the 
      entire contents (sans waveform sample data) printed. When
      false (the default for this program) only print a summary of 
      how many offending docs were found.  

    """

    for nrmcol in nrmlist:
        errs=db._check_links(xref_key=nrmcol,
                          collection=wfcollection,
                          error_limit=elimit)
        broken=errs[0]
        undef=errs[1]
        if verbose:
            if len(broken)==0:
                print('check_links found no broken links with normalized key=',nrmcol)
            else:
                print('check_link found the following docs in ',wfcollection,
                  ' with broken links to ',nrmcol)
                print_bad_wf_docs(db[wfcollection],broken)
            if len(undef)==0:
                print('check_links found no undefined linking key to normalized key=',
                  nrmcol)
            else:
                print('check_link found the following docs in ',wfcollection,
                  ' with undefined link keys to ',nrmcol)
                print_bad_wf_docs(db[wfcollection],undef)
        else:
            if len(broken)==0:
                print('check_links found no broken links with normalized key=',nrmcol)
            else:
                print('normalization test on normalized key=',nrmcol,' found problems')
                print('Found broken links in ',len(broken),
                      'documents checked')
                print('Note error count limit=',elimit)
                print('If the count is the same it means all data probably contain missing cross referencing ids')
                print('Run in verbose mode to find out more information you will need to fix the problem')
            
def run_check_attribute_types(db,col,elimit,verbose):
    """
    Run the verify function called check_attribute_types and produces 
    a reasonable summary or readable (verbose) printed output of the results.  
    The function checks for schema collisions of two types:  (1) type
    mismatch of what what the schema asks and what is actually found 
    and (2) data with keys not found in the schema definition. 
    It treats the two problems together
    but returns containers that link to each problem separately.  
    This function prints a summary of the result of the function 
    when not verbose.  When verbose a json dump of all attributes
    in offending documetions are written to stdout - this can get 
    quite huge but use the elimit arg to reduce the size.
    
    :param db:  mspass Database handle
    :param collection:  collection name to be scanned.  
    :param elimit:  number of errors allowed before the test is 
      terminated. For large data sets with many problems this test
      could take a long time just to return a summary.  Keep this 
      parameter small until you find common errors to reduce effort.
    :verbose:  when false (default for this program) only a summary 
      of offending keys and counts of the number of problems found 
      are printed. When true every document with offending data will
      be printed with the values (most useful for type mismatch errors)

    """
    # Although the function allows it this program won't support
    # a query for now because it would get ugly with different 
    # definitions for each collection listed.  Verbose is 
    # also always off.  Note the solution to enter one from 
    # an input is to use json.loads
    errs=db._check_attribute_types(col,error_limit=elimit)
    mismatch=errs[0]
    undef=errs[1]
    print('check_attribute_types result for collection=',col)
    if verbose: 
        if len(mismatch)==0:
            print('Collection has no type inconsistencies with schema')
        else:
            print('//////Collection=',col,' has type inconsistencies in ',
                  len(mismatch),' documents////')
            print('///The following have types that do not match schema////')
            print_id_keyed_dict(mismatch)
        if len(undef)==0:
            print('Collection has no data with keys not defined in schema')
        else:
            print('//////Collection=',col,' has unrecogized keys in ',
                  len(undef),' documents/////')
            print('////The following are offending data with doc ids///')
            print_id_keyed_dict(undef)
    else:
        if len(mismatch)==0:
            print('Collection has no type inconsistencies with schema')
        else:
            mmkeys=dict()
            for k in mismatch:
                badkeys=mismatch[k]
                for bad in badkeys:
                    if bad in mmkeys:
                        n=mmkeys[bad]
                        n+=1
                        mmkeys[bad]=n
                    else:
                        mmkeys[bad]=1
            print('Collection found ',len(mismatch),
                  ' documents with type inconsistencies')
            print('Offending keys and number found follow:')
            print(json_util.dumps(mmkeys,indent=2))
        #Same for undef with minor differences in what is printed
        # maybe should make this a function
        if len(undef)==0:
            print('Collection has no type inconsistencies with schema')
        else:
            mmkeys=dict()
            for k in undef:
                badkeys=undef[k]
                for bad in badkeys:
                    if bad in mmkeys:
                        n=mmkeys[bad]
                        n+=1
                        mmkeys[bad]=n
                    else:
                        mmkeys[bad]=1
            print('Collection found ',len(undef),
                  ' documents with keys not defined in the schema')
            print('Offending keys and number found follow:')
            print(json_util.dumps(mmkeys,indent=2))

                
def run_check_required(db,col,required_list,elimit,verbose):
    """
    Each standard collection in mspass has some key attributes that
    are essential to be useful.   This test should be run to verify 
    critical data are defined before a major processing run.   This
    function is a print wrapper for the test function check_required.  
    The program default runs it with verbose false in which case it only 
    prints a summary of offending keys and the number of documents 
    found that were missing each required key.  When verbose is true
    the contents of all offending documents will be printed in a 
    readable layout with json_util.dumps.  

    """
    errs=db._check_required(col,required_list,error_limit=elimit)
    mismatch=errs[0]
    undef=errs[1]
    print('////Results from run_check_required on collection=',col)
    if verbose:
        if len(mismatch)==0:
            print('Collection has no type mismatches')
        else:
            print('/////Collection has ',len(mismatch),
              ' documents with a type mismatch//////')
            print('/////Mismatched data with doc ids follow/////')
            print_id_keyed_dict(mismatch)
            print('//////////////////////////////////////////////////////////////')
        if len(undef)==0:
            print('Collection has all required data')
        else:
            print('Collection has at least ',len(undef),
              ' documents lacking one or more attributes')
            print('///////ids of bad docs with missing keys listed follow')
            print_id_keyed_dict(undef)
            print('//////////////////////////////////////////////////////////////')
    else:
        if len(mismatch)==0:
            print('Collection has no type mismatches')
        else:
            mmkeys=dict()
            for k in mismatch:
                badkeys=mismatch[k]
                for bad in badkeys:
                    if bad in mmkeys:
                        n=mmkeys[bad]
                        n+=1
                        mmkeys[bad]=n
                    else:
                        mmkeys[bad]=1
            print('Collection found ',len(mismatch),
                  ' documents with type inconsistencies')
            print('Offending keys and number found follow:')
            print(json_util.dumps(mmkeys,indent=2))
        if len(undef)==0:
            print('Collection has all required data')
        else:
            mmkeys=dict()
            for k in undef:
                badkeys=undef[k]
                for bad in badkeys:
                    if bad in mmkeys:
                        n=mmkeys[bad]
                        n+=1
                        mmkeys[bad]=n
                    else:
                        mmkeys[bad]=1
            print('Collection found ',len(undef),
                  ' documents with required keys that were not defined')
            print('Offending keys and number found follow:')
            print(json_util.dumps(mmkeys,indent=2))

def get_required(collection):
    if collection=='site':
        return ['lat','lon','elev'] 
    elif collection=='channel':
        return ['lat','lon','elev','hang','vang']
    elif collection=='source':
        return ['lat','lon','depth','time']
    elif collection=='wf_TimeSeries' or collection=='wf_Seismogram':
        return ['npts','delta','starttime']
    else:
        raise MsPASSError('No data on required attributes for collection='
                          +collection,'Fatal')
def main(args=None):
    # As a script that would be run from the shell we let 
    # any functions below that throw exception do so and assume they 
    # will write a message that can help debug what went wrong
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser(prog="dbverify",
                    usage="%(prog)s dbname [-t TEST -c [collection ...] -n [normalize ... ] -error_limit n -v]",
                    description="MsPASS database verify program")
    parser.add_argument('dbname',
                       metavar='dbname',
                       type=str,
                       help='MongoDB database name on which to run tests')
    parser.add_argument('-t',
                        '--test',
                        action='store',
                        type=str,
                        default='normalization',
                        help='Select which test to run.  '
                          + 'Current options:  normalization, required, schema_check'
                          )
    parser.add_argument('-c',
                        '--collection',
                        action='store',
                        nargs='*',
                        default=['wf_TimeSeries'],
                        help='Collection(s) on which the test is to be run.  '
                               + 'Only schema_check supports multiple collections in one run'
                        )
    parser.add_argument('-n',
                       '--normalize',
                       nargs='*',
                       default=['site_id','channel_id','source_id'],
                       help='List of normalization keys to test\n'
                         + '(Used only for -test normalization option'
                       )
    parser.add_argument('-r',
                        '--require',
                        nargs='*',
                        default=[],
                        help='List of keys of required attributes for required test'
                        )
    parser.add_argument('-e',
                        '--error_limit',
                        action='store',
                        type=int,
                        default=1000,
                        help='Set error limit - stop checking when this many errors are found\n'
                        + 'Default is 1000'
                        )
    parser.add_argument('-v',
                       '--verbose',
                       action='store_true',
                       help='When used print offending values.  Otherwise just return a summary'
                       )

    args = parser.parse_args(args)
    test_to_run=args.test
    dbname=args.dbname
    dbclient=DBClient()
    db=Database(dbclient,dbname)
    col_to_test=args.collection
    normalize=args.normalize
    reqlist=args.require
    verbose=args.verbose
    elimit=args.error_limit

    # If python had a switch case it would be used here.  this 
    # is the list of known tests.  the program can only run one 
    # test per execution.  Intentional to make output more readable
    if test_to_run=='normalization':
        if len(col_to_test)>1:
            print('WARNING:  normalization test can only be run on one collection at a time')
            print('Parsed a list with the following contents:  ',col_to_test)
            print('Running test on the first item in that list')
        col=col_to_test[0]
        if not isinstance(col,str):
            print('Invalid value parsed for -c option=',col)
            exit(-1)
        run_check_links(db,col,normalize,elimit,verbose)
    elif test_to_run=='required':
        if len(col_to_test)>1:
            print('WARNING:  required test can only be run on one collection at a time')
            print('Parsed a list with the following contents:  ',col_to_test)
            print('Running test on the first item in that list')
        col=col_to_test[0]
        if not isinstance(col,str):
            print('Invalid value parsed for -c option=',col_to_test)
            exit(-1)
        if len(reqlist)==0:
            # Depends on default being an empty list. For default 
            # case run this small function.
            # This is currently a funtion above with const list values
            # returned for each known collection.  It may eventually 
            # be replaced a function using the schema
            required_list=get_required(col)
        else:
            required_list=reqlist
        run_check_required(db,col,required_list,elimit,verbose)
    elif test_to_run=='schema_check':
        for col in col_to_test:
            run_check_attribute_types(db,col,elimit,verbose)
    else:
        print('Unrecognized value for --test value parsed=',test_to_run)
        print('Must be one of:  normalization, required, or schema_check')
    

if __name__ == "__main__":
    main()
