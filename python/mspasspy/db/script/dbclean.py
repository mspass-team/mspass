#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a python script that defines a command line 
tool we call dbclean.  dbclean is designed to fix errors detected
with a companion command line tool called dbverify.  The options 
of this tool largely map into errors or potential errors detected by 
dbverify.  

"""
import argparse
import sys
from bson import json_util
from mspasspy.db.database import Database
from mspasspy.db.client import Client as DBClient

def rename_list_to_dict(rlist):
    """
    Helper for main to parse args for rename operator.  The args are 
    assumed to be a pair of strings separated by a ":".  These are 
    parsed into a dict that is returned with the old document key to 
    be replaced as the (returned) dict key and the value of the return 
    being set as the string defining the new key.  

    """
    result=dict()
    for val in rlist:
        pair=val.split(':')
        if len(pair)!=2:
            print('Cannot parse pair defined as ',val)
            print('-r args are expected to be pairs of keys strings with a : separator')
            print('Type dbclean --help for usage details')
            exit(-1)
        result[pair[0]]=pair[1]
    return result
        

def main(args):
    """
    """
    parser = argparse.ArgumentParser(prog="dbclean",
                                     usage="%(prog)s dbname collection [-ft] [-d k1 ...] [-r kold:knew ... ] [-v] [-h]",
                                     description="MsPASS program to fix most errors detected by dbverify")
    parser.add_argument('dbname',
                        metavar='dbname',
                        type=str,
                        help='MongoDB database name to be fixed')
    parser.add_argument('collection',
                        metavar='collection',
                        type=str,
                        help='MongoDB collection name to be fixed')
    parser.add_argument('-ft',
                        '--fixtypes',
                        action='store_true',
                        help='Enable automatic type mismatch repair'
                        )
    parser.add_argument('-d',
                        '--delete',
                        nargs='*',
                        default=[],
                        help='List of keys of key-value pairs to be deleted from all documents'
                        )
    parser.add_argument('-r',
                        '--rename',
                        nargs='*',
                        default=[],
                        help='Change the keys of documents using pattern defined in args of form oldkey:newkey'
                        )
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        help='When used be echo each fix - default works silently'
                        )
    
    args = parser.parse_args(args)
    dbname=args.dbname
    collection=args.collection
    fixtypes=args.fixtypes
    delete=args.delete
    rename=args.rename
    verbose=args.verbose
    
    
    # not a very robust way to detect this condition but it should work
    # it is not robust because it assumes a behavior in argparse for 
    # args with a list
    if len(delete)>0:
        enable_deletion=True 
    else:
        enable_deletion=False
    if len(rename)>0:
        enable_rename=True
    else:
        enable_rename=False
    if not(fixtypes or enable_deletion or enable_rename):
        print('Usage error:  you must define at least one clean operation')
        print('Type:  dbclean --help to get usage help')
        exit(-1)
        
    if enable_rename:
        rename_map=rename_list_to_dict(rename)
        
    dbclient=DBClient()
    db=Database(dbclient,dbname)
    print('Starting processing of ',collection,
          ' collection of database named=',dbname)
    
    # Intentionally do the delete and rename operations before 
    # a type check to allow cleaning any keys. The set of dicts below 
    # accumulate counts of edits for each key
    
    if enable_deletion:
        delcounts=db._delete_attributes(collection,delete,verbose=verbose)
        print('delete processing compeleted on collection=',collection)
        print('Number of documents changed for each key requested follow:')
        print(json_util.dumps(delcounts,indent=4))
    if enable_rename:
        repcounts=db._rename_attributes(collection,rename_map,verbose=verbose)
        print('rename processing compeleted on collection=',collection)
        print('Here is the set of changes requested:')
        print(json_util.dumps(rename_map))
        print('Number of documents changed for each key requested follow:')
        print(json_util.dumps(repcounts,indent=4))
    if fixtypes:
        fixcounts=db._fix_attribute_types(collection,verbose=verbose)
        print('fixtype processing compeleted on collection=',collection)
        print('Keys of documents changed and number changed follow:')
        print(json_util.dumps(fixcounts,indent=4))

if __name__ == "__main__":
    main(sys.argv[1:])
