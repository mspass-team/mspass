#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import sys
from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
from mspasspy.db.normalize import normalize_mseed

"""
This file contains a python script that defines a command line
tool we call normalize_mseed.   It is intended to be a used as a 
preprocessing tool to "normalize" the contents of the wf_miniseed 
collection.   The idea of the tool is that miniseed data, by
definition (the mini of miniseed means part of the see format) is 
incomplete and lacks station metadata.   The only thing miniseed 
data have are the four seed station code strings (net,sta, chan, and loc)
and time tags for each data packet.   The wf_miniseed collection is 
used to produce an index into a set of miniseed files that are 
presumed to define a set of waveform time segments.  This tool 
will try to create the entries channel_id and (optionally) site_id 
for each record in wf_miniseed.   It issues a report to stdout 
on completion that give the number of documents set and the number 
of documents for which no match could be found in the channel (or site)
collections.

usage: python normalize_mseed.py dbname [--normalize_site --blocksize n -wfquery querystring]
"""

def main(args=None):
    """
    Main function for normalize_mseed command line tool.   
    All this function does is parse the command line to build the 
    args sent to the function called "normalize_mseed" defined above. 
    Note that function may need to be moved to the normalize python 
    module.  
    """

    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="python normalize_mseed.py",
        usage="%(prog)s dbname [--normalize_site --blocksize n -wfquery querystring]",
        description="MsPASS program to create cross-referencing keys from wf_miniseed to channel",
    )
    parser.add_argument(
        "dbname", metavar="dbname", type=str, help="MongoDB database name to be normalized"
    )
    parser.add_argument(
        "--normalize_site",
        action="store_true",
        help="Normalize site collection as well as channel collection (default is false)",
    )
    parser.add_argument(
        "--blocksize",
        type=int,
        default=1000,
        help="Set number of entries to cache for each bulk update call for MongoDB"
    )
    parser.add_argument(
        "--wfquery",
        type=str,
        default="",
        help="Optional query to apply to wf_miniseed collection"
        
    )
    
    args = parser.parse_args(args)
    dbname = args.dbname
    print("Normalizing database with name=",dbname)

    normalize_site = args.normalize_site
    if normalize_site:
        print("Will normalize all matching wf_miniseed documents site_id and channel_id")
    else:
        print("Will normalize all matching wf_miniseed documents with channel_id")
    blocksize = args.blocksize
    if len(args.wfquery) > 0:
        query = eval(args.query)
    else:
        query = {}
    
    dbclient = DBClient()
    db=Database(dbclient,dbname)
    
    ret_tuple = normalize_mseed(db,
                                wfquery=query,
                                blocksize=blocksize,
                                normalize_site=normalize_site)
    print("Normalization completed for wf_miniseed collection of database = ",dbname)
    if len(args.wfquery) > 0:
        print("Applied with this query to wf_miniseed:  ",args.query)
    print("Number of documents processed=",ret_tuple[0])
    print("Number of channel_ids set=",ret_tuple[1])
    print("Number of site_ids set=",ret_tuple[2])
        
if __name__ == "__main__":
    main()