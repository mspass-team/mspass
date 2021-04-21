#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <vector>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>

#include "libmseed.h"
#include "mspass/io/mseed_index.h"

using namespace std;
namespace mspass::io
{
using namespace mspass::io;

vector<mseed_index> mseed_file_indexer(const string inputfile)
{
  MS3Record *msr = 0;
  uint32_t flags = MSF_SKIPNOTDATA ;
  int8_t ppackets = 0;
  int8_t verbose = 0;
  int retcode;
  char last_sid[128],current_sid[128];

  vector<mseed_index> indexdata;

  /* Enable accumulation of up to 10 error and warning messages */
  ms_rloginit (NULL, NULL, NULL, NULL, 10);

  /* Loop over the input file record by record */
  int64_t fpos=0;
  uint64_t start_foff,nbytes;
  int count=0;
  char net[16],sta[16],loc[16],chan[16];  //larger than needed but  safe
  mseed_index ind;
  nstime_t stime,lptime;
  while ((retcode = ms3_readmsr (&msr, inputfile.c_str(), &fpos, NULL,
                                 flags, verbose)) == MS_NOERROR)
  {
    if(count==0)
    {
      /* We have to treat the first block specially because of the logic of
      how this loop is constructed and the way the ms3 reader works.
      A problem, however, is this may fail if there first block is not a
      miniseed data block.   Probably should test this on a full seed file */
      start_foff=fpos;
      stime=msr->starttime;
      strcpy(last_sid,msr->sid);
      ++count;
      continue;
    }

    strcpy(current_sid,msr->sid);
    if(strcmp(current_sid,last_sid))
    {
      if(ms_sid2nslc(last_sid,net,sta,loc,chan))
      {
        /* This needs to be replaced to use the libmseed log functions */
        fprintf(stderr,"source id string=%s in packet number %d could not be decoded - skipping one or more packets\n",
            last_sid,count);
        strcpy(last_sid,current_sid);
        start_foff=fpos;
        continue;
      }
      nbytes=fpos-start_foff;
      ind.net=net;
      ind.sta=sta;
      ind.loc=loc;
      ind.chan=chan;
      ind.foff=start_foff;
      ind.nbytes=nbytes;
      ind.starttime=MS_NSTIME2EPOCH(static_cast<double>(stime));
      ind.last_packet_time=MS_NSTIME2EPOCH(static_cast<double>(lptime));
      indexdata.push_back(ind);
      start_foff=fpos;
      stime=msr->starttime;
    }
    strcpy(last_sid,current_sid);
    //msr3_print (msr, ppackets);
    ++count;
    lptime=msr->starttime;
  }

  if (retcode != MS_ENDOFFILE)
    ms_rlog_emit (NULL, 0, verbose);
  else
  {
    /* cleanup last block */
    if(ms_sid2nslc(last_sid,net,sta,loc,chan))
    {
      fprintf(stderr,"ms_sid2nslc function failed on last data block - data at end of file dropped\n");
    }
    else
    {
      nbytes=fpos-start_foff;
      ind.net=net;
      ind.sta=sta;
      ind.loc=loc;
      ind.chan=chan;
      ind.foff=start_foff;
      ind.nbytes=nbytes;
      ind.starttime=MS_NSTIME2EPOCH(static_cast<double>(stime));
      ind.last_packet_time=MS_NSTIME2EPOCH(static_cast<double>(lptime));
      indexdata.push_back(ind);
    }
  }

  /* Make sure everything is cleaned up */
  ms3_readmsr (&msr, NULL, NULL, NULL, 0, 0);
  return indexdata;
}
} // End namespace mspass::io
