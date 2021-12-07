#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <vector>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>

#include "libmseed.h"
#include "mspass/io/mseed_index.h"
#include "mspass/utility/ErrorLogger.h"

using namespace std;
namespace mspass::io
{
using namespace mspass::io;
using mspass::utility::ErrorSeverity;
/* Using this file scope typedef to avoid the absurdly complex syntax of an
std::pair constructor with complex objects like this */
typedef std::pair<std::vector<mseed_index>,mspass::utility::ErrorLogger> MSDINDEX_returntype;


MSDINDEX_returntype   mseed_file_indexer(const string inputfile,
  const bool segment_timetears,const bool Verbose)
{
  const string function_name("mseed_file_indexer");
  MS3Record *msr = 0;
  /* This thing is used for the thread safe reader.   It uses the common
  plain C implicit signal to alloc the struct it contains when the
  pointer is NULL.   Each call then uses the same data in the msfp
  struct.  The weird cleanup call at the end of the read loop
  calls the equivalent of a destructor.*/
  MS3FileParam *msfp = NULL;
  uint32_t flags = MSF_SKIPNOTDATA ;
  // int8_t ppackets = 0;
  int8_t verbose = 0;
  int retcode;
  char last_sid[128],current_sid[128];
  /* This is used to define a time tear (gap).  When the computed endtime of
  the previous packet read mismatches the starttime of the current packet we
  create  a segment by defining a new index entry terminated at the time
  tear */
  const double time_tear_tolerance(0.5);

  vector<mseed_index> indexdata;

  /* Enable accumulation of up to 10 error and warning messages.  This
  capability is currently not functional.  I (glp) think internal libmseed
  errors get posted this way but by default go to stderr.  I think we need
  a mechanism to post them to elog. */
  ms_rloginit (NULL, NULL, NULL, NULL, 10);
  mspass::utility::ErrorLogger elog;

  /* Loop over the input file record by record */
  int64_t fpos=0;
  uint64_t start_foff,nbytes;
  int count=0;
  char net[16],sta[16],loc[16],chan[16];  //larger than needed but  safe
  mseed_index ind;
  nstime_t stime,lptime;
  int64_t npts(0);
  double last_packet_samprate,last_packet_endtime,expected_starttime, last_dt;
  double epoch_stime;   // mseed saves times in some mysterious format.  This hold epoch start time
  bool found_time_tear(false); //used to simplify logic of handling time tears
  /* It is not clear what verbose means in this function so we currently always
  turn it off.   Note Verbose and verbose are different - a bit dangerous but
  that is what is for now.
  Also changed dec 2021:  changed to thread safe version.  Requires adding
  msfp struct initialized as NULL.  */
  while ((retcode = ms3_readmsr_r (&msfp,&msr, inputfile.c_str(), &fpos, NULL,
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
      epoch_stime = MS_NSTIME2EPOCH(static_cast<double>(stime));
      strcpy(last_sid,msr->sid);
      ++count;
      npts=msr->samplecnt;
      last_packet_samprate=msr->samprate;
      last_dt = 1.0/last_packet_samprate;
      /* endtime has to be computed from number of samples and starttime.  It
      is not stored in the header.  We need this to check for time tears. */
      last_packet_endtime = MS_NSTIME2EPOCH(static_cast<double>(stime))
                                     + ((float)(npts-1)*last_dt);
      continue;
    }
    /* The cost of computing these is tiny even though they are ignored when
    segment_timetears is false.*/
    epoch_stime = MS_NSTIME2EPOCH(static_cast<double>(msr->starttime));
    expected_starttime = last_packet_endtime + (last_dt);
    if(segment_timetears)
    {
      if(fabs(epoch_stime-expected_starttime)>time_tear_tolerance)
      {
        found_time_tear = true;
      }
      else
      {
        //small cost to force this every packet but more robust this way
        found_time_tear = false;
      }
    }
    else
    {
      /* Easier to add error log entry here when a time tear is detected but
      when not creating additional index entries. */
      if(Verbose)
      {
        stringstream ss;
        ss << "WARNING:  Detected time tear at packet number "<<count<<" in file "<<inputfile
           <<endl
           << "last packet sid=" << last_sid <<endl
           << "Current packet sid=" << current_sid<<endl
           << "Time difference across time tear (current-last)="
           << epoch_stime - expected_starttime<<endl
           << "Current packet epoch time="<<epoch_stime<<endl
           << "Function was run without segmentation option.   Reader will have to handle this problem as a gap"<<endl;
        elog.log_error(function_name,ss.str(),ErrorSeverity::Complaint);
      }
    }

    strcpy(current_sid,msr->sid);
    if(strcmp(current_sid,last_sid) || found_time_tear)
    {
      if(ms_sid2nslc(last_sid,net,sta,loc,chan))
      {
        stringstream ss;
        ss << "source id string="<<last_sid<<" in packet number "<<count
           << " of file "<< inputfile << " could not be decode"<<endl
           << "This segment will be dropped.  Some data in this file will be inaccessible";
        elog.log_error(function_name,ss.str(),ErrorSeverity::Complaint);
        strcpy(last_sid,current_sid);
        start_foff=fpos;
        continue;
      }
      if(found_time_tear && Verbose)
      {
        /* In this logic landing here means we have a time tear.
        Note if segment_timetears is false we won't log these messages.
        Careful if the logic changes*/
        stringstream ss;
        ss << "Detected time tear a packet number "<<count<<" in file "<<inputfile
           <<endl
           << "last packet sid=" << last_sid <<endl
           << "Current packet sid=" << current_sid<<endl
           << "Time difference across time tear (current-last)="
           << epoch_stime - expected_starttime<<endl
           << "Current packet epoch time="<<epoch_stime<<endl
           << "Creating a new index entry at the time tear"<<endl;
        elog.log_error(function_name,ss.str(),ErrorSeverity::Informational);
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
      ind.samprate=last_packet_samprate;
      ind.npts=npts;
      ind.endtime=ind.starttime + (static_cast<double>(npts))/ind.samprate;
      indexdata.push_back(ind);
      start_foff=fpos;
      stime=msr->starttime;
      npts=msr->samplecnt;
      found_time_tear = false;
    }
    strcpy(last_sid,current_sid);
    //msr3_print (msr, ppackets);
    ++count;
    npts+=msr->samplecnt;
    lptime=msr->starttime;
    last_packet_samprate=msr->samprate;
    last_dt = 1.0/last_packet_samprate;
    last_packet_endtime = lptime + ((float)(npts-1)*last_dt);
  }

  if (retcode != MS_ENDOFFILE)
  {
    /* This call needs a mechanism to extract any library error messages and
    translate them to elog.  For now we write a simpler generic message.
    Obspy's reader is known to fail reading files with corrupted packets so
    this error needs to be logged anyway as an Invalid return*/
    ms_rlog_emit (NULL, 0, verbose);
    elog.log_error(function_name,
       "miniseed read function exited before end of file with an unrecoverable read error.\nReader may fail and the index for this file may be incomplete",
        ErrorSeverity::Invalid);
  }
  else
  {
    /* cleanup last block */
    if(ms_sid2nslc(last_sid,net,sta,loc,chan))
    {
      elog.log_error(function_name,
          "ms_sid2nslc function failed on last data block - data at end of file dropped\n",
          ErrorSeverity::Complaint);
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
      ind.samprate=last_packet_samprate;
      ind.npts=npts;
      ind.endtime=ind.starttime + (static_cast<double>(npts))/ind.samprate;
      indexdata.push_back(ind);
    }
  }

  /* Make sure everything is cleaned up.  Documentation says this is needed
  to invoke the plain C equivalent of a destructor.*/
  ms3_readmsr_r (&msfp, &msr, NULL, NULL, NULL, 0, 0);
  return MSDINDEX_returntype(indexdata,elog);
}
} // End namespace mspass::io
