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
/*! Inline class used to make a cleaner interface the ugly libmseed function 
  for dealing with what they call an "sid".  
  */
class MSEED_sid
{
  public:
    MSEED_sid()
    {
      string s("");
      net=s;
      sta=s;
      chan=s;
      loc=s;
    }
    MSEED_sid(const char *sid);
    MSEED_sid(const MSEED_sid& parent)
    {
      net=parent.net;
      sta=parent.sta;
      chan=parent.chan;
      loc=parent.loc;
    };
    string net;
    string sta;
    string chan;
    string loc;
    MSEED_sid& operator=(const MSEED_sid& parent)
    {
      if(this != &parent)
      {
        net=parent.net;
        sta=parent.sta;
        chan=parent.chan;
        loc=parent.loc;
      }
      return *this;
    };
    bool operator!=(const MSEED_sid& other) const;
    friend ostream& operator<<(ostream& os, MSEED_sid& self)
    {
      string sep(":");
      os << self.net 
        <<sep<<self.sta
        <<sep<<self.chan
        <<sep<<self.loc;
      return os;
    };
    
};
/*! Constructor for this class.  

  Just copies c strings to std::strings in the class.  Throws a simple 
  int exception if the libmseed parser returns an error. 
  This could be improved if I could make sense of the error logger by 
  retrieving the message this obnoxious function posts to its error log. 
  */
MSEED_sid::MSEED_sid(const char *sid)
{
  char net[16],sta[16],loc[16],chan[16];  //larger than needed but  safe
  /* sid apparently is not const in prototype so we need this cast to 
     make it more kosher for this class */
  if(ms_sid2nslc(const_cast<char *>(sid),net,sta,loc,chan)==0)
  {
    this->net = string(net);
    this->sta = string(sta);
    this->chan = string(chan);
    this->loc = string(loc);
  }
  else
  {
    throw 1;
  }
}
bool MSEED_sid::operator!=(const MSEED_sid& other) const
{
  if( this->net==other.net && this->sta==other.sta && this->chan==other.chan && this->loc==other.loc)
    return false;
  else
    return true;
};
/* Using this file scope typedef to avoid the absurdly complex syntax of an
std::pair constructor with complex objects like this */
typedef std::pair<std::vector<mseed_index>,mspass::utility::ErrorLogger> MSDINDEX_returntype;
thread_local std::string buffer;
/*! Internal function translates miniseed reader function return codes 
  to readable messages posted in mseed_file_indexer to ErrorLogger. 
  */
std::string MS_code_to_message(int retcode)
{
  string message("Read error detected by libmseed reader function ms3_readmsr_r\n");
  message += "File index will be empty or truncated\n";
  switch(retcode)
  {
    case MS_GENERROR:
      message += "MS_GENERROR(-1) return - generic unspecified error";
      break;
    case MS_NOTSEED:
      message += "MS_NOTSEED(-2) return - Data not SEED";
      break;
    case MS_WRONGLENGTH:
      message += "MS_WRONGLENGTH(-3) return - Length of data read was not correct";
      break;
    case MS_OUTOFRANGE:
      message += "MS_OUTOFRANGE(-4) return - SEED record length out of range";
      break;
    case MS_UNKNOWNFORMAT:
      message += "MS_UNKNOWNFORMAT(-5) return - data encoding format value in packet is invalid";
      break;
    case MS_STBADCOMPFLAG:
      message += "MS_STBADCOMPFLAG(-6) return - compression flag value is invalid";
      break;
    case MS_INVALIDCRC:
      message += "MS_INVALIDCRC(-7) return - CRC value in packet is invalid";
      break;
    default:
      message += "Unknown return code - this should not happen and is likely a version skew problem";
  }
  return message;
}
/*! \brief Indexing function using libmseed low level function ms3_readmsr_r. 
 *
 * This function uses what has become the standard reader for miniseed from 
 * IRIS DMC called libmseed.  It uses the low level C function ms3_readmsr_r 
 * to read an input file one packet at a time.   It uses the version that
 * is claimed to be thread safe.   
 *
 * The complexities of seed can cause a number of problems.   This version 
 * tries to deal these complexities:
 * 1.  miniseed files are often produced by concatenation of data form multiple 
 *     channel.  Any change in station id returned by the function triggers a 
 *     new index entry.
 * 2.  Packet errors will force a new segment.
 * 3.  Time tears defined by either a jump or accumulated time mismatch of 
 *     more than 1/2 sample will trigger a new segments. 
 * 4.  Changes in sample interval trigger a new segments.   That is
 *     actually implicit in point 1 with the "sid" because of the 
 *     seed channel code naming convention. 
 * \return std::pair   first is a vector of index data.  second is 
 *   an ErrorLogger object.  Caller should test for empty vector 
 *   that is a signal for a open failure or a file that probably isn't 
 *   miniseed.   The content of elog should always be tested as any 
 *   errors there should be inspected/handled.   
 * */
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
  //char last_sid[128],current_sid[128];
  MSEED_sid last_sid,current_sid;
  /* This is used to define a time tear (gap).  When the computed endtime of
  the previous packet read mismatches the starttime of the current packet we
  create  a segment by defining a new index entry terminated at the time
  tear */
  const double time_tear_tolerance(0.5);

  vector<mseed_index> indexdata;

  mspass::utility::ErrorLogger elog;

  /* Loop over the input file record by record */
  int64_t fpos=0;
  uint64_t start_foff,nbytes;
  mseed_index ind;
  /* These values have a different time standard structure than 
   * epoch times.  These can only be compared with epoch times by 
   * calling the function MS_NSTIME2EPOCH
   */
  nstime_t stime;
  int64_t npts(0),current_npts;
  uint64_t number_packets_read(0),number_valid_packets(0);
  double last_packet_samprate,last_packet_endtime,expected_starttime, last_dt;
  /* These are used to handle long time series where the accumulated time 
   * from the start of a segment (many packets) gets inconsistent with 
   * the next packet's starttime.*/
  double segment_starttime,computed_segment_starttime;
  /* mseed stores time in an int (I think) this holds float 
   * conversions for current and last packet read.*/
  double current_epoch_stime,last_epoch_stime; 
  /* loop break boolean */
  bool data_available;
  /* It is not clear what verbose means in this function so we currently always
  turn it off.   Note Verbose and verbose are different - a bit dangerous but
  that is what is for now.
  Also changed dec 2021:  changed to thread safe version.  Requires adding
  msfp struct initialized as NULL. 

  March 2024:  changed from while to do-while loop.  That improves the logic
  because of the weird way this function works.  Runs one packet at a time 
  but the read loads msr with the data in the packet.   The do-while 
  loop allows the main loop to always act the same on each packet it 
  processes.   Requires, however, an initialization and cleanup section 
  at top and after exiting the loop.
  */
  /* Although we don't use it this log initialization seems necessary as 
     libmseed functions will dogmatically use the facility */
  ms_rloginit (NULL, NULL, NULL, NULL, 10);
  do
  {
    bool timetear_detected(false),sid_change_detected(false);
    retcode = ms3_readmsr_r (&msfp,&msr, inputfile.c_str(), &fpos, NULL,
                                     flags, verbose);
    switch(retcode)
    {
      case MS_NOERROR:
        try
        {
          current_sid=MSEED_sid(msr->sid);
        }catch(...)
        {
          stringstream ss;
          ss << "source id string="<<current_sid<<" in packet number "<<number_packets_read
                 << " of file "<< inputfile << " could not be decoded but reader did not flag an error"<<endl
                 << "Segment break at this point is likely"<<endl;
              elog.log_error(function_name,ss.str(),ErrorSeverity::Complaint);
          continue;

        }
        /* Land here for normal reads with no error return*/
        if(number_valid_packets == 0)
        {
          /* Initializations needed for first packet in the file */
          last_sid = current_sid;
          stime = msr->starttime;
          current_epoch_stime =  MS_NSTIME2EPOCH(static_cast<double>(stime));
          last_epoch_stime = current_epoch_stime;
          last_packet_samprate = msr->samprate;
          last_dt = 1.0/last_packet_samprate;
          npts = msr->samplecnt;
          last_packet_endtime = current_epoch_stime
                              + static_cast<double>(npts-1)*last_dt;
          segment_starttime = last_epoch_stime;
          start_foff = 0;
        }
        else
        {
          stime = msr->starttime;
          current_epoch_stime = MS_NSTIME2EPOCH(static_cast<double>(stime));
          expected_starttime = last_packet_endtime + last_dt;
          computed_segment_starttime = segment_starttime + last_dt*static_cast<double>(npts);
          if(current_sid!=last_sid)
              sid_change_detected=true;
          if(segment_timetears)
          {
            if( (fabs(current_epoch_stime-expected_starttime)>time_tear_tolerance)
              || (fabs(current_epoch_stime-computed_segment_starttime)>time_tear_tolerance) )
              timetear_detected=true;
            else
              /* This explicit setting isn't essential but makes the logic clearer.*/
              timetear_detected=false;
          }

          if( sid_change_detected || timetear_detected  )
          {
            nbytes=fpos-start_foff;
            ind.net=last_sid.net;
            ind.sta=last_sid.sta;
            ind.chan=last_sid.chan;
            ind.loc=last_sid.loc;
            ind.foff=start_foff;
            ind.nbytes=nbytes;
            ind.starttime=segment_starttime;
            ind.last_packet_time=last_epoch_stime;
            ind.samprate=last_packet_samprate;
            ind.npts=npts;
            ind.endtime=ind.starttime + (static_cast<double>(npts))/ind.samprate;
            indexdata.push_back(ind);
            /* Initate a new segment.  Note the only difference if there was a decoding 
             * error is the index entry will be dropped. */
            last_sid = current_sid;
            last_epoch_stime =  current_epoch_stime;
            last_packet_samprate = msr->samprate;
            last_dt = 1.0/last_packet_samprate;
            npts = msr->samplecnt;
            last_packet_endtime = current_epoch_stime
                              + static_cast<double>(npts-1)*last_dt;
            segment_starttime = current_epoch_stime;
            start_foff = fpos;
          }
          else
          {
            /* Packets without a break land here */
            last_sid = current_sid;
            stime = msr->starttime;
            last_epoch_stime =  MS_NSTIME2EPOCH(static_cast<double>(stime));
            last_packet_samprate = msr->samprate;
            last_dt = 1.0/last_packet_samprate;
            current_npts =  msr->samplecnt;
            npts += current_npts;
            last_packet_endtime = current_epoch_stime
                              + static_cast<double>(current_npts-1)*last_dt;
          }
        }
        ++number_valid_packets;
        data_available = true;
        break;
      case MS_ENDOFFILE:
        if(number_valid_packets>0)
        {
          nbytes=fpos-start_foff;
          ind.net = last_sid.net;
          ind.sta = last_sid.sta;
          ind.chan = last_sid.chan;
          ind.loc = last_sid.loc;
          ind.foff=start_foff;
          ind.nbytes=nbytes;
          ind.starttime=segment_starttime;
          ind.last_packet_time=last_epoch_stime;
          ind.samprate=last_packet_samprate;
          ind.npts=npts;
          ind.endtime=ind.starttime + (static_cast<double>(npts))/ind.samprate;
          indexdata.push_back(ind);
          data_available = false;
        }
        else
        {
          elog.log_error(function_name,
              string("Hit end of file before reading any valid packets\nEmpty index"),
              ErrorSeverity::Invalid);
        }
        break;
      default:
          /* All other error conditions end here.  Function MS_code_to_message
             translates to a rational error message return */
          string message=MS_code_to_message(retcode);
          elog.log_error(function_name,message,ErrorSeverity::Complaint);
          data_available = false;
    };
    ++number_packets_read;
  }while(data_available);
  /* Make sure everything is cleaned up.  Documentation says this is needed
  to invoke the plain C equivalent of a destructor.*/
  ms3_readmsr_r (&msfp, &msr, NULL, NULL, NULL, 0, 0);
  buffer.clear();
  ms_rlog_emit(NULL,0,verbose);
  return MSDINDEX_returntype(indexdata,elog);
}
} // End namespace mspass::io
