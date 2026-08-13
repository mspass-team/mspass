#include <algorithm>
#include <errno.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <math.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <time.h>
#include <vector>

#include "libmseed.h"
#include "mspass/io/mseed_index.h"
#include "mspass/utility/ErrorLogger.h"
#include "mspass/utility/MsPASSError.h"

using namespace std;
namespace mspass::io {
using namespace mspass::io;
using mspass::utility::ErrorSeverity;
/*! Inline class used to make a cleaner interface the ugly libmseed function
  for dealing with what they call an "sid".
  */
class MSEED_sid {
public:
  MSEED_sid() {
    string s("");
    net = s;
    sta = s;
    chan = s;
    loc = s;
  }
  MSEED_sid(const char *sid);
  /*! Standard copy constructor.
  \param parent source station id to copy.
  */
  MSEED_sid(const MSEED_sid &parent) {
    net = parent.net;
    sta = parent.sta;
    chan = parent.chan;
    loc = parent.loc;
  };
  /*! SEED network code. */
  string net;
  /*! SEED station code. */
  string sta;
  /*! SEED channel code. */
  string chan;
  /*! SEED location code. */
  string loc;
  /*! Standard assignment operator.
  \param parent source station id to assign from.
  \return reference to this station id after assignment.
  */
  MSEED_sid &operator=(const MSEED_sid &parent) {
    if (this != &parent) {
      net = parent.net;
      sta = parent.sta;
      chan = parent.chan;
      loc = parent.loc;
    }
    return *this;
  };
  /*! Test whether any network, station, channel, or location code differs.
  \param other station id to compare with this object.
  \return true when at least one sid component is different.
  */
  bool operator!=(const MSEED_sid &other) const;
  /*! Write this station id as net:sta:chan:loc.
  \param os output stream to receive the station id.
  \param self station id to write.
  \return output stream after writing self.
  */
  friend ostream &operator<<(ostream &os, MSEED_sid &self) {
    string sep(":");
    os << self.net << sep << self.sta << sep << self.chan << sep << self.loc;
    return os;
  };
};
/*! Constructor for this class.

  Just copies c strings to std::strings in the class.  Throws a simple
  int exception if the libmseed parser returns an error.
  This could be improved if I could make sense of the error logger by
  retrieving the message this obnoxious function posts to its error log.
  */
MSEED_sid::MSEED_sid(const char *sid) {
  char net[16], sta[16], loc[16], chan[16]; // larger than needed but  safe
  /* sid apparently is not const in prototype so we need this cast to
     make it more kosher for this class */
  if (ms_sid2nslc(const_cast<char *>(sid), net, sta, loc, chan) == 0) {
    this->net = string(net);
    this->sta = string(sta);
    this->chan = string(chan);
    this->loc = string(loc);
  } else {
    throw 1;
  }
}
bool MSEED_sid::operator!=(const MSEED_sid &other) const {
  if (this->net == other.net && this->sta == other.sta &&
      this->chan == other.chan && this->loc == other.loc)
    return false;
  else
    return true;
};
/* Using this file scope typedef to avoid the absurdly complex syntax of an
std::pair constructor with complex objects like this */
typedef std::pair<std::vector<mseed_index>, mspass::utility::ErrorLogger>
    MSDINDEX_returntype;
thread_local std::string buffer;
/*! Internal function translates miniseed reader function return codes
  to readable messages used by mseed_file_indexer exceptions.
  */
std::string MS_code_to_message(int retcode) {
  string message(
      "Read error detected by libmseed reader function ms3_readmsr_r\n");
  message += "No file index was returned\n";
  switch (retcode) {
  case MS_GENERROR:
    message += "MS_GENERROR(-1) return - generic unspecified error";
    break;
  case MS_NOTSEED:
    message += "MS_NOTSEED(-2) return - Data not SEED";
    break;
  case MS_WRONGLENGTH:
    message +=
        "MS_WRONGLENGTH(-3) return - Length of data read was not correct";
    break;
  case MS_OUTOFRANGE:
    message += "MS_OUTOFRANGE(-4) return - SEED record length out of range";
    break;
  case MS_UNKNOWNFORMAT:
    message += "MS_UNKNOWNFORMAT(-5) return - data encoding format value in "
               "packet is invalid";
    break;
  case MS_STBADCOMPFLAG:
    message +=
        "MS_STBADCOMPFLAG(-6) return - compression flag value is invalid";
    break;
  case MS_INVALIDCRC:
    message += "MS_INVALIDCRC(-7) return - CRC value in packet is invalid";
    break;
  default:
    message += "Unknown return code - this should not happen and is likely a "
               "version skew problem";
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
 * 2.  Packet errors abort indexing so a damaged file cannot produce a partial
 *     index.
 * 3.  A time mismatch between consecutive packets of more than 1/2 the
 *     previous sample interval triggers a new segment.
 * 4.  A sample-rate change beyond the relative tolerance triggers a new
 *     segment even when the source identifier and time are continuous.
 * \return std::pair   first is a vector of index data.  second is
 *   an ErrorLogger object.  The content of elog should always be tested as
 *   any errors there should be inspected/handled.  A valid empty file
 *   produces an empty vector; a parse failure throws MsPASSError.
 * */
MSDINDEX_returntype mseed_file_indexer(const string inputfile,
                                       const bool segment_timetears,
                                       const bool Verbose) {

  const string function_name("mseed_file_indexer");
  MS3Record *msr = 0;
  /* This thing is used for the thread safe reader.   It uses the common
  plain C implicit signal to alloc the struct it contains when the
  pointer is NULL.   Each call then uses the same data in the msfp
  struct.  The weird cleanup call at the end of the read loop
  calls the equivalent of a destructor.*/
  MS3FileParam *msfp = NULL;
  uint32_t flags = MSF_VALIDATECRC;
  // int8_t ppackets = 0;
  int8_t verbose = 0;
  int retcode;
  // char last_sid[128],current_sid[128];
  MSEED_sid last_sid, current_sid;
  vector<mseed_index> indexdata;

  mspass::utility::ErrorLogger elog;

  /* libmseed reports an empty file as MS_NOTSEED.  Empty input is a valid
     index with no segments, while an unreadable or nonempty damaged file is
     left to the reader so it can report the appropriate parse error. */
  {
    std::ifstream empty_test(inputfile, std::ios::binary | std::ios::ate);
    if (empty_test.is_open() && empty_test.tellg() == 0)
      return MSDINDEX_returntype(indexdata, elog);
  }

  /* Loop over the input file record by record */
  int64_t fpos = 0;
  uint64_t start_foff(0), nbytes(0);
  /* These values have a different time standard structure than
   * epoch times.  These can only be compared with epoch times by
   * calling the function MS_NSTIME2EPOCH
   */
  nstime_t stime;
  int64_t npts(0), last_packet_npts(0), record_length(0);
  uint64_t number_packets_read(0), number_valid_packets(0);
  double last_packet_samprate(0.0), expected_starttime(0.0), last_dt(0.0);
  double segment_starttime(0.0), segment_samprate(0.0);
  /* mseed stores time in an int (I think) this holds float
   * conversions for current and last packet read.*/
  double current_epoch_stime(0.0), last_epoch_stime(0.0);
  nstime_t last_packet_stime(0);
  /* loop break boolean */
  bool data_available(true);
  /* Keep libmseed's verbosity off.  The public Verbose argument controls the
  single-line time-tear diagnostics emitted below.
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
  ms_rloginit(NULL, NULL, NULL, NULL, 10);
  auto append_segment = [&]() {
    mseed_index ind;
    ind.net = last_sid.net;
    ind.sta = last_sid.sta;
    ind.chan = last_sid.chan;
    ind.loc = last_sid.loc;
    ind.foff = start_foff;
    ind.nbytes = nbytes;
    ind.starttime = segment_starttime;
    ind.last_packet_time = last_epoch_stime;
    ind.samprate = segment_samprate;
    ind.npts = npts;
    ind.endtime =
        ind.starttime + (static_cast<double>(npts - 1)) / ind.samprate;
    indexdata.push_back(ind);
  };
  auto cleanup_reader = [&]() {
    ms3_readmsr_r(&msfp, &msr, NULL, NULL, NULL, 0, 0);
    buffer.clear();
    ms_rlog_emit(NULL, 0, verbose);
  };

  try {
    do {
      bool timetear_detected(false), sid_change_detected(false),
          samprate_change_detected(false);
      retcode = ms3_readmsr_r(&msfp, &msr, inputfile.c_str(), &fpos, NULL,
                              flags, verbose);
      switch (retcode) {
      case MS_NOERROR:
        try {
          current_sid = MSEED_sid(msr->sid);
        } catch (...) {
          stringstream ss;
          ss << "source id string=" << msr->sid << " in packet number "
             << number_packets_read + 1 << " of file " << inputfile
             << " could not be decoded but reader did not flag an error";
          throw mspass::utility::MsPASSError(ss.str(), ErrorSeverity::Invalid);
        }
        /* Land here for normal reads with no error return. */
        stime = msr->starttime;
        current_epoch_stime = MS_NSTIME2EPOCH(static_cast<double>(stime));
        record_length = msr->reclen;
        if (number_valid_packets == 0) {
          /* Initializations needed for first packet in the file. */
          last_sid = current_sid;
          last_epoch_stime = current_epoch_stime;
          last_packet_stime = stime;
          last_packet_samprate = msr->samprate;
          segment_samprate = msr->samprate;
          last_dt = 1.0 / last_packet_samprate;
          last_packet_npts = msr->samplecnt;
          npts = msr->samplecnt;
          segment_starttime = last_epoch_stime;
          start_foff = static_cast<uint64_t>(fpos);
        } else {
          if (current_sid != last_sid)
            sid_change_detected = true;

          const long double current_samprate = msr->samprate;
          const long double previous_samprate = last_packet_samprate;
          const long double samprate_tolerance =
              1.0e-12L * std::max(std::fabs(current_samprate),
                                  std::fabs(previous_samprate));
          samprate_change_detected =
              std::fabs(current_samprate - previous_samprate) >
              samprate_tolerance;

          expected_starttime = last_epoch_stime +
                               static_cast<double>(last_packet_npts) * last_dt;
          if (segment_timetears && !sid_change_detected &&
              !samprate_change_detected) {
            const long double timing_error =
                std::fabs(static_cast<long double>(stime - last_packet_stime) *
                              previous_samprate -
                          static_cast<long double>(last_packet_npts) *
                              static_cast<long double>(NSTMODULUS));
            timetear_detected =
                timing_error > 0.5L * static_cast<long double>(NSTMODULUS);
          }

          if (timetear_detected && Verbose) {
            stringstream diagnostic;
            diagnostic << function_name << ": time tear at packet "
                       << number_packets_read + 1 << " SID " << msr->sid
                       << " previous expected time " << setprecision(17)
                       << expected_starttime << " actual start "
                       << current_epoch_stime;
            cerr << diagnostic.str() << '\n';
          }

          if (sid_change_detected || samprate_change_detected ||
              timetear_detected) {
            nbytes = static_cast<uint64_t>(fpos) - start_foff;
            append_segment();
            npts = msr->samplecnt;
            segment_starttime = current_epoch_stime;
            segment_samprate = msr->samprate;
            start_foff = static_cast<uint64_t>(fpos);
          } else {
            /* Packets without a break land here. */
            npts += msr->samplecnt;
          }

          last_sid = current_sid;
          last_epoch_stime = current_epoch_stime;
          last_packet_stime = stime;
          last_packet_samprate = msr->samprate;
          last_dt = 1.0 / last_packet_samprate;
          last_packet_npts = msr->samplecnt;
        }
        ++number_valid_packets;
        data_available = true;
        break;
      case MS_ENDOFFILE:
        if (number_valid_packets > 0) {
          /* fpos is the offset of the last record when EOF is returned. */
          nbytes = static_cast<uint64_t>(fpos) - start_foff + record_length;
          append_segment();
        }
        data_available = false;
        break;
      default:
        /* A damaged input cannot produce a trustworthy partial index. */
        throw mspass::utility::MsPASSError(MS_code_to_message(retcode),
                                           ErrorSeverity::Invalid);
      };
      ++number_packets_read;
    } while (data_available);
  } catch (...) {
    cleanup_reader();
    throw;
  }
  cleanup_reader();
  return MSDINDEX_returntype(indexdata, elog);
}
} // End namespace mspass::io
