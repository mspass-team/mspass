#ifndef _MSEED_INDEX_H_
#define _MSEED_INDEX_H_
#include "mspass/utility/ErrorLogger.h"
#include <string>
#include <vector>
namespace mspass::io {

class mseed_index {
public:
  std::string net;
  std::string sta;
  std::string loc;
  std::string chan;
  size_t foff;
  size_t nbytes;
  size_t npts;
  double samprate;
  double starttime;
  double endtime;
  double last_packet_time;
  /* These aren't really essential because the compiler should automatically
  generate them, but better to be explicit since the std::vector demands them*/
  mseed_index() {
    net = "";
    sta = "";
    loc = "";
    chan = "";
    foff = 0;
    nbytes = 0;
    npts = 0;
    samprate = 0.0;
    starttime = 0.0;
    endtime = 0.0;
    last_packet_time = 0.0;
  };
  mseed_index(const mseed_index &parent)
      : net(parent.net), sta(parent.sta), loc(parent.loc), chan(parent.chan) {
    foff = parent.foff;
    nbytes = parent.nbytes;
    npts = parent.npts;
    samprate = parent.samprate;
    starttime = parent.starttime;
    endtime = parent.endtime;
    last_packet_time = parent.last_packet_time;
  };
  mseed_index &operator=(const mseed_index &parent) {
    if (&parent != this) {
      net = parent.net;
      sta = parent.sta;
      loc = parent.loc;
      chan = parent.chan;
      foff = parent.foff;
      npts = parent.npts;
      nbytes = parent.nbytes;
      samprate = parent.samprate;
      starttime = parent.starttime;
      endtime = parent.endtime;
      last_packet_time = parent.last_packet_time;
    }
    return *this;
  };
  friend std::ostringstream &operator<<(std::ostringstream &ss,
                                        const mseed_index &ind);
};
/*! \brief Construct an index for a miniseed file.

Miniseed is a packetized data format in which time series data are
packaged into packets of a fixed size with a minimal header needed to
uniquely define the contents.  Because of that format it is possible
and common practice to concatenate miniseed files with packets arranged
in time sequence together.  That is particularly essential for large data
sets and on HPC file systems that have performance problems with many
small files.   This function was written to build an index for such files
to provide a means for a reader to efficiently find a particular piece of
data and decode the miniseed packets into TimeSeries objects.  In mspass
this function would, to most users, be treated as under the hood and
of interest only if something breaks.

\param inputfile is the miniseed file to be indexed.
\param segment_timetears is a boolean that controls the behavior when a time
  tear is encountered.  A time tear is defined as a mismatch in the computed
  endtime of the last packet read and the current packet starttime differing
  by more than 1/2 a sample.  When false these are ignored assuming the
  reader will handle the problem by some form of gap processing.  When true
  a new index entry will be created at the time tear.  Always use true
  if there is any possibility of the same channel of data in the file
  in consecutive packets that aren't an actual time tear in this sense.
  e.g. event data concenated so all channels are back to back would require
  using this parameter true.
\param Verbose is a boolean largely controlling how time tears are or are not
  logged.  That is, at present if this parameter is true any time the logic
  detects a time tear it is logged in the returned error log as an informational
  log message.   If false only reading errors for things like garbled miniseed
  packets are logged.
\param return is an std::pair.  "First" contains a vector of objects
  called mseed_index that contain the basic information defining an index for
  inputfile.  See class description of mseed_index for more details. "second"
  contains an ErrorLogger objects.  Caller should test that the contents are
  empty and if not save the error log or print it.
*/
std::pair<std::vector<mseed_index>, mspass::utility::ErrorLogger>
mseed_file_indexer(const std::string inputfile, const bool segment_timetears,
                   const bool Verbose);

} // namespace mspass::io
#endif
