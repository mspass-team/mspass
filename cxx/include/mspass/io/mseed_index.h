#ifndef _MSEED_INDEX_H_
#define _MSEED_INDEX_H_
#include <vector>
#include <string>
namespace mspass::io
{

class mseed_index
{
public:
  std::string net;
  std::string sta;
  std::string loc;
  std::string chan;
  size_t foff;
  size_t nbytes;
  double starttime;
  double last_packet_time;
  /* These aren't really essential because the compiler should automatically
  generate them, but better to be explicit since the std::vector demands them*/
  mseed_index()
  {
    net="";
    sta="";
    loc="";
    chan="";
    foff=0;
    nbytes=0;
    starttime=0.0;
    last_packet_time=0.0;
  };
  mseed_index(const mseed_index& parent) : net(parent.net),sta(parent.sta),
     loc(parent.loc),chan(parent.chan)
  {
    foff=parent.foff;
    nbytes=parent.nbytes;
    starttime=parent.starttime;
    last_packet_time=parent.last_packet_time;
  };
  mseed_index& operator=(const mseed_index& parent)
  {
    if(&parent != this)
    {
      net=parent.net;
      sta=parent.sta;
      loc=parent.loc;
      chan=parent.chan;
      foff=parent.foff;
      nbytes=parent.nbytes;
      starttime=parent.starttime;
      last_packet_time=parent.last_packet_time;
    }
    return *this;
  };
  friend std::ostringstream& operator<< (std::ostringstream& ss,const mseed_index& ind);
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
\param return is a vector of objects called mseed_index that contain 
  the basic information defining an index for inputfile.  See 
  class description for more details. 
*/
std::vector<mseed_index> mseed_file_indexer(const std::string inputfile);

} // end namespace 
#endif
