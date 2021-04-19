#include <iomanip>
#include <sstream>
#include "mspass/io/mseed_index.h"
namespace mspass::io
{
using namespace mspass::io;
std::ostringstream& operator<< (std::ostringstream& ss,const mseed_index& ind)
{
  ss << ind.net <<" "
     << ind.sta << " ";
  if(ind.loc.size()>0)
    ss << ind.loc<<" ";
  else
    ss << "NULL ";
  ss << ind.chan<<" "
     << ind.foff<<" "
     << ind.nbytes<<" "
     << std::setprecision(20)
     << ind.starttime<<" "
     << ind.last_packet_time;
  return ss;
};
}
