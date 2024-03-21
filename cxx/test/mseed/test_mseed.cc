#include "mspass/io/mseed_index.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/ErrorLogger.h"
using namespace std;
using namespace mspass::io;
using namespace mspass::seismic;
int main(int argc, char **argv)
{
	string fname;
	fname=string(argv[1]);
	std::pair<std::vector<mseed_index>,mspass::utility::ErrorLogger> msfiret;
	std::vector<mseed_index> ind;
	mspass::utility::ErrorLogger elog;

	msfiret=mseed_file_indexer(fname.c_str(),true,true);
	ind=msfiret.first;
	elog=msfiret.second;
	cout << "size of index="<<ind.size()<<endl;
  cout << "Content:"<<endl;
  for(auto x=ind.begin();x!=ind.end();++x)
    cout << x->net
      << " " << x->sta
      << " " << x->chan
      << " " << x->foff
      << " " << x->nbytes
      << " " << x->npts
      << " " << x->samprate
      << std::setprecision(20)
      << " " << x->starttime
      << " " << x->endtime<<endl;
	cout << "Size of error log="<<elog.size()<<endl;
}


