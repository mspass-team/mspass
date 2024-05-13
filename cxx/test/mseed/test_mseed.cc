#include <assert.h>
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
  /* these tests are keyed to one and only one test file.  
     They will need to be changed if the file is changed
     to add tests for problem data - may be needed in 
     the future */
  assert(ind.size()==5);
  assert(ind[0].sta=="E2000");
  assert(ind[0].chan=="VHE");
  assert(ind[0].foff==0);
  assert(ind[0].nbytes==4096);
  assert(ind[0].npts==70);
  assert(fabs(ind[0].samprate-0.1)<0.0001);
  assert(ind[1].sta=="E2000");
  assert(ind[1].foff==4096);
  assert(ind[1].npts==2434403);
  assert(ind[2].sta=="A2000");
  assert(ind[2].chan=="UHE");
  assert(ind[2].foff==1921024);
  assert(ind[2].nbytes==4096);
  assert(ind[2].npts==15);
  assert(fabs(ind[2].samprate-0.01)<0.0001);
  assert(ind[3].sta=="A2000");
  assert(ind[3].foff==1925120);
  assert(ind[3].npts==47205);
}


