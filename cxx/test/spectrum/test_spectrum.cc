#include <vector>
#include <cfloat>
#include <time.h>
#include <assert.h>
#include "mspass/algorithms/deconvolution/wavelet.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/algorithms/deconvolution/dpss.h"
#include "misc/blas.h"


/* Hack test */
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms::deconvolution;
using namespace mspass::utility;

/* convience function for testing dpss - returns a matrix of Slepian
functions in the columns of the matrix.   It prints timing data
outside the copy loop to estimate the base cost of computing the slepian
functions for increasing array sizes.

Note the dmatrix returned here puts the tapers in the columns of the matrix.
MTPowerSpectrumEngine uses the transpose. */
dmatrix *compute_slepians(int n, double tbp, int ntapers)
{
  dmatrix *result=new dmatrix(n,ntapers);
  double *work = new double[ntapers*n];
  clock_t stime,etime;
  double cpu_time;

  stime = clock();
  dpss_calc(n,tbp,0,ntapers-1,work);
  etime = clock();
  cpu_time= ((double) (etime-stime)) / CLOCKS_PER_SEC;
  cout << "dpss_calc compute time="<<cpu_time
    <<" for Vector length="<<n<<" ntapers="<<ntapers<<endl;
  for(auto j=0,ii=0;j<ntapers;++j)
  {
    /* This construct is more than a little weird.   Done because I didn't
    want to return a reference to the large matrix so have ths pointer stuff*/
    double *ptr;
    for(auto i=0;i<n;++i,++ii)
    {
      ptr = result->get_address(i,j);
      *ptr = work[ii];
    }
  }
  delete [] work;
  return result;
}
/* This small test function was added to validate this code relative to the
current standard of German Prieto's mtspec library.   His library has two
features this function tests.
1.   mtspec always normalizes eigentapers by 1/eigenvalue from the \
     eigenvalue equation used to compute the eigentapers.  The dpss library
     we use in mspass normalizes the tapers by the L2 norm of each vector.
     This validates that the tapers dpss produces really are orthogonal.
2.   mtspec has an automatic feature to use a spline method to compute the
     eigentapers when the number of samples is huge.  Calling this function
     on a huge vector validates that the tapers returned with huge vectors
     by dpss are not distorted.
Both of these issues are motivated by a mismatch in results I obtained with
this implementation and mtspec.  Note we didn't choose to add mtspec to the
mspass container because mtspec object aren't serializable since the
native code is FORTRAN90.
*/
void test_dpss_othogonality(int n,double tbp,int ntapers)
{
  int i,j;
  dmatrix *tapers=compute_slepians(n,tbp,ntapers);
  /* First test normalization to 1.0 */
  cout << "L2 norm of eigentapers - should all be close to 1.0"<<endl;
  for(j=0;j<ntapers;++j)
  {
    double nrm;
    nrm = dnrm2(n,tapers->get_address(0,j),1);
    cout << nrm << " ";
    double testval;
    // conservative test but seems necessary
    testval=fabs(1.0-nrm)/sqrt((double)n);
    assert(testval<DBL_EPSILON);
  }
  cout << endl;
  cout << "All diagonal - these should all be zero"<<endl;
  for(j=0;j<ntapers-1;++j)
  {
    for(i=j+1;i<ntapers;++i)
    {
      double dotij = ddot(n,tapers->get_address(0,j),1,tapers->get_address(0,i),1);
      cout << "(" << j << ","<<i<<")="<<dotij<<" ";
      double testval;
      // conservative test but seems necessary
      testval=fabs(dotij)/sqrt((double)n);
      assert(testval<DBL_EPSILON);
    }
  }
  /* This is a hack, temporary step used to plot the tapers */
  ofstream ofs;
  ofs.open("tapers.csv",std::ofstream::out);
  ofs << *tapers;
  delete tapers;
}
int main(int argc, char **argv)
{
  cout << "mspass spectrum test program starting"<<endl
    << "Trying default constructor for MTPowerSpectrumEngine"<<endl;
  MTPowerSpectrumEngine mtemp;
  cout << "Trying main constructor for 3 TBP values"<<endl;
  MTPowerSpectrumEngine mtpse2(512,2.5,4,1024,0.01);
  MTPowerSpectrumEngine mtpse4(512,4,8,1024,0.01);
  /* explicit test of nondefault value for arg3 */
  MTPowerSpectrumEngine mtpse5(512,5,10,1024,0.01);
  assert(mtpse2.dt()==0.01);
  assert(mtpse4.dt()==0.01);
  assert(mtpse5.dt()==0.01);
  cout << "df="<<mtpse2.df()<<endl;
  /* for this test assume if this works for one it works for all */
  double testdf = 50.0/static_cast<double>(mtpse2.nf()-1);
  assert(mtpse5.df()==testdf);
  double *gtmp,*rtmp;
  gtmp=gaussian(10.0,1.0,512);
  rtmp=rickerwavelet(5.0,0.01,512);
  vector<double> g,r;
  int i,nfft;
  nfft=mtpse2.fftsize();
  assert(nfft==1024);
  nfft=mtpse4.fftsize();
  assert(nfft==1024);
  nfft=mtpse5.fftsize();
  assert(nfft==1024);
  for(i=0;i<512;++i)
  {
    g.push_back(gtmp[i]);
    r.push_back(rtmp[i]);
  }
  cout << "Trying apply method for raw vector of doubles"<<endl;
  vector<double> G,f;
  G=mtpse5.apply(g);
  f=mtpse5.frequencies();
  cout << "Results (f,power)"<<endl;
  for(i=0;i<G.size();++i)
  {
    cout << f[i]<<" "<<G[i]<<endl;
  }
  cout << "Trying TimeSeries apply method"<<endl;
  TimeSeries ts(200);
  ts.set_t0(0.0);
  ts.set_dt(0.01);
  ts.set_tref(TimeReferenceType::Relative);
  ts.set_npts(200);
  ts.set_live();
  ts.s=g;
  PowerSpectrum ps2,ps4,ps5;
  ps2=mtpse2.apply(ts);
  assert(ps2.live());
  ps4=mtpse4.apply(ts);
  assert(ps4.live());
  ps5=mtpse5.apply(ts);
  assert(ps5.live());
  f=mtpse2.frequencies();
  cout << "Results f,tbp25,tbp4,tbp5"<<endl;
  for(i=0;i<f.size();++i)
  {
    cout << ps2.frequency(i)<<" "<<ps2.spectrum[i]
      << " "<<ps4.spectrum[i]
      << " "<<ps5.spectrum[i]<<endl;
  }
  cout << "Testing interpolation for power method"<<endl;
  int nfreq2=2*f.size();
  double df2=ps2.df()/2.0;
  for(i=0;i<nfreq2;++i)
  {
      double f2=ps2.f0()+i*df2;
      double amp=sqrt(ps2.power(f2));
      cout << f2 << " "<<amp<<endl;
  }
  /*  New test for orthogonality of slepian functions for range of
  vector sizes */
  cout << "Verifying slepian function (dpss) library"<<endl;
  cout << "Test for 50000 sample window"<<endl;
  /* run these tests with a fixed tbp and numbe of tapers */
  const double tbp(5.0);
  const int ntapers(8);
  //test_dpss_othogonality(10000,tbp,ntapers);
  /* Ran this test for 10000, 50000, 10000, and 1000000.  100,000 and 
  1,000,000 gave garbage tapers.  50000 seems ok so we use that as the 
  ceiling in the MsPASS power spectrum estimator using the dpss library.
  Prieto uses scipy's implementation that seems to do like his old f90 
  code and use a different algorithm for long time series*/
  test_dpss_othogonality(50000,tbp,ntapers);
}
