#include <math.h>
#include "mspass/algorithms/deconvolution/dpss.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;


/* Return a vector of n doubles contain a ricker wavelet defined by center
 * frequence fpeak sampled at dt.   Not checking is done to assure dt is
 * rational.   IMPORTANT to note output is circular shifted to zero phase
 * so fft of result will not produce a time shift. */
double *rickerwavelet(float fpeak, float dt, int n)
{
    //modified from http://toto-share.com/2011/05/cc-ricker-wavelets-code/
    int i, k;
    int nw;
    int nc;
    double nw1, alpha, beta;

    nw1 = 2.2/fpeak/dt;
    nw = 2*floor(nw1/2)+1;
    nc = floor(nw/2);
    double *wricker=new double[n];

    for (i=0; i<nw; i++)
    {
        k = i+1;
        alpha = (nc-k+1)*fpeak*dt*M_PI;
        beta = pow(alpha, 2.0);
        wricker[i] = (1 - (beta*2)) * exp(-beta);
    }
    for (i=nw; i<n; i++)
        wricker[i]=0;
    double *ricker=new double[n];
    if(fpeak==0)
    {
        ricker[0]=1.0;
        for(int i=1; i<n; i++)
            ricker[i]=0.0;
    }
    else
    {
        for(i=0; i<nc; i++)
            ricker[n-nc+i]=wricker[i];
        for(i=nc; i<nw; i++)
            ricker[i-nc]=wricker[i];
        for(i=nw-nc; i<n-nc; i++)
            ricker[i]=0;
    }
    delete [] wricker;
    return(ricker);
}

/* Return an array of doubles of length n define a gaussian (wavelet)
 * function with peak width sigma in time domain units   (i.e. sigma
 * is a time duration, not frequency).   Output is uniformly sampled
 * at interval dt and duration (n-1)*dt.  Output is phase shifted to
 * zero phase equivalent so an fft will produce no time shifts.
 * That means the actual gaussian is split with the peak at zero and
 * negative times phase shifted the left of sample n-1 (circular shift
 * of the data vector). */
double *gaussian(float sigma, float dt, int n)
{
    double total=dt*(n-1);
    double tover2=total/2.0;
    double *t=new double[n];
    if(n%2)
    {
        for(int i=0; i<=tover2/dt; i++)
            t[i]=i*dt;
        for(int i=tover2/dt+1; i<n; i++)
            t[i]=-tover2+(i-tover2/dt-1)*dt;
    }
    else
    {
        for(int i=0; i<=n/2; i++)
            t[i]=i*dt;
        for(int i=n/2+1; i<n; i++)
            t[i]=-(n-i)*dt;
    }
    double *gw=new double[n];
    if(sigma==0)
    {
        gw[0]=1.0;
        for(int i=1; i<n; i++)
            gw[i]=0.0;
    }
    else
    {
        for(int i=0; i<n; i++)
            gw[i]=exp(-(t[i]/sigma)*(t[i]/sigma));
    }
    delete [] t;
    return(gw);
}
double *slepian0(double tbp, int n)
{
  double *w=new double[n];
  /* We only need the 0th order taper here so sequ and seql are 0 in this call*/
  dpss_calc(n,tbp,0,0,w);
  /* dpss_calc does not normalize the functions returned so we normalize the
  result to unit L2 norm. */
  double nrm(0.0);
  for(int i=0;i<n;++i) nrm += w[i]*w[i];
  nrm=sqrt(nrm);
  for(int i=0;i<n;++i) w[i]/=nrm;
  return w;
}
} // End namespace
