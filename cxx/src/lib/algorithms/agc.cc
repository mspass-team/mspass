#include <stdlib.h>
#include <math.h>
#include <string>
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/algorithms.h"
namespace mspass::algorithms
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

/* This uses the same algorithm as seismic unix BUT with a vector ssq
 * instead of the scalar form.   Returns a gain function a the same sample
 * rate as teh original data with the gain factor applied to each 3c sample.
 * The gain is averaged over scale twin ramping on and off using the same
 * cumulative approach used in seismic unix algorithm. */
/* This function uses the same algorith as seismic unix BUT with a vector ssq
   instead of the scalar form used for a simple time series.  Returns a
   gain function at the same sample rate as the original data.  The original
   data can then be restored by scaling each vector sample by 1/gain at
   each sample.   */
TimeSeries agc(Seismogram& d, const double twin)
{
    try{
        /* First deal with the processing history */
        dmatrix agcdata(3,d.npts());
        double val,rms,ssq,gain,lastgain;
        size_t i,k;
        CoreTimeSeries gf(dynamic_cast<BasicTimeSeries& >(d),
                dynamic_cast<Metadata&>(d));
        gf.set_t0(d.t0()+gf.dt());
        gf.set_npts(d.npts());
        /* this is inefficient but needed to mesh with older push_back algorithm. */
        gf.s.clear();
        int nwin,iwagc;
        nwin=round(twin/(d.dt()));
        iwagc=nwin/2;
        if(iwagc<=0)
        {
            d.elog.log_error("agc","Illegal gain time window - resolves to less than one sample",
                ErrorSeverity::Invalid);
            return TimeSeries();
        }
        if(iwagc>d.npts()) iwagc=d.npts();
        /* First compute sum of squares in initial wondow to establish the
         * initial scale */
        for(i=0,ssq=0.0;i<iwagc;++i)
        {
            for(k=0;k<3;++k)
            {
                val=d.u(k,i);
                ssq+=val*val;
            }
        }
        int normalization;
        normalization=3*iwagc;
        rms=ssq/((double)normalization);
        if(rms>0.0)
        {
            gain=1.0/sqrt(rms);
            for(k=0;k<3;++k)
            {
                agcdata(k,0) = gain*d.u(k,0);
            }
            gf.s.push_back(gain);
        }
        else
        {
            gf.s.push_back(0.0);
            lastgain=0.0;
        }
        for(i=1;i<=iwagc;++i)
        {
            for(k=0;k<3;++k)
            {
                val=d.u(k,i+iwagc);
                ssq+=val*val;
                ++normalization;
            }
            rms=ssq/((double)normalization);
            if(rms>0.0)
            {
                lastgain=gain;
                gain=1.0/sqrt(rms);
            }
            else
            {
                if(lastgain==0.0)
                    gain=0.0;
                else
                    gain=lastgain;

            }
            gf.s.push_back(gain);
            lastgain=gain;
            for(k=0;k<3;++k) agcdata(k,i) = gain*d.u(k,i);
        }
        int isave;
        for(i=iwagc+1,isave=iwagc+1;i<d.npts()-iwagc;++i,++isave)
        {
           for(k=0;k<3;++k)
           {
               val=d.u(k,i+iwagc);
               ssq+=val*val;
               val=d.u(k,i-iwagc);
               ssq-=val*val;
           }
           rms=ssq/((double)normalization);
            if(rms>0.0)
            {
                lastgain=gain;
                gain=1.0/sqrt(rms);
            }
            else
            {
                if(lastgain==0.0)
                    gain=0.0;
                else
                    gain=lastgain;

            }
            gf.s.push_back(gain);
            lastgain=gain;
            for(k=0;k<3;++k) agcdata(k,i) = gain*d.u(k,i);
        }
        /* ramping off */
        for(i=isave;i<d.npts();++i)
        {
            for(k=0;k<3;++k)
            {
                val=d.u(k,i-iwagc);
                ssq -= val*val;
                --normalization;
            }
            rms=ssq/((double)normalization);
            if(rms>0.0)
            {
                lastgain=gain;
                gain=1.0/sqrt(rms);
            }
            else
            {
                if(lastgain==0.0)
                    gain=0.0;
                else
                    gain=lastgain;

            }
            gf.s.push_back(gain);
            lastgain=gain;
            for(k=0;k<3;++k) agcdata(k,i) = gain*d.u(k,i);
        }
        d.u=agcdata;
        gf.set_live();
        gf.set_npts(gf.s.size());
        return gf;
    }catch(...){
        string uxperr("Something threw an unexpected exception");
        d.elog.log_error("agc",uxperr,ErrorSeverity::Invalid);
        /* Return an empty TimeSeries object in this case. */
        return TimeSeries();
    }
}
}// End mspass namespace
