#include "mspass/algorithms/deconvolution/ScalarDecon.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::utility;

ScalarDecon::ScalarDecon(const Metadata &md) : shapingwavelet(md)
{
    try {
        /* This has to be defined or the shapingwavlet constructor will
         * fail in the current implementation */
        int nfft=md.get_int("operator_nfft");
        wavelet.reserve(nfft);
        data.reserve(nfft);
        result.reserve(nfft);
    } catch(...) {
        throw;
    };
}
ScalarDecon::ScalarDecon(const vector<double>& d, const vector<double>& w)
    : data(d),wavelet(w)
{
    result.reserve(data.size());
}
ScalarDecon::ScalarDecon(const ScalarDecon& parent)
    : data(parent.data),wavelet(parent.wavelet),result(parent.result)
{
}
ScalarDecon& ScalarDecon::operator=(const ScalarDecon& parent)
{
    if(this!=&parent)
    {
        wavelet=parent.wavelet;
        data=parent.data;
        result=parent.result;
    }
    return *this;
}
int ScalarDecon::load(const vector<double> &w,const vector<double> &d)
{
    wavelet=w;
    data=d;
    result.clear();
    return 0;
}
/* this an next method are normally called back to back.  To assure
stability we must have both call the clear method for result.   Could
do that in only loadwavelet, but that could produce funny bugs downsteam
so we always clear for stability at a small cost */
int ScalarDecon::loaddata(const vector<double> &d)
{
  data=d;
  result.clear();
  return 0;
}
int ScalarDecon::loadwavelet(const vector<double> &w)
{
  wavelet=w;
  result.clear();
  return 0;
}
void ScalarDecon::changeparameter(const Metadata& md)
{
    shapingwavelet=ShapingWavelet(md);
}
void ScalarDecon::change_shaping_wavelet(const ShapingWavelet& nsw)
{
    shapingwavelet=nsw;
}
}  //End namespace
