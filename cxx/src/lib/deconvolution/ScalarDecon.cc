#include "mspass/deconvolution/ScalarDecon.h"
using namespace mspass;
namespace mspass{
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
void ScalarDecon::changeparameter(const Metadata& md)
{
    shapingwavelet=ShapingWavelet(md);
}
void ScalarDecon::change_shaping_wavelet(const ShapingWavelet& nsw)
{
    shapingwavelet=nsw;
}
}  //End namespace
