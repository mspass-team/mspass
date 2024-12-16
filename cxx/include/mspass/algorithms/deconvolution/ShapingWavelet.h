#ifndef __SHAPING_WAVELET_H__
#define __SHAPING_WAVELET_H__
#include "mspass/utility/Metadata.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
namespace mspass::algorithms::deconvolution{
/*! \brief Frequency domain shaping wavelet.

Frequency domain based deconvolution methods all use a shaping wavelet for
output to avoid ringing.  Frequency domain deconvolution methods in this Library
contain an instance of this object.  In all cases it is hidden behind the interface.
A complexity, however, is that all frequency domain methods will
call the Metadata driven constructor.

This version currently allows three shaping wavelets:  Gaussin, Ricker, and
Slepian0.  The first two are standard.  The last is novel and theoretically
can produce an actual output with the smalle posible sidebands*/
class ShapingWavelet
{
public:
    ShapingWavelet() {
        dt=-1;
        df=-1;
    };
    /*! \brief Construct using a limited set of analytic forms for the wavelet.
     *
     This constructor is used to create a ricker or gaussian shaping wavelet
     with parameters defined by parameters passed through the Metadata object.

     \param md - Metadata object with parameters specifying the wavelet.
     \npts - length of signal to be generated which is the same as the fft
       size for real valued signals.   If set 0 (the default) the constructor
       will attempt to get npts from md using the keyword "operator_nfft".
     */
    ShapingWavelet(const mspass::utility::Metadata& md, int npts=0);
    /* \brief Use a wavelet defined by a TimeSeries object.
     *
     This constructor uses the data stored in a TimeSeries object to define
     the shaping wavelet.   Note to assure output is properly time aligned
     this signal is forced to be symmetric around relative time 0.   That is
     necessary because of the way this, like every fft we are aware of, handles
     phase.   Hence, the user should assure zero time is an appropriate
     zero reference (e.g. a zero phase filter should the dominant peak at 0
     time.)   If the input data size is smaller than the buffer size
     specified the buffer is zero padded.

     \param w - TimeSeries specifying wavelet.   Note dt will be extracted
       and stored in this object.
     \param nfft - buffer size = fft length for frequency domain representation
       of the wavelet.  If ns of d is less than nfft or the time range defined
       by d is does not exceed -(nfft/2)*dt to (nfft2)*dt the result will
       be sero padded before computing the fft. if nfft is 0 the d.ns will
       set the fft length.
       */
    ShapingWavelet(mspass::seismic::CoreTimeSeries d,int nfft=0);
    /*! Construct a Ricker wavelet shaping filter with peak frequency fpeak. */
    ShapingWavelet(const double fpeak,const double dtin, const int n);
    /*! Construct a zero phase Butterworth filter wavelet.

    This is the recommended constructor to use for adjustable bandwidth shaping.
    It is the default for CNR3CDecon.
    \param npolelo is the number of poles for the low corner
    \param f3dblo is the 3db point for the low corner of the passband
    \param nplolehi is the number of poles for the upper corner filter
    \param f3dbhi is the 3db point for the high corner of the passband.
    */
    ShapingWavelet(const int npolelo, const double f3dblo,
                const int npolehi, const double f3dbhi,
                    const double dtin, const int n);
    ShapingWavelet(const ShapingWavelet& parent);
    ShapingWavelet& operator=(const ShapingWavelet& parent);
    /*! Return a pointer to the shaping wavelet this object defines in
     * the frequency domain. */
    ComplexArray *wavelet() {
        return &w;
    };
    /*! Return the impulse response of the shaping filter.   Expect the
     * result to be symmetric about 0 (i.e. output spans nfft/2 to nfft/2.*/
    mspass::seismic::CoreTimeSeries impulse_response();
    double freq_bin_size() {
        return df;
    };
    double sample_interval() {
        return dt;
    };
    std::string type() {
        return wavelet_name;
    };
    int size()const{
      return w.size();
    };
private:
    int nfft;
    /*! Frequency domain form of the shaping wavelet. */
    ComplexArray w;
    double dt,df;
    std::string wavelet_name;
    friend boost::serialization::access;
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & nfft;
        ar & dt;
        ar & df;
        ar & wavelet_name;
        ar & w;
    }
};
}
#endif
