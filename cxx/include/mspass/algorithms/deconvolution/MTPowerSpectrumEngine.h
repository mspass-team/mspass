#ifndef __MTPOWERSPECTRUM_ENGINE_H__
#define  __MTPOWERSPECTRUM_ENGINE_H__

#include <memory>
#include <vector>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/algorithms/deconvolution/PowerSpectrum.h"

namespace mspass::algorithms::deconvolution{
/*! \brief Multittaper power spectral estimator.

The multitaper method uses averages of spectra windowed by Slepian functions.
This class can be used to compute power spectra.  For efficiency the design
has constructors that build the Slepian functions and cache them in a
private area.  Use of the pply method that returns vector of
power spectral estimates.  Get methods can be called to get frequency
properties of the data returned.  This could have been done with a class, but
that was judged better left to a python wrapper using this class.
*/
class MTPowerSpectrumEngine
{
public:
  MTPowerSpectrumEngine();
  MTPowerSpectrumEngine(const int winsize, const double tbp, const int ntapers);
  MTPowerSpectrumEngine(const MTPowerSpectrumEngine& parent);
  //~MTPowerSpectrumEngine();
  MTPowerSpectrumEngine& operator=(const MTPowerSpectrumEngine& parent);
  /*! \process a TimeSeries.

  This is one of two methods for applying the multiaper algorithm to data.
  This one uses dt and data length to set the Rayleigh bin size (df).   If
  the input data vector length is not the same as the operator length an
  elog complaint is posted to parent.   Short data are processed but should
  be considered suspect unless the sizes differ by only a tiny fraction
  (e.g. and off by one error from rounding).  Long data will be truncated
  on the right (i.e. sample 0 will be the start of the window used).

  \param parent is the data to process
  \return vector containing estimated power spwecrum
  */
  PowerSpectrum apply(const mspass::seismic::TimeSeries& d);
  /*! \brief Low level processing of vector of data.

  This is lower level function that processes a raw vector of data.   Since
  it does not know the sample interval it cannot compute the rayleigh bin
  size so if callers need that feature they must do that (simple) calculation
  themselves.   Unlike the TimeSeries method this one will throw an
  exception if the input data size does not match the operator size.

  \param d is the vector of data to process.  d.size() must this->taperlen() value.
  \return vector containing estimated power spectrum (usual convention with
    0 containing 0 frequency value)
  \exception throw a MsPASSError if the size of d does not match operator length
  */
  std::vector<double> apply(const std::vector<double>& d);
  double df() const {return deltaf;};

  std::vector<double> frequencies();
  /*! Retrieve the taper length.*/
  int taper_length() const
  {
    return taperlen;
  };
  /*! Retrieve time-bandwidth product.*/
  double time_bandwidth_product()  const
  {
    return tbp;
  };
  /*! Return number of tapers used by this engine. */
  int number_tapers() const
  {
    return ntapers;
  };
  /*! \brief PUtter equivalent of df.

  The computation of the Rayleigh bin size (dt) is actually quote trivial but
  this convenience functon allows users of the  vector<double> method to
  handle the comutation easily.  It uses the internal oeprator size, however,
  to compute the df size it returns because the operator is dogmatic about
  using that size.  Users wishing to call the frequency method and the apply
  method on raw vector data need to call this function before calling frequency.

  \param dt is the data sample interval (time domain)

  \return computed df
  */
  double set_df(double dt)
  {
    deltaf=1.0/(dt*static_cast<double>(taperlen));
    return deltaf;
  };
private:
  int taperlen;
  int ntapers;
  double tbp;
  mspass::utility::dmatrix tapers;
  /* Frequency bin interval of last data processed.*/
  double deltaf;
  std::shared_ptr<gsl_fft_complex_wavetable> wavetable;
  std::shared_ptr<gsl_fft_complex_workspace> workspace;
};
} //namespace ed
#endif
