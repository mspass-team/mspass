#ifndef __MTPOWERSPECTRUM_ENGINE_H__
#define  __MTPOWERSPECTRUM_ENGINE_H__

#include <memory>
#include <vector>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/seismic/PowerSpectrum.h"

namespace mspass::algorithms::deconvolution{
/*! \brief Multittaper power spectral estimator.

The multitaper method uses averages of spectra windowed by Slepian functions.
This class can be used to compute power spectra.  For efficiency the design
has constructors that build the Slepian functions and cache them in a
private area.  We use this model because computing spectra on a large data
set in parallel will usually be done with a fixed time window.  The expected
use is that normally the engine is created once and passed as an argument to
functions using it in a map operator.

This class uses the apply model for processing.  It accepts raw vector or
TimeSeries data.  The former assumes the sample interval is 1 while the second
scales the spectrum to have units of 1/Hz.
*/
class MTPowerSpectrumEngine
{
public:
  /*! Default constructor.  Do not use as it produces a null object that is no functional.*/
  MTPowerSpectrumEngine();
  /*! \brief construct with full definition.

  This should be the normal constructor used to create this object.  It creates
  and caches the Slepian tapers that are used on calls the apply method.

  \param winsize is the length of time windows in samples the operator will
    be designed to compute.
  \param tbp is the time bandwidth product to use for the operator.
  \param ntapers is the number of tapers to actually use for the operator.
    Note the maximum ntapers is always int(tbp*2).  If ntapers is more than
    2*tbp a mesage will be posted to cerr and ntapers set to tbp*2.
    */
  MTPowerSpectrumEngine(const int winsize, const double tbp, const int ntapers);
  /*! Standard copy constructor*/
  MTPowerSpectrumEngine(const MTPowerSpectrumEngine& parent);
  /*! Destructor.  Not trivial as it has to delete the fft workspace and
  cached tapers. */
  ~MTPowerSpectrumEngine();
  /*! Standard assignment operator. */
  MTPowerSpectrumEngine& operator=(const MTPowerSpectrumEngine& parent);
  /*! \process a TimeSeries.

  This is one of two methods for applying the multiaper algorithm to data.
  This one uses dt and data length to set the Rayleigh bin size (df).   If
  the input data vector length is not the same as the operator length an
  elog complaint is posted to parent.   Short data are processed but should
  be considered suspect unless the sizes differ by only a tiny fraction
  (e.g. and off by one error from rounding).  Long data will be truncated
  on the right (i.e. sample 0 will be the start of the window used).
  The data return will be scaled to psd in units if 1/Hz.

  \param parent is the data to process
  \return vector containing estimated power spwecrum
  */
  mspass::seismic::PowerSpectrum apply(const mspass::seismic::TimeSeries& d);
  /*! \brief Low level processing of vector of data.

  This is lower level function that processes a raw vector of data.   Since
  it does not know the sample interval it cannot compute the rayleigh bin
  size so if callers need that feature they must do that (simple) calculation
  themselves.   Unlike the TimeSeries method this one will throw an
  exception if the input data size does not match the operator size.  It
  returns power spectral density assuming a sample rate of 1.  i.e. it
  scales to correct for the gsl fft scaling by of the forward transform by N. 

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
  gsl_fft_complex_wavetable *wavetable;
  gsl_fft_complex_workspace *workspace;
};
} //namespace ed
#endif
