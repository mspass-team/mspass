#ifndef __MTPOWERSPECTRUM_ENGINE_H__
#define __MTPOWERSPECTRUM_ENGINE_H__

#include "mspass/algorithms/deconvolution/GSLFFTResources.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/dmatrix.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

namespace mspass::algorithms::deconvolution {
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
class MTPowerSpectrumEngine {
public:
  /*! Default constructor.  Do not use as it produces a null object that is no
   * functional.*/
  MTPowerSpectrumEngine();
  /*! \brief construct with full definition.

  This should be the normal constructor used to create this object.  It creates
  and caches the Slepian tapers that are used on calls the apply method.

  \param winsize is the length of time windows in samples the operator will
    be designed to compute.
  \param tbp is the time bandwidth product to use for the operator.
  \param ntapers is the number of tapers to actually use for the operator.
    Note the maximum ntapers is always int(tbp*2).  If ntapers is more than
    2*tbp a message will be posted to cerr and ntapers set to tbp*2.
  \param nfftin is the size of the fft workspace to use for computation.
    When less than the winsize (the default forces this) set to 2*winsize+1.
  \param dtin sets the operator sample interval stored in the object and used
    to compute frequency bin size from fft length.
    */
  MTPowerSpectrumEngine(const int winsize, const double tbp, const int ntapers,
                        const int nfftin = -1, const double dtin = 1.0);
  /*! Standard copy constructor*/
  MTPowerSpectrumEngine(const MTPowerSpectrumEngine &parent);
  /*! Destructor.  Not trivial as it has to delete the fft workspace and
  cached tapers. */
  ~MTPowerSpectrumEngine();
  /*! Standard assignment operator. */
  MTPowerSpectrumEngine &operator=(const MTPowerSpectrumEngine &parent);
  /*! \brief Process a TimeSeries.

  This is one of two methods for applying the multiaper algorithm to data.
  This one uses dt and data length to set the Rayleigh bin size (df).   If
  the input data vector length is not the same as the operator length an
  elog complaint is posted to parent.   Short data are processed but should
  be considered suspect unless the sizes differ by only a tiny fraction
  (e.g. and off by one error from rounding).  Long data will be truncated
  on the right (i.e. sample 0 will be the start of the window used).
  The data return will be scaled to psd in units if 1/Hz.

  \param d is the data to process
  \return vector containing estimated power spwecrum
  */
  mspass::seismic::PowerSpectrum apply(const mspass::seismic::TimeSeries &d);
  /*! \brief Low level processing of vector of data.

  This is lower level function that processes a raw vector of data.   Since
  it does not know the sample interval it cannot compute the rayleigh bin
  size so if callers need that feature they must do that (simple) calculation
  themselves.   Unlike the TimeSeries method this one will throw an
  exception if the input data size does not match the operator size.  It
  returns power spectral density assuming a sample rate of 1.  i.e. it
  scales to correct for the gsl fft scaling by of the forward transform by N.

  \param d is the vector of data to process.  d.size() must this->taperlen()
  value.
  \return vector containing estimated power spectrum (usual convention with
    0 containing 0 frequency value)
  \exception throw a MsPASSError if the size of d does not match operator length
  */
  std::vector<double> apply(const std::vector<double> &d);
  /*! Return the frquency bin size defined for this operator. */
  double df() const { return deltaf; };
  /*! Return and std::vector of all frequencies for spectral estimates this
  operator computes. */
  std::vector<double> frequencies();
  /*! Retrieve the taper length.*/
  int taper_length() const { return taperlen; };
  /*! Retrieve time-bandwidth product.*/
  double time_bandwidth_product() const { return tbp; };
  /*! Return number of tapers used by this engine. */
  int number_tapers() const { return ntapers; };
  /*! Return size of fft used by this operator - usually not the same as taper
  length.*/
  int fftsize() const { return nfft; };
  /*! Retrieve the internally cached required data sample interval. */
  double dt() const { return operator_dt; };
  /*! \brief Putter equivalent of df.

  The computation of the Rayleigh bin size is complicated a bit by the folding
  properties of fft algorithms that have to handle odd and even length
  inputs differently.   This algorithm uses the internally set nfft
  value to set the frequency bin size for even or odd nfft and the input sample
  interval.  NOTE POSSIBLE CONFUSION that input is time sample interval
  NOT the actual frquency bin size.  The reason is that the odd/even issue
  makes df dependent on if the fft size is even or odd.   We include this
  method as a convenience as that is an implementation detail for the fft
  algorithm.

  Note also this method sets not just df but the internally stored sample
  interval (symbol operator_dt in the source code.)

  \param dt is the data sample interval (time domain)

  \return computed df
  */
  double set_df(double dt) {
    const std::string caller("MTPowerSpectrumEngine::set_df");
    if (!std::isfinite(dt) || dt <= 0.0)
      throw mspass::utility::MsPASSError(
          caller + ": sample interval must be finite and positive",
          mspass::utility::ErrorSeverity::Invalid);
    const int this_nf = this->nf();
    if (this_nf <= 1)
      throw mspass::utility::MsPASSError(
          caller + ": engine fft length is not configured",
          mspass::utility::ErrorSeverity::Invalid);
    const double fny = 1.0 / (2.0 * dt);
    const double new_deltaf = fny / static_cast<double>(this_nf - 1);
    if (!std::isfinite(new_deltaf) || new_deltaf <= 0.0)
      throw mspass::utility::MsPASSError(
          caller + ": sample interval produces an invalid frequency spacing",
          mspass::utility::ErrorSeverity::Invalid);
    this->operator_dt = dt;
    this->deltaf = new_deltaf;
    return deltaf;
  };
  /*! Return tne number of frequency bins in estimates the operator will
   * compute. */
  int nf() {
    /* this simple formula depends upon integer truncation when used with
    nfft as an odd number.   For reference, this is what prieto uses in
    the python multitaper package:
    if (nfft%2 == 0):
        nf = int(nfft/2 + 1)
    else:
        nf = int((nfft+1)/2)
    they will yield the same result but this is simpler and faster
    */
    return (this->nfft) / 2 + 1;
  };

private:
  int taperlen;
  int ntapers;
  int nfft;
  double tbp;
  double operator_dt;
  mspass::utility::dmatrix tapers;
  /* Frequency bin interval of last data processed.*/
  double deltaf;
  gsl_fft_complex_wavetable *wavetable;
  gsl_fft_complex_workspace *workspace;
  friend boost::serialization::access;
  template <class Archive>
  void save(Archive &ar, const unsigned int version) const {
    ar & taperlen;
    ar & ntapers;
    ar & nfft;
    ar & tbp;
    ar & operator_dt;
    ar & tapers;
    ar & deltaf;
  }
  template <class Archive> void load(Archive &ar, const unsigned int version) {
    int loaded_taperlen;
    int loaded_ntapers;
    int loaded_nfft;
    double loaded_tbp;
    double loaded_operator_dt;
    mspass::utility::dmatrix loaded_tapers;
    double loaded_deltaf;
    ar & loaded_taperlen;
    ar & loaded_ntapers;
    ar & loaded_nfft;
    ar & loaded_tbp;
    ar & loaded_operator_dt;
    ar & loaded_tapers;
    ar & loaded_deltaf;

    const std::string caller("MTPowerSpectrumEngine serialization load");
    if (loaded_nfft <= 0 || loaded_taperlen <= 0 || loaded_ntapers <= 0)
      throw mspass::utility::MsPASSError(
          caller + ": archived fft, taper, and taper-count lengths must be "
                   "positive",
          mspass::utility::ErrorSeverity::Invalid);
    if (loaded_nfft < loaded_taperlen)
      throw mspass::utility::MsPASSError(
          caller + ": archived fft length is shorter than taper length",
          mspass::utility::ErrorSeverity::Invalid);
    if (!std::isfinite(loaded_tbp) || loaded_tbp <= 0.0 ||
        !std::isfinite(loaded_operator_dt) || loaded_operator_dt <= 0.0 ||
        !std::isfinite(loaded_deltaf) || loaded_deltaf <= 0.0)
      throw mspass::utility::MsPASSError(
          caller + ": archived spectral parameters must be finite and "
                   "positive",
          mspass::utility::ErrorSeverity::Invalid);
    if (!loaded_tapers.storage_is_consistent() ||
        loaded_tapers.rows() != static_cast<size_t>(loaded_ntapers) ||
        loaded_tapers.columns() != static_cast<size_t>(loaded_taperlen))
      throw mspass::utility::MsPASSError(
          caller + ": archived taper matrix dimensions are inconsistent",
          mspass::utility::ErrorSeverity::Invalid);
    auto resources = detail::AllocateGSLFFTResources(loaded_nfft, caller);

    loaded_tapers.swap(tapers);
    if (wavetable != nullptr)
      gsl_fft_complex_wavetable_free(wavetable);
    if (workspace != nullptr)
      gsl_fft_complex_workspace_free(workspace);
    taperlen = loaded_taperlen;
    ntapers = loaded_ntapers;
    nfft = loaded_nfft;
    tbp = loaded_tbp;
    operator_dt = loaded_operator_dt;
    deltaf = loaded_deltaf;
    wavetable = resources.wavetable.release();
    workspace = resources.workspace.release();
  }
  BOOST_SERIALIZATION_SPLIT_MEMBER()
};
} // namespace mspass::algorithms::deconvolution
#endif
