#ifndef _POWER_SPECTRUM_H_
#define _POWER_SPECTRUM_H_
#include "mspass/seismic/BasicSpectrum.h"
#include "mspass/utility/ErrorLogger.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include <vector>
namespace mspass::seismic {
/*! Class defining the concept of a power psectrum. */
class PowerSpectrum : public mspass::seismic::BasicSpectrum,
                      public mspass::utility::Metadata {
public:
  /*! Descriptive name assigned by creator of algorithm used to generate it.

  There are a number of different algorithms to generate power spectra.  this
  name field should be used to set a unique name for a given algorithm.
  */
  std::string spectrum_type;
  /*! Vector of spectral estimates.  spectrum[0] == estimate at f0. */
  std::vector<double> spectrum;
  /*! MsPASS Error logging class.

  MsPASS uses an error logger to allow posting of error messages that
  go with the data.  That approach is needed in map reduce to allow the
  error messages to be cleanly preserved. */
  mspass::utility::ErrorLogger elog;
  /*! Default constructor.   Makes am empty, dead datum. */
  PowerSpectrum();
  /*! Standard constructor template.

  This constructor is a template largely to allow vectors of data other than
  double as inputs.   If the input is not double it is converted on input
  to an std::vector<double> container stored in the spectrum attribute.

  \param md contents of this Metadata container are copied to the result.
  \param d vector of sample data to load into spectrum array.
  \param dfin frequency bin size (sample interval)
  \param nm name defining algorithm used to constuct this spectral estimate.
  \param f0in frequency of component 0 of input (default is 0.0).
  \param dtin parent sampell interval of data used to make this estimate
  \param npts_in number of points in parent time series used to compute
    this estimate.
  */

  template <class T>
  PowerSpectrum(const mspass::utility::Metadata &md, const std::vector<T> &d,
                const double dfin, const std::string nm, const double f0in,
                const double dtin, const int npts_in);
  PowerSpectrum(const PowerSpectrum &parent);
  PowerSpectrum &operator=(const PowerSpectrum &parent);
  /*! \brief Standard accumulation operator.

  Sometimes we need to sum power spectra.  Type examplel would be
  total noise amplitude on a 3C seismogram or average noise amplitude in
  an array of instruments.   This can be used to build such sum in
  the usual way.  Add spectral elements sample by sample.

  \exception will throw a MsPaSSError if the left and right side
  are not equal length of have different f0 of df values.*/
  PowerSpectrum &operator+=(const PowerSpectrum &other);
  /*! \brief Compute amplitude spectrum from power spectrum.

  The amplitude spectrum is sqrt of the power values.  This is a
  convenience class to return the values in that form. */
  std::vector<double> amplitude() const;
  /*! \brief power at a given frequency.

  Returns the power estimate at a specified frequency.  Uses a linear
  interpolation between nearest neighbors.  Returns the frequency exceeds
  the Nyquist silently returns 0.

  \param f is the frequency for which amplitude is desired.

  \exception MsPASSErorr object will be throw f f is less than 0.
  */
  double power(const double f) const;

  double frequency(const int sample_number) const {
    const std::string base_error("PowerSpectrum::frequency:  ");
    if (sample_number < 0)
      throw mspass::utility::MsPASSError(
          base_error + "Sample number parameter passed cannot be negative");
    if (sample_number >= (this->nf()))
      throw mspass::utility::MsPASSError(
          base_error +
          "Sample number parameter passed xceeds range of spectrum array");
    return this->f0() + sample_number * this->df();
  };
  std::vector<double> frequencies() const;
  /* \brief Return number of frequencies in estimate (size of spectrum vector).

  Users should be aware that when zero padding is used the size of the
  spectrum vector will be larger than half the number of time series samples.*/
  size_t nf() const { return spectrum.size(); };
  /*! Return the nyquist frequency for this estimate. */
  double Nyquist() const { return 1.0 / (2.0 * this->parent_dt); };
};
/*! Fully parameterized constructor template.

See class definition of PowerSpectrum for usage.  Template to allow
data vector of multiple types.
*/

template <class T>
PowerSpectrum::PowerSpectrum(const mspass::utility::Metadata &md,
                             const std::vector<T> &d, const double dfin,
                             const std::string nm, const double f0in,
                             const double dtin, const int npts_in)
    : BasicSpectrum(dfin, f0in, dtin, npts_in), mspass::utility::Metadata(md),
      elog() {
  spectrum_type = nm;
  spectrum.reserve(d.size());
  for (size_t k = 0; k < d.size(); ++k)
    spectrum.push_back(static_cast<double>(d[k]));
  this->set_live();
};
} // namespace mspass::seismic
#endif
