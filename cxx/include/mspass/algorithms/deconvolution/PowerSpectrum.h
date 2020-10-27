#ifndef _POWER_SPECTRUM_H_
#define _POWER_SPECTRUM_H_
#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/ErrorLogger.h"
namespace mspass::algorithms::deconvolution{
class PowerSpectrum : public mspass::utility::Metadata
{
public:
  /* The data are public here in keeping with philosophy of TimeSeries and
  Seismogram.  See MsPaSS documentation for motivation.  */
  double df;
  double f0;
  std::string spectrum_type;
  std::vector<double> spectrum;
  mspass::utility::ErrorLogger elog;
  PowerSpectrum();
  template <class T> PowerSpectrum(const mspass::utility::Metadata& md,
    const std::vector<T>& d,double dfin, std::string nm);
  PowerSpectrum(const PowerSpectrum& parent);
  PowerSpectrum& operator=(const PowerSpectrum& parent);
  /*! \brief Standard accumulation operator.

  Sometimes we need to sume power spectra.  Type examplel would be
  total noise amplitude on a 3C seismogram or average noise amplitude in
  an array of instruments.   This can be used to build such sum in
  the usual way.  Add spectral elements sample by sample.

  \exception will throw a MsPaSSError if the left and right side
  are not equal length. */
  PowerSpectrum& operator+=(const PowerSpectrum& other);
  /*! \brief Compute amplitude spectrum from power spectrum.

  The amplitude spectrum is sqrt of the power values.  This is a
  convenience class to return the values in that form. */
  std::vector<double> amplitude() const;
  /*! \brief Amplitude at a given frequency.

  This is an overloaded method that returns the interpolated
  amplitude (sqrt(power)) at a requested frequency.   If the frequency
  exceeds the Nyquist the function silently returns 0.

  \param f is the frequency for which amplitude is desired.

  \exception will throw a MsPaSSError if f is less than 0.
  */
  double amplitude(const double f) const;
  int nf()const{return spectrum.size();};
};
template <class T> PowerSpectrum::PowerSpectrum(const mspass::utility::Metadata& md,
    const std::vector<T>& d,double dfin, std::string nm) : mspass::utility::Metadata(md),elog()
{
  df=dfin;
  f0=0.0;
  spectrum_type=nm;
  spectrum.reserve(d.size());
  for(int k=0;k<d.size();++k)
    spectrum.push_back(static_cast<double>(d[k]));
};
}  //end namespace
#endif
