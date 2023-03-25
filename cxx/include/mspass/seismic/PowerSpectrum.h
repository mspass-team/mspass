#ifndef _POWER_SPECTRUM_H_
#define _POWER_SPECTRUM_H_
#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ErrorLogger.h"
#include "mspass/seismic/BasicSpectrum.h"
namespace mspass::seismic
{
/*! Class defining the concept of a power psectrum. */
class PowerSpectrum : public mspass::seismic::BasicSpectrum,
                      public mspass::utility::Metadata
{
public:
  std::string spectrum_type;
  std::vector<double> spectrum;
  mspass::utility::ErrorLogger elog;
  PowerSpectrum();
  template <class T> PowerSpectrum(const mspass::utility::Metadata& md,
    const std::vector<T>& d,const double dfin, const std::string nm);
  PowerSpectrum(const PowerSpectrum& parent);
  PowerSpectrum& operator=(const PowerSpectrum& parent);
  /*! \brief Standard accumulation operator.

  Sometimes we need to sum power spectra.  Type examplel would be
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
  /*! \brief power at a given frequency.

  Returns the power estimate at a specified frequency.  Uses a linear
  interpolation between nearest neighbors.  Returns the frequency exceeds
  the Nyquist silently returns 0.

  \param f is the frequency for which amplitude is desired.

  \exception MsPASSErorr object will be throw f f is less than 0.
  */
  double power(const double f) const;

  double frequency(const int sample_number) const
  {
    const std::string base_error("PowerSpectrum::frequency:  ");
    if(sample_number<0) throw mspass::utility::MsPASSError(base_error
        + "Sample number parameter passed cannot be negative");
    if(sample_number>=(this->nf())) throw mspass::utility::MsPASSError(base_error
        + "Sample number parameter passed xceeds range of spectrum array");
    return this->f0()+sample_number*this->df();
  };
  std::vector<double> frequencies() const;
  size_t nf()const{return spectrum.size();};
  double Nyquist() const {return nyquist_frequency;};
private:
  double nyquist_frequency;
};
template <class T> PowerSpectrum::PowerSpectrum(const mspass::utility::Metadata& md,
    const std::vector<T>& d,const double dfin,const std::string nm)
      : BasicSpectrum(dfin,0.0),mspass::utility::Metadata(md),elog()
{
  spectrum_type=nm;
  spectrum.reserve(d.size());
  for(size_t k=0;k<d.size();++k)
    spectrum.push_back(static_cast<double>(d[k]));
  nyquist_frequency=dfin*static_cast<double>(d.size());
};
}  //end namespace
#endif
