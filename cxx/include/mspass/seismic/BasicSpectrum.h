#ifndef _BASIC_SPECTRUM_H_
#define _BASIC_SPECTRUM_H_
#include "mspass/utility/MsPASSError.h"
#include <math.h>
#include <vector>
namespace mspass::seismic {
/*! Base class for family of data objects created by Fourier transforms.

There are a range of algorithms used in seismology that center on the use
of Fourier Transforms.  This base class is intended to be used as
the base for any mspass C++ data object based on Fourier transforms
where the data are stored with a vector defined on a uniform grid in
frequency.  That means any algorithm based on fFTs.   The value of using
a base class is that power spectra and complex spectra have many
common concpets shared by this base class.  Classic use of inheritance to
avoid redundant code.
*/

class BasicSpectrum {
public:
  /*! Default constructor.   sets frequency interval to 1 and f0 to 0 */
  BasicSpectrum() {
    dfval = 1.0;
    f0val = 0.0;
  };
  /*! Parameterized constructor.

  \param dfin frequency bin size
  \param f0in frequency of first component of data vector of regular frequency
  grid.
  \param dtin parent sample interval of spectrum estimate.
  \param npts_in number of actual samples of parent time series (may not be
     the same as spectrum link when zero padding is used. subclasses should
     provide a way to handle zero padding */
  BasicSpectrum(const double dfin, const double f0in, const double dtin,
                const int npts_in) {
    is_live = false;
    dfval = dfin;
    f0val = f0in;
    parent_dt = dtin;
    parent_npts = npts_in;
  };
  /*! Standard copy constructor */
  BasicSpectrum(const BasicSpectrum &parent) {
    is_live = parent.is_live;
    dfval = parent.dfval;
    f0val = parent.f0val;
    parent_dt = parent.parent_dt;
    parent_npts = parent.parent_npts;
  };
  BasicSpectrum &operator=(const BasicSpectrum &parent) {
    if (this != (&parent)) {
      is_live = parent.is_live;
      dfval = parent.dfval;
      f0val = parent.f0val;
      parent_dt = parent.parent_dt;
      parent_npts = parent.parent_npts;
    }
    return *this;
  };
  /*~ Destructor.*/
  virtual ~BasicSpectrum() {};
  /*! Test live condition of the data.

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  bool live() const { return is_live; };
  /*! Test live condition of the data.

  Returns true if the data are marked as being bad  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  This method is the negation of the live method.
  */
  bool dead() const { return !is_live; };
  /*! Mark this datum bad.

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  void kill() { is_live = false; };
  /*! Mark this datum good..

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  void set_live() { is_live = true; };
  /*! Return the (fixed) frequemcy bin size.*/
  double df() const { return this->dfval; };
  /*! Return the frequency of the first (0) component of the spectrum vector.
  This value is normally 0 but the api allows it to be nonzero.  That is useful
  for windowing to store only data in a limited passband.
  */
  double f0() const { return this->f0val; };
  /*! \brief Return the original sample interval of data used to generate
  spectrum.

  When zero padding is used the original sample interval of data cannot be
  known without additional data.   In this implementation we require the
  user to store that information with a protected attribute within the object.
  This retrieves that stored value. */
  double dt() const { return this->parent_dt; };
  /*! \brief Return the Rayleigh bin size for this spectrum.

  The Rayleigh bin size of a spectrum is 1/T where T is the length of the
  original time series.  This method returns that attribute of this spectrum.
  Note the rayleigh and df methods will return the same number only when
  a spectrum is computed with no zero padding.
  */
  double rayleigh() const {
    return 1.0 / (this->parent_dt * static_cast<double>(this->parent_npts));
  };
  /*! Return number of points in parent time series.  Of use mostly
   * internally.*/
  int timeseries_npts() const { return parent_npts; }
  /*! Return the integer sample number of the closest sample to the specified
  frequency.   Uses rounding.   Will throw a MsPASSError object if the
  specified frequency is not within the range of the data.
  */
  int sample_number(const double f) const {
    int itest = static_cast<int>(round((f - f0val) / dfval));
    if (itest < 0) {
      throw mspass::utility::MsPASSError(
          "BasicSpectrum::sample_number:  f must be positive or greater than "
          "f0 if f0!=0",
          mspass::utility::ErrorSeverity::Fatal);
    } else if (itest >= this->nf()) {
      throw mspass::utility::MsPASSError(
          "BasicSpectrum::sample_number:  f received exceeds the length of "
          "stored vector",
          mspass::utility::ErrorSeverity::Fatal);
    } else {
      return itest;
    }
  };
  /*! Setter for the frequency bin interval - use with caution. */
  void set_df(const double dfin) { dfval = dfin; };
  /*! Setter for the initial frequency value. */
  void set_f0(const double f0in) { f0val = f0in; };
  /*! Setter for internally stored parent data sample interval. */
  void set_dt(const double dtin) { parent_dt = dtin; };
  /*! Setter for internal parent number of data points (need by rayleigh
  method). Note one should only use this in constructors and when creating an
  instance from pieces.*/
  void set_npts(const int npts_in) { parent_npts = npts_in; };
  /*! Return an std::vector container containing the frequency of each
  sample in the spectrum vector.  Commonly necessary for plotting.
  Made virtual because nf method needs to be virtual.*/
  virtual std::vector<double> frequencies() const = 0;
  /*! Return frequency at a specified sample number.  Virtual to allow
  subclasses to throw an error for illegal value.   */
  virtual double frequency(const int sample_number) const = 0;
  /*! Return the number of frequency bin.*/
  virtual size_t nf() const = 0;
  /*! Return the Nyquist frequency.   Virtual because if f0 is not zero
  and the number of points is not the full fft output this needs to be
  handled differently. */
  virtual double Nyquist() const = 0;

protected:
  double dfval;
  double f0val;
  double parent_dt;
  double parent_npts;
  bool is_live;
};
} // namespace mspass::seismic
#endif
