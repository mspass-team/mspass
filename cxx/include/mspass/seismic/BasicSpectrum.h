#ifndef _BASIC_SPECTRUM_H_
#define _BASIC_SPECTRUM_H_
#include <vector>
#include <math.h>
#include "mspass/utility/MsPASSError.h"
namespace mspass::seismic{
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
class BasicSpectrum
{
public:
  /*! Default constructor.   sets frequency interval to 1 and f0 to 0 */
  BasicSpectrum(){dfval=1.0;f0val=0.0;};
  /*! Parameterized constructor.

  \param dfin frequency bin size
  \param f0in frequency of first component of data vector of regular frequency grid. */
  BasicSpectrum(const double dfin, const double f0in)
  {
    is_live=false;
    dfval=dfin;
    f0val=f0in;
  };
  /*! Standard copy constructor */
  BasicSpectrum(const BasicSpectrum& parent)
  {
    is_live=parent.is_live;
    dfval=parent.dfval;
    f0val=parent.f0val;
  };
  /*~ Destructor.*/
  virtual ~BasicSpectrum(){};
  /*! Test live condition of the data.

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  bool live()const{return is_live;};
  /*! Test live condition of the data.

  Returns true if the data are marked as being bad  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  This method is the negation of the live method.
  */
  bool dead()const{return !is_live;};
  /*! Mark this datum bad.

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  void kill(){is_live=false;};
  /*! Mark this datum good..

  Returns true if the data are marked as being good.  This method is part of
  a four methods for handling the concept of "live"==gppd versus "dead" == bad.
  The concept was borrowed from seismic reflection processing.
  */
  void set_live(){is_live=true;};
  /*! Return the (fixed) frequemcy bin size.*/
  double df() const{return dfval;};
  /*! Return the frequency of the first (0) component of the spectrum vector.
  This value is normally 0 but the api allows it to be nonzero.  That is useful
  for windowing to store only data in a limited passband.
  */
  double f0() const{return f0val;};
  /*! Return the integer sample number of the closest sample to the specified
  frequency.   Uses rounding.   Will throw a MsPASSError object if the
  specified frequency is not within the range of the data.
  */
  int sample_number(const double f) const
  {
    int itest=static_cast<int>( round( (f-f0val)/dfval ) );
    if(itest<0)
    {
      throw mspass::utility::MsPASSError(
        "BasicSpectrum::sample_number:  f must be positive or greater than f0 if f0!=0",
        mspass::utility::ErrorSeverity::Fatal);
    }
    else if(itest>=this->nf())
    {
      throw mspass::utility::MsPASSError(
        "BasicSpectrum::sample_number:  f received exceeds the length of stored vector",
        mspass::utility::ErrorSeverity::Fatal);
    }
    else
    {
      return itest;
    }
  };
  /*! Setter for the frequency bin interval - use with caution. */
  void set_df(const double dfin){dfval=dfin;};
  /*! Setter for the initial frequency value. */
  void set_f0(const double f0in){f0val=f0in;};
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
  bool is_live;
};
}  //End namspace
#endif
