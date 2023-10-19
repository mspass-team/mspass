#ifndef _MSPASS_BUTTERWORTH_H_
#define _MSPASS_BUTTERWORTH_H_
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
namespace mspass::algorithms{
/*! \brief MsPASS implementation of Butterworth filter as processing object.

MsPASS has an existing filter routine that can implement buterworth filters
via obspy.   This class was created to allow a clean interface to Butterworth
filtering from C++ code that needs such an operator.   The original use was
an experimental deconvolution code, but there will likely be others because
simple, efficient filters are a common internal need for potential applications.

This C++ class can be viewed as a wrapper for Seismic Unix functions that
implement Butterworth filters.  The parent functions were found in the cwp
lib and were called bfdesign, bfhighpass, and bflowpass.  Those three functions
are the central tools used to implement this class.  All the rest is really
just a wrapper to provide an object oriented api to the su functions.

This  class implements a processing object concept.  That is, it is intended
to be constructed and then used for processing multiple data objects with fixed
parameters.   For Butterwoth filtering the parameters are relatively simple
(mainly two corner frequencies and number of poles defining the filter rolloff).
A complexity, however, is that the class was designed to allow automatic
handling of multiple sample rate data.   That is handled internally by
caching the sample interval of the data and automatically adjusting the
coefficients when the sample interval changes.   Note that feature only works
for MsPASS data objects CoreTimeSeries and Seismogram where the sample interval
is embedded in the object. The raw interface with a simple vector cannot know
that.  The method to change the expected sample interval has some sanity
checks to reduce, but not eliminate the possibility of mistakes that will create
unstable filters.
*/
class Butterworth
{
public:
  /*! \brief Default constructor.

  The default constructor does not define a null.  The default generates an
  antialiasing filter identical to the default in the antialias function
  in seismic unix.   That is it produces a low pass filter with
  a band edge (pass parameter) at 60% of Nyquist and a stop edge at
  Nyquist.   It then calls the su bfdesign function to compute
  the number of poles and the 3db frequency of the corner to define this
  filter.   These can be retrieved with getters (see below)
  */
  Butterworth();
  /*! \brief Fully parameterized constructor with args similar to subfilt.

  A butterworth filter can be described two ways: (1) corner frequency and
  number of poles and (2) by band stop and band frequencies.  This
  constructor is used to define the filter by stop and pass band parameters.
  Frequencies must satisfy fstoplo<fpasslo and fpasshi<fstophi as the four
  frequencis define a bandd pass between the fpasslo and fpasshi.  The stop
  frequencies define where the response should near zero.   Thus for
  band pass filters the apasslo and apasshi should be 1 and the stop Amplitudes
  a small number like 0.01.  For a band reject filter set stop amplitudes to 1
  and pass amplitudes to small numbers like 0.01.
  (For a reject filter the pass frequencies act like stop frequencies for a
  bandbpass filer - this is mostly like the subfilt seismic unix program).
  The booleans control which terms are enabld.  When enable_lo is true the
  lo components are used an when enable_hi is true the high componens are used.

  There is a confusing nomenclature related to "high" and "low".   In this
  implmentation I always take low to mean the low side of the passband
  and high to be the high side of the passband as described above for the
  4 frequency point defiitions.   The issue is "lowcut" versus "lowpass".
  Seismic Unix really mixes this up as their implmenetation (which I used here)
  refernces bflowpass and bfhighpass but subfilt uses the inverse lowcut and
  higcut terminology.  A geeky implementation detail is I actually
  changed the names of the functions to eliminate the confusion in the
  implementation. That matters only if you want to compare what we did here
  to the original seismic unix code.

  Note the iir filter coefficiets are always derived from the poles and Frequencies
  so this constructor is just an alternate way to define the filter without the abstraction
  of number of poles.

  \param zerophase when true use a zerophase filter.
     When false defines a one pass minimum phase filter.
  \param enable_lo is a boolean that when true enables the low band parameters
     for the filter (i.e. the highpass=low-cut components)
  \param enable_hi is a boolean taht when true enables the parameters defining
    the upper frequency band edge  (i.e. lowpass=high-cut parameters)
  \param fstoplo - stop band frequency for lower band edge
  \param astoplo - amplitude at stop frequency
    (small number for band pass, 1 for band reject)
  \param fpasslo - pass band frequency for lower band edge
  \param apasslo - amplitude at fpasslo frequency
    (1 for bandpass, small number for band reject)
  \param fstophi - stop band frequency for upper band edge
  \param astophi - amplitude at stop frequency
      (small number for band pass, 1 for band reject)
  \param fpasshi - pass band frequency for upper band edge
  \param apasshi - amplitude at fpasshi frequency
      (1 for bandpass, small number for band reject)
  \param sample_interval is the expected data sample interval
  */
  Butterworth(const bool zerophase, const bool enable_lo, const bool enable_hi,
    const double fstoplo, const double astoplo,
    const double fpasslo, const double apasslo,
    const double fpasshi, const double apasshi,
    const double fstophi, const double astophi,
    const double sample_interval);
  /*! Construct using tagged valus created from a Metadata container.

  This behaves exactly like the fully parameterized contructor except it
  gets the parameters from metadata.  Metadata keys in initial implementation
  are identical to the argument names defined above.  The best guidance
  for using this constuctor is to look a the comments in the default
  parameter file.*/
  Butterworth(const mspass::utility::Metadata& md);
  /*! \brief Construct by defining corner frequencies and number of npoles

  Butterworth filters can also be defind by a corner frequency and number of poles.
  In fact, only the nondimensional form of these parameters are stored as
  private attributes to define the filter.
  \param zerophase when true use a zerophase filter.
     When false defines a one pass minimum phase filter.
  \param enable_lo is a boolean that when true enables the low band parameters
     for the filter (i.e. the highpass=low-cut components)
  \param enable_hi is a boolean taht when true enables the parameters defining
    the upper frequency band edge  (i.e. lowpass=high-cut parameters)
  \param npolelo is the number of poles for the low frequency corner (highpass)
  \param f3dblo is the corner frequency for the low frequency corner (highpass)
  \param npolehi is the number of poles for the high frequency corner (lowpass)
  \param f3dbhi is the corner frequency for the high frequency corner (lowpass)
  \param sample_interval is the expected data sample interval

  */
  Butterworth(const bool zerophase, const bool enable_lo, const bool enable_hi,
     const int npolelo, const double f3dblo,
      const int npolehi, const double f3dbhi,
        const double sample_interval);
  /*! Standard copy conststructor. */
  Butterworth(const Butterworth& parent);
  /*! Standard assignment operator. */
  Butterworth& operator=(const Butterworth& parent);
  /*! \brief Return the impulse response.

  The response of a linear filter like the butterworth filter can always
  be described by either the time domain impulse response or its fourier
  transform commonly called the tranfer function.  This function returns
  the impulse response centered in a time window with a specified number
  of samples using the current sample interval cached in the object.
  Note the return has dt and the impulse is at the center of the data window
  (n/2) with t0 set so the functions zero is correct if using the implict
  time scale (time method) of a time series object.

  \param n is the number of samples to generate to characterize the impulse
  response.  The function is always returned centered on the vector of
  length n and t0 of the TimeSeries is set to make that impulse point
  be time 0.
  */
  mspass::seismic::CoreTimeSeries impulse_response(const int n);
  /*! \brief pply the filter to a CoreTimeSeries object.

  This method alters the data vector inside d in place and changes no
  other parts of the data.   Automatic switching of data sample rate is
  used on the operator.  That is, if the sample rate of the data is
  different than the operator sample rate the internal operator
  coefficients will be adjusted to the new sample rate.  The operator
  sample rate will also be changed to the sample rate of d whenever
  the sample rate changes from the previous call.

  This method has a safety to prevent irrational sample rate changes.
  The IRR filter used to compute a Butterworth filter becomes unstable if
  the low pass filter component (high corner) approach Nyquist or worse
  exceed Nyquist.   This method will throw a MsPASSError exception if the
  sample rate of d is too low for the filter high corner.
  (current 90% of Nyquist).  When this error is throw the data will be
  unaltered and the internal sample rate will be left in the previous state.

  \param d input data to be filtered - altered in place.
  \exception throws a MsPASSError if the hi corner is inconsistent with the
  sample rate of d
  */
  void apply(mspass::seismic::CoreTimeSeries& d);
  /*! \brief Apply the filter to a CoreTimeSeries object.

  This method alters the data vector inside d in place and changes no
  other parts of the data.   Automatic switching of data sample rate is
  used on the operator.  That is, if the sample rate of the data is
  different than the operator sample rate the internal operator
  coefficients will be adjusted to the new sample rate.  The operator
  sample rate will also be changed to the sample rate of d whenever
  the sample rate changes from the previous call.

  This method has a safety to prevent irrational sample rate changes.
  The IRR filter used to compute a Butterworth filter becomes unstable if
  the low pass filter component (high corner) approach Nyquist or worse
  exceed Nyquist.   This method will automatically disable the high corner
  (lowpass) component of the filter if the corner approaches or exceed
  Nyquist.  When that happens the internal sample rate is restored to the
  previous value and a complaint message is posted to elog of d.

  \param d input data to be filtered - altered in place.
  \exception none, but callers should consider checking for errors posted to
  elog
  */
  void apply(mspass::seismic::TimeSeries& d);
  /*! \brief Filter a raw vector of data.

  Use this method to apply the filter to a raw vector of data.  The
  C++ interface uses an std::vector container, but the python api in MsPASS
  allows this to be a double numpy array or any iterable version of a
  vector container (meaning storage as a contiguous block of memory).
  If this method is used it is assumed the sample interval defined for the
  operator is the same as the for the input data.

  \param d is the data to be filtered (note the data are altered in place)
  */
  void apply(std::vector<double>& d);
  /*! \brief Apply the filter to a CoreSeismogram object.

  This method alters the data vector inside d in place and changes no
  other parts of the data.   Automatic switching of data sample rate is
  used on the operator.  That is, if the sample rate of the data is
  different than the operator sample rate the internal operator
  coefficients will be adjusted to the new sample rate.  The operator
  sample rate will also be changed to the sample rate of d whenever
  the sample rate changes from the previous call.

  This method has a safety to prevent irrational sample rate changes.
  The IRR filter used to compute a Butterworth filter becomes unstable if
  the low pass filter component (high corner) approach Nyquist or worse
  exceed Nyquist.   This method will throw a MsPASSError exception if the
  sample rate of d is too low for the filter high corner.
  (current 90% of Nyquist).   When this error is throw the data will be
  unaltered and the internal sample rate will be left in the previous state.

  \param d input data to be filtered - altered in place.
  \exception throws a MsPASSError if the hi corner is inconsistent with the
  sample rate of d
  */
  void apply(mspass::seismic::CoreSeismogram& d);
  /*! \brief Apply the filter to a CoreTimeSeries object.

  This method alters the data vector inside d in place and changes no
  other parts of the data.   Automatic switching of data sample rate is
  used on the operator.  That is, if the sample rate of the data is
  different than the operator sample rate the internal operator
  coefficients will be adjusted to the new sample rate.  The operator
  sample rate will also be changed to the sample rate of d whenever
  the sample rate changes from the previous call.

  This method has a safety to prevent irrational sample rate changes.
  The IRR filter used to compute a Butterworth filter becomes unstable if
  the low pass filter component (high corner) approach Nyquist or worse
  exceed Nyquist.   This method will automatically disable the high corner
  (lowpass) component of the filter if the corner approaches or exceed
  Nyquist.  When that happens the internal sample rate is restored to the
  previous value and a complaint message is posted to elog of d.

  \param d input data to be filtered - altered in place.
  \exception none, but callers should consider checking for errors posted to
  elog
  */
  void apply(mspass::seismic::Seismogram& d);
  /*! \brief Return the response of the filter in the frequency domain.

  The impulse response of any linear system can always be characterized by
  either the time domain response to spike signal or the alternative
  frequency domain version of the same function commonly called the
  transfer function.  This method returns the transfer funtion as a
  mspass::algorithms::deconvolution::ComplexArray container.  Use methods
  in that object to get amplitude and phase response functions.

  \param n is the number of points that should be used to characterize the
  transfer function.  Note because we are dealing with strictly real valued
  signals the array returned will be folded at the Nyquist frequency in the
  standard way of all FFT implementations (current implementation uses the
  fft in the gnu scientific library that definitely does that).

  */
  mspass::algorithms::deconvolution::ComplexArray transfer_function(const int n);
  /*! \brief set the sample interval assumed for input data.

  This function can be used when running with raw data vectors if the sample
  interval of the data series is different from that called on construction
  or set previously.   This is a nontrivial change because the filter
  coefficients depend upon sample interval.   In particular, for this
  implementation npoles and the 3db frequency points stored internally
  are altered when this function is called.  If the frequency intervals
  change the expectation is the user will create a new instance of this
  object.

  Warning:  this routine does not implement the safeties built into
  TimeSeries and Seismogram apply methods.  It will silently change the upper
  corner to an unstable position if called inappropriately.

  \param dtnew is the new sample interval to set for the operator.
  */
  void change_dt(const double dtnew)
  {
    this->f3db_lo *= (dtnew/(this->dt));
    this->f3db_hi *= (dtnew/(this->dt));
    this->dt=dtnew;
  };
  /*! Return the low frequency 3db corner (in Hz).*/
  double low_corner() const
  {
    return f3db_lo/dt;
  };
  /*! Return the high frequency 3db corner (in Hz).*/
  double high_corner() const
  {
    return f3db_hi/dt;
  };
  /*! Return the number of poles defining the highpass (lowcut) element
  of the filter.*/
  int npoles_low() const {return npoles_lo;};
  /*! Return the number of poles defining the lowpass (highcut) element
  of the filter.*/
  int npoles_high() const {return npoles_hi;};
  /*! Return the current operator sample interval.*/
  double current_dt()const {return dt;};
  /*! Return a string defining the type of operator this filter defines.
  Currently can be one of the following:  bandpass, lowpass, or highpass.
  It is possible to construct a band reject filter with the right
  constructor, but the implementation of this method will not detect that
  condition.  A band reject filter will be incorrectly tagged bandpass.
  The algorithm just looks to see which  of the band edges are defined
  (the hi and lo concepts described above) and guesses the filter type.
  If both are off it returns "Undefined".
  */
  std::string filter_type() const
  {
    if(use_lo)
    {
      if(use_hi)
        return std::string("bandpass");
      else
        return std::string("highpass");
    }
    else
    {
      if(use_hi)
        return std::string("lowpass");
      else
        return std::string("Undefined");
    }
  };
  /*! Return true if the filter is defined as a zero phase filter.
  Returns false if it is minimum phase. */
  bool is_zerophase() const
  {
    return zerophase;
  };
private:
  bool use_lo,use_hi;
  bool zerophase;

  /* bfdesign sets npoles and 3db points based on specified band properties.
  These stored these when the constructor calls bfdesign for high and
  low pass elements. Names should make clear which is which. These are
  always stored in nondimensional form (f_nd=f*dt)*/
  double f3db_lo, f3db_hi;
  int npoles_lo, npoles_hi;
  double dt;
  /* These three functions are nearly exact copies of seismic unix
  functions with a couple of differences:
  1.  All floats in the su code are made double here to mesh with modern arch
  2.  bfhighpass is changed to bflowpass and bflowpass is changed to bfhighcut to mesh
  with the names used here more cleanly.  that is, pass and cut define
  the opposite sense of a filter and it gets very confusing when the
  terms get mixed up in the same set of code.  This way low always means the
  low end of the pass band and high is the high end of the pass band.
  */

  void bfdesign (double fpass, double apass, double fstop, double astop,
    int *npoles, double *f3db);
  void bfhighcut (int npoles, double f3db, int n, double p[], double q[]);
  void bflowcut (int npoles, double f3db, int n, double p[], double q[]);
  /* These internal methods use internal dt value to call bfdesign with
  nondimensional frequencies. Hence they should always be called after
  a change in dt.  They are also handy to contain common code for constructors. */
  void set_lo(const double fstop, const double fpass,
    const double astop, const double apass);
  void set_hi(const double fstop, const double fpass,
    const double astop, const double apass);
};
}  // namespace end
#endif
