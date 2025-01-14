#ifndef _AMPLITUDES_H_
#define _AMPLITUDES_H_
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/VectorStatistics.h"
#include <sstream>
namespace mspass::algorithms::amplitudes {
double PeakAmplitude(const mspass::seismic::CoreTimeSeries &d);
double PeakAmplitude(const mspass::seismic::CoreSeismogram &d);
double RMSAmplitude(const mspass::seismic::CoreTimeSeries &d);
double RMSAmplitude(const mspass::seismic::CoreSeismogram &d);
double PercAmplitude(const mspass::seismic::CoreTimeSeries &d,
                     const double perf);
double PercAmplitude(const mspass::seismic::CoreSeismogram &d,
                     const double perf);
double MADAmplitude(const mspass::seismic::CoreTimeSeries &d);
double MADAmplitude(const mspass::seismic::CoreSeismogram &d);
enum class ScalingMethod {
  Peak,     /*! Use peak amplitude method - equivalent to Linfinity norm*/
  RMS,      /*! Use RMS amplitude method - L2 norm of data.*/
  ClipPerc, /*! Use a percent clip scaling method as used in seismic unix.*/
  MAD       /*! Use median absolute deviation scaling - a form of L1 norm*/
};
const std::string scale_factor_key("calib");
/*! \brief Scaling function for atomic data objects in mspass.

An atomic data object in this case means a class that is a child of
Metadata and BasictimeSeries.   The function pulls the existing
value for scale_factor_key (calib), computes a scale factor based on
computed amplitude metric by specified method, scales the to have that
metric be that defined by level, and then sets the value associated with
scale_factor_key appropriately to define conversion back to the original
units.

\param d is the data to be scale.  Works only if
  overloaded functions PeakAmplitude, PercAmplitude, MADAmplitude, and
  RMSAmplitude are defined for d.  Currently that means CoreTimeSeries and
  CoreSeismogram.  Note in mspass this assumes history preservation is handled
  in python wrappers.
\param method sets the scaling metric defined through ScalingMethod eum class.
\param level has two different contexts.   For PercAmplitude it must be a
 a number n with 0<n<=1.0
\param win defines a time window to use for computing the amplitude.
 It the window exeeds the data range it will be reduced to the range of
 the data.  Similarly, if the window is invalid (defined as end time less
 than start time) the window will be adjusted to the full data range.
\return computed amplitude
*/

template <typename Tdata>
double scale(Tdata &d, const ScalingMethod method, const double level,
             const mspass::algorithms::TimeWindow win) {
  if ((method == ScalingMethod::ClipPerc) && (level <= 0.0 || level > 1.0))
    throw mspass::utility::MsPASSError(
        "scale function:  illegal perf level specified for clip percentage "
        "scale - must be between 0 and 1\nData unaltered - may cause "
        "downstream problems",
        mspass::utility::ErrorSeverity::Suspect);
  try {
    double newcalib(1.0);
    /* the else condition here should perhaps generate an elog message but
    did not implement to allow this template to be used for CoreTimeSeries
    and CoreSeismogram that do not have an elog attribute.*/
    if (d.is_defined(scale_factor_key)) {
      newcalib = d.get_double(scale_factor_key);
    }
    /* Handle time windowing. Log window mismatches but silently handle
    cast where the window is invalid - used as a way to override any
    time windowing */
    mspass::algorithms::TimeWindow ampwindow;
    if (win.start > win.end) {
      ampwindow.start = d.t0();
      ampwindow.end = d.endtime();
    } else if ((fabs(win.start - d.t0()) / d.dt() > 0.5) ||
               (fabs(win.end - d.endtime()) / d.dt() > 0.5)) {
      std::stringstream ss;
      ss << "Window time range is inconsistent with input data range"
         << std::endl
         << "Input data starttime=" << d.t0()
         << " and window start time=" << win.start
         << " Difference=" << d.t0() - win.start << std::endl
         << "Input data endtime=" << d.endtime()
         << " and window end time=" << win.end
         << " Difference=" << d.endtime() - win.end << std::endl
         << "One or the other exceeds 1/sample interval=" << d.dt() << std::endl
         << "Window for amplitude calculation changed to data range";
      d.elog.log_error("scale", ss.str(),
                       mspass::utility::ErrorSeverity::Complaint);
      ampwindow.start = d.t0();
      ampwindow.end = d.endtime();
    } else {
      ampwindow = win;
    }
    Tdata windowed_data;
    windowed_data = mspass::algorithms::WindowData(d, ampwindow);
    double amplitude, dscale;
    switch (method) {
    case ScalingMethod::Peak:
      amplitude = PeakAmplitude(windowed_data);
      break;
    case ScalingMethod::ClipPerc:
      amplitude = PercAmplitude(windowed_data, level);
      break;
    case ScalingMethod::MAD:
      amplitude = MADAmplitude(windowed_data);
      break;
    case ScalingMethod::RMS:
    default:
      amplitude = RMSAmplitude(windowed_data);
    };
    /* needed to handle case with a vector of all 0s*/
    if (amplitude > 0.0) {
      dscale = level / amplitude;
      newcalib /= dscale;
      d *= dscale;
      d.put(scale_factor_key, newcalib);
    } else {
      std::stringstream ss;
      ss << "Data array is all 0s and cannot be scaled";
      d.elog.log_error("scale", ss.str(),
                       mspass::utility::ErrorSeverity::Complaint);
      /* This may not be necessary but it assures this value is always set on
      return even if it means nothing*/
      d.put(scale_factor_key, newcalib);
    }
    return amplitude;
  } catch (...) {
    throw;
  };
}
/*! Generic function to scale ensembles.

This function is the ensemble version of the scale function defined
elsewhere in this file.   It applies a scaling member by member using
the scale function for each.  The template is for member data type.

\param d is the data to be scale.  Works only if
  overloaded functions PeakAmplitude, PercAmplitude, MADAmplitude, and
  RMSAmplitude are defined for ensemble members.  Currently that means
CoreTimeSeries and CoreSeismogram.  Note in mspass this assumes history
preservation is handled in python wrappers.
\param method sets the scaling metric defined through ScalingMethod eum class.
\param level has two different contexts.   For PercAmplitude it must be a
 a number n with 0<n<=1.0
\param win is a TimeWindow range that defines where the metric being used
  to compute the a amplitudes of each member is to be computed.   A fixed
  time window is used for the entire ensemble so this approach is best used
  on data shifted to relative time on a particular seismic phase arrival time.
  To use the entire data range for the scaling pass a window with an end time
  less than the start time.  That is used by the function as a signal to
  ignore the actual range and use the entire data range instead.

\return vector of computed amplitudes
*/
template <typename Tdata>
std::vector<double>
scale_ensemble_members(mspass::seismic::Ensemble<Tdata> &d,
                       const ScalingMethod &method, const double level,
                       const mspass::algorithms::TimeWindow win) {
  if ((method == ScalingMethod::ClipPerc) && (level <= 0.0 || level > 1.0))
    throw mspass::utility::MsPASSError(
        "scale_ensemble_members function:  illegal perf level specified for "
        "clip percentage scale - must be between 0 and 1\nData unaltered - may "
        "cause downstream problems",
        mspass::utility::ErrorSeverity::Suspect);
  try {
    typename std::vector<Tdata>::iterator dptr;
    std::vector<double> amps;
    amps.reserve(d.member.size());
    for (dptr = d.member.begin(); dptr != d.member.end(); ++dptr) {
      double thisamp;
      thisamp = scale(*dptr, method, level, win);
      amps.push_back(thisamp);
    }
    return amps;
  } catch (...) {
    throw;
  };
}
/*! Generic function to apply an ensemble average scale factor.

Sometimes we want to preserve true relative amplitudes between members of an
ensemble but we need to scale the overall data to some range (e.g order 1 for
plotting). Use this function to do that for ensembles.  The
scale_ensemble_members function, in contrast, scales each member separately.

\param d is the data to be scale.  Works only if
  overloaded functions PeakAmplitude, PercAmplitude, MADAmplitude, and
  RMSAmplitude are defined for ensemble members.  Currently that means
CoreTimeSeries and CoreSeismogram.  Note in mspass this assumes history
preservation is handled in python wrappers.
\param method sets the scaling metric defined through ScalingMethod eum class.
\param level has two different contexts.   For PercAmplitude it must be a
 a number n with 0<n<=1.0
\param use_mean (boolean)  when true use the mean log amplitude to set the
 gain.  Default uses median.

\return computed average amplitude
*/
template <typename Tdata>
double scale_ensemble(mspass::seismic::Ensemble<Tdata> &d,
                      const ScalingMethod &method, const double level,
                      const bool use_mean) {
  if ((method == ScalingMethod::ClipPerc) && (level <= 0.0 || level > 1.0))
    throw mspass::utility::MsPASSError(
        "scale_ensemble function:  illegal perf level specified for clip "
        "percentage scale - must be between 0 and 1\nData unaltered - may "
        "cause downstream problems",
        mspass::utility::ErrorSeverity::Suspect);
  try {
    double avgamp; // defined here because the value computed here is returned
                   // on success
    typename std::vector<Tdata>::iterator dptr;
    std::vector<double> amps;
    amps.reserve(d.member.size());
    size_t nlive(0);
    for (dptr = d.member.begin(); dptr != d.member.end(); ++dptr) {
      double amplitude;
      if (dptr->dead())
        continue;
      switch (method) {
      case ScalingMethod::Peak:
        amplitude = PeakAmplitude(*dptr);
        break;
      case ScalingMethod::ClipPerc:
        amplitude = PercAmplitude(*dptr, level);
        break;
      case ScalingMethod::MAD:
        amplitude = MADAmplitude(*dptr);
        break;
      case ScalingMethod::RMS:
      default:
        amplitude = RMSAmplitude(*dptr);
      };
      ++nlive;
      amps.push_back(log(amplitude));
    }
    /*Silently return a 0 if there are no live data members*/
    if (nlive == 0)
      return 0.0;
    mspass::utility::VectorStatistics<double> ampstats(amps);
    if (use_mean) {
      avgamp = ampstats.mean();
    } else {
      avgamp = ampstats.median();
    }

    /* restore to a value instead of natural log*/
    avgamp = exp(avgamp);
    /* Silently do nothing if all the data are zero.   Would be better to
    log an error but use as a "Core" object doesn't contain an elog attribute*/
    if (avgamp <= 0.0) {
      return 0.0;
    }
    double dscale = level / avgamp;
    /* Now scale the data and apply calib */
    for (dptr = d.member.begin(); dptr != d.member.end(); ++dptr) {
      if (dptr->live()) {
        double calib;
        (*dptr) *= dscale;
        if (dptr->is_defined(scale_factor_key)) {
          calib = dptr->get_double(scale_factor_key);
        } else {
          calib = 1.0;
        }
        calib /= dscale;
        dptr->put(scale_factor_key, calib);
      }
    }
    return avgamp;
  } catch (...) {
    throw;
  };
}
/*! Convert an std::vector to a unit vector based on L2 norm.*/
template <class T> std::vector<T> normalize(const std::vector<T> &d) {
  size_t N = d.size();
  std::vector<T> result;
  result.reserve(N);
  double d_nrm(0.0);
  ;
  for (size_t i = 0; i < N; ++i) {
    d_nrm += (d[i] * d[i]);
    result.push_back(d[i]);
  }
  d_nrm = sqrt(d_nrm);
  for (size_t i = 0; i < N; ++i)
    result[i] /= d_nrm;
  return result;
}
/*! \brief Holds parameters defining a passband computed from snr.

This deconvolution operator has the option to determine the optimal
shaping wavelet on the fly based on data bandwidth.  This class is
a struct in C++ disguise used to hold the encapsulation of the output
of function(s) used to estimate the data bandwidth.  This class is used
only internally with CNR3CDecon to hold this information.  It is not
expected to be visible to python in MsPaSS, for example.
*/
class BandwidthData {
public:
  /*! Low corner frequency for band being defined. */
  double low_edge_f;
  /*! Upper corner frequency for band being defined. */
  double high_edge_f;
  /*! Signal to noise ratio at lower band edge. */
  double low_edge_snr;
  /*! Signal to noise ratio at upper band edge. */
  double high_edge_snr;
  /* This is the frequency range of the original data */
  double f_range;
  BandwidthData() {
    low_edge_f = 0.0;
    high_edge_f = 0.0;
    low_edge_snr = 0.0;
    high_edge_snr = 0.0;
    f_range = 0.0;
  };
  /*! Return a metric of the estimated bandwidth divided by total frequency
   * range*/
  double bandwidth_fraction() const {
    if (f_range <= 0.0)
      return 0.0;
    else
      return (high_edge_f - low_edge_f) / f_range;
  };
  /*! Return bandwidth in dB. */
  double bandwidth() const {
    if (f_range <= 0.0)
      return 0.0;
    else {
      double ratio = high_edge_f / low_edge_f;
      return 20.0 * log10(ratio);
    }
  };
};
/*! \brief Estimate signal bandwidth.

Signal bandwidth is a nontrivial thing to estimate as a general problem.
The algorithm here is known to be fairly functional for most seismic data
that has a typical modern broaband response.  It makes an implicit assumption
that the noise floor rises relative to the data at low frequencies making
the problem of finding the lower band edge one of just simply looking for
a frequency where the spectrum of the signal is "significantly" larger than
the noise.  At the high frequency end it depends on a similar assumption that
comes from a different property of earthquake data.  That is, we know know
that all seismic signals have some upper frequency limit created by a range of
physical processes.  That means that at high enough frequency the signal to
noise ratio will always fall to a small value or hit the high corner of the
system antialias filter.  The basic algorithm used here then is an
enhanced search algorithm that hunts for the band edge and a high and low
frequency end where the snr passes through a specified cutoff threshold.
The algorithm has some enhancements appropriate only for multitaper spectra.
The algorithm searches from f*tbw forward to find frequency where snr exceeds
snr_threshold.   It then does the reverse from a user specified upper frequency.
To avoid issues with lines in noise spectra snr must exceed the threshold
by more than 2*tbw frequency bins for an edge to be defined.  The edge back is
defined as 2*tbw*df from the first point satisfying that constraint.

Note this function handles the calculation correctly if the signal and
noise windows have a drastically different length.  A subtle feature of
psd estimates of stationary processes is that the psd level scales by
1/length of the analysis window.   snr estimates correct for this effect.

\param df is the expected signal frequency bin size.   An error will be
thrown if that does not match the power spectrem s df.
\param s is a (multitaper) power spectrum of the signal time window
\param n is the comparable (multitaper) power spectrum of the noise time window.
\param snr_threshold is the snr threshold used to define the band edges.
\param tbp is the time-bandwidth product of the multitaper estimator used to
  estimate s and n (they should be the same or you are asking trouble but that
  is not checked).  tbp determines the expected smoothness of the spectrum and
is used in the band edge estimation as described above.
\param fhs is an abbreviation for "frequence high start".   Use this argument
  to set frequency where backward (working downward in f that is) search for
  the upper band edge should start.   This parameter is highly recommended for
  teleseismic body wave data phases where the high frequencies just don't exist.
  P phases, for example, should set this parameter somewhere between 2 and 5 Hz.
  direct S phases should be more like 1 Hz, but that depends up on the data.
  The reason this is necessary is sometimes data have high frequency lines in
  the spectrum that can fool this simple algorithm.   In a C++ program there is
  a default for this parameter of -1.0.  When this argument is negative OR
  if the frequency is over the Nyquist of the data it will be silently set to
  80% of the nyquist of s.
\param fix_high_edge_to_fhs is a boolean that does what the verbose name
  says.  That is, when set true the search for the upper bandwidth edge is
  disable and the upper bandwidth frequency edge is set to fhs.   That
  option can be useful for teleseismic data as high frequency colored noise
  bursts at low thresholds can lead to poor estimates of the upper band edge.

\return BandwidthData class describing the bandwidth determined by the
algorithm.
*/
BandwidthData EstimateBandwidth(const double signal_df,
                                const mspass::seismic::PowerSpectrum &s,
                                const mspass::seismic::PowerSpectrum &n,
                                const double snr_threshold, const double tbp,
                                const double fhs = -1.0,
                                const bool fix_high_edge_to_fhs = false);
/*! \brief Create summary statistics of snr data based on signal and noise
spectra.

This function is a close companion to EstimateBandwidth.   EstimateBandwidth
aims only to find the upper and lower range of a frequency range it judges to
be signal.  This function takes the output of EstimateBandwidth and uses it
to compute a vector of snr values across the bandwidth defined by the
output of EstimateBandwidth.  It returns the results in a Metadata container
with the following keys and the concepts they defines:
  "median_snr" - median value of snr in the band
  "maximum_snr" - largest snr in the band
  "minimum_snr" - smallest snr in the band
  "q1_4_snr" - lower quartile (25% point of the distribution) of snr values
  "q3_4_snr" - upper quartile (75% point of the distribution) of snr values
  "mean_snr" - arithmetic mean ofsnr values
  "stats_are_valid" - booelan caller should used as the name suggests.  That is,
     caller should first fetch this attribute and handle the resulting null
     condition.  When this is false it means the data have not detectable
     signal based on the computed spectra.

This function handles the calculation correctly if the signal and
noise windows have a drastically different length.  A subtle feature of
psd estimates of stationary processes is that the psd level scales by
1/length of the analysis window.   snr estimates correct for this effect.

Note the function does attempt to avoid Inf and NaN values that are possible
if the noise value at some frequency is zero (negative is treated like 0).
If the signal amplitude is nonzero and the noise amplitude is 0 snr is
set to large value (actually 999999.9).  If the signal amplitude is also
zero the snr value is set to -1.0.  If you find minimum_snr is -1.0 it means
at least one frequency bin had the equivalent of a NaN condition.

\param s - signal power spectrum (must be the same data used for
EstimateBandwith)
\param n - noise power spectrum (must be the same data used for
EstimateBandwidth)
\param bwd - data returned from s and n in preceding call to EstimateBandiwth.

\return Metadata container with summary statistics keyed as described above
*/
mspass::utility::Metadata
BandwidthStatistics(const mspass::seismic::PowerSpectrum &s,
                    const mspass::seismic::PowerSpectrum &n,
                    const BandwidthData &bwd);
} // namespace mspass::algorithms::amplitudes
#endif
