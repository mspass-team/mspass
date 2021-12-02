#ifndef _AMPLITUDES_H_
#define _AMPLITUDES_H_
#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/utility/VectorStatistics.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/algorithms.h"
namespace mspass::algorithms::amplitudes{
double PeakAmplitude(const mspass::seismic::CoreTimeSeries& d);
double PeakAmplitude(const mspass::seismic::CoreSeismogram& d);
double RMSAmplitude(const mspass::seismic::CoreTimeSeries& d);
double RMSAmplitude(const mspass::seismic::CoreSeismogram& d);
double PercAmplitude(const mspass::seismic::CoreTimeSeries& d,const double perf);
double PercAmplitude(const mspass::seismic::CoreSeismogram& d,const double perf);
double MADAmplitude(const mspass::seismic::CoreTimeSeries& d);
double MADAmplitude(const mspass::seismic::CoreSeismogram& d);
enum class ScalingMethod
{
  Peak, /*! Use peak amplitude method - equivalent to Linfinity norm*/
  RMS,  /*! Use RMS amplitude method - L2 norm of data.*/
  ClipPerc, /*! Use a percent clip scaling method as used in seismic unix.*/
  MAD   /*! Use median absolute deviation scaling - a form of L1 norm*/
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

template <typename Tdata> double scale(Tdata& d,const ScalingMethod method,
  const double level, const mspass::algorithms::TimeWindow win)
{
  if((method==ScalingMethod::ClipPerc) && (level<=0.0 || level>1.0))
    throw mspass::utility::MsPASSError("scale function:  illegal perf level specified for clip percentage scale - must be between 0 and 1\nData unaltered - may cause downstream problems",
       mspass::utility::ErrorSeverity::Suspect);
  try{
    double newcalib(1.0);
    /* the else condition here should perhaps generate an elog message but
    did not implement to allow this template to be used for CoreTimeSeries
    and CoreSeismogram that do not have an elog attribute.*/
    if(d.is_defined(scale_factor_key))
    {
      newcalib=d.get_double(scale_factor_key);
    }
    /* Handle time windowing. Log window mismatches but silently handle
    cast where the window is invalid - used as a way to override any
    time windowing */
    mspass::algorithms::TimeWindow ampwindow;
    if(win.start>win.end)
    {
      ampwindow.start=d.t0();
      ampwindow.end=d.endtime();
    }
    else if( (fabs(win.start-d.t0())/d.dt()>0.5)
       || (fabs(win.end-d.endtime())/d.dt() > 0.5) )
    {
      std::stringstream ss;
      ss << "Window time range is inconsistent with input data range"<<std::endl
         << "Input data starttime="<<d.t0()<<" and window start time="
         << win.start <<" Difference="<<d.t0()-win.start<<std::endl
         << "Input data endtime="<<d.endtime()<<" and window end time="
         << win.end <<" Difference="<<d.endtime()-win.end<<std::endl
         << "One or the other exceeds 1/sample interval="<<d.dt()<<std::endl
         << "Window for amplitude calculation changed to data range";
      d.elog.log_error("scale",ss.str(),mspass::utility::ErrorSeverity::Complaint);
      ampwindow.start=d.t0();
      ampwindow.end=d.endtime();
    }
    else
    {
      ampwindow=win;
    }
    Tdata windowed_data;
    windowed_data=mspass::algorithms::WindowData(d,ampwindow);
    double amplitude,dscale;
    switch(method)
    {
      case ScalingMethod::Peak:
        amplitude=PeakAmplitude(windowed_data);
        dscale = level/amplitude;
        newcalib /= dscale;
        break;
      case ScalingMethod::ClipPerc:
        amplitude=PercAmplitude(windowed_data,level);
        /* for this scaling we use level as perf and output level is frozen
        to be scaled to order unity*/
        dscale = 1.0/amplitude;
        newcalib /= dscale;
        break;
      case ScalingMethod::MAD:
        amplitude=MADAmplitude(windowed_data);
        dscale = level/amplitude;
        newcalib /= dscale;
        break;
      case ScalingMethod::RMS:
      default:
        amplitude=RMSAmplitude(windowed_data);
        dscale = level/amplitude;
        newcalib /= dscale;
    };
    d *= dscale;
    d.put(scale_factor_key,newcalib);
    return amplitude;
  }catch(...){throw;};
}
/*! Generic function to scale ensembles.

This function is the ensemble version of the scale function defined
elsewhere in this file.   It applies a scaling member by member using
the scale function for each.  The template is for member data type.

\param d is the data to be scale.  Works only if
  overloaded functions PeakAmplitude, PercAmplitude, MADAmplitude, and
  RMSAmplitude are defined for ensemble members.  Currently that means CoreTimeSeries and
  CoreSeismogram.  Note in mspass this assumes history preservation is handled
  in python wrappers.
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
template <typename Tdata> std::vector<double> scale_ensemble_members(mspass::seismic::Ensemble<Tdata>& d,
  const ScalingMethod& method, const double level, const mspass::algorithms::TimeWindow win)
{
  if((method==ScalingMethod::ClipPerc) && (level<=0.0 || level>1.0))
    throw mspass::utility::MsPASSError("scale_ensemble_members function:  illegal perf level specified for clip percentage scale - must be between 0 and 1\nData unaltered - may cause downstream problems",
       mspass::utility::ErrorSeverity::Suspect);
  try{
    typename std::vector<Tdata>::iterator dptr;
    std::vector<double> amps;
    amps.reserve(d.member.size());
    for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
    {
      double thisamp;
      thisamp=scale(*dptr,method,level,win);
      amps.push_back(thisamp);
    }
    return amps;
  }catch(...){throw;};
}
/*! Generic function to apply an ensemble average scale factor.

Sometimes we want to preserve true relative amplitudes between members of an ensemble
but we need to scale the overall data to some range (e.g order 1 for plotting).
Use this function to do that for ensembles.  The scale_ensemble_members function,
in contrast, scales each member separately.

\param d is the data to be scale.  Works only if
  overloaded functions PeakAmplitude, PercAmplitude, MADAmplitude, and
  RMSAmplitude are defined for ensemble members.  Currently that means CoreTimeSeries and
  CoreSeismogram.  Note in mspass this assumes history preservation is handled
  in python wrappers.
\param method sets the scaling metric defined through ScalingMethod eum class.
\param level has two different contexts.   For PercAmplitude it must be a
 a number n with 0<n<=1.0
\param use_mean (boolean)  when true use the mean log amplitude to set the
 gain.  Default uses median.

\return computed average amplitude
*/
template <typename Tdata> double scale_ensemble(mspass::seismic::Ensemble<Tdata>& d,
  const ScalingMethod& method, const double level, const bool use_mean)
{
  if((method==ScalingMethod::ClipPerc) && (level<=0.0 || level>1.0))
    throw mspass::utility::MsPASSError("scale_ensemble function:  illegal perf level specified for clip percentage scale - must be between 0 and 1\nData unaltered - may cause downstream problems",
       mspass::utility::ErrorSeverity::Suspect);
  try{
    double avgamp;   //defined here because the value computed here is returned on success
    typename std::vector<Tdata>::iterator dptr;
    std::vector<double> amps;
    amps.reserve(d.member.size());
    size_t nlive(0);
    for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
    {
      double amplitude;
      if(dptr->dead()) continue;
      switch(method)
      {
        case ScalingMethod::Peak:
          amplitude=PeakAmplitude(*dptr);
          break;
        case ScalingMethod::ClipPerc:
          amplitude=PercAmplitude(*dptr,level);
          break;
        case ScalingMethod::MAD:
          amplitude=MADAmplitude(*dptr);
          break;
        case ScalingMethod::RMS:
        default:
          amplitude=RMSAmplitude(*dptr);
      };
      ++nlive;
      amps.push_back(log(amplitude));
    }
    /*Silently return a 0 if there are no live data members*/
    if(nlive==0) return 0.0;
    mspass::utility::VectorStatistics<double> ampstats(amps);
    if(use_mean)
    {
      avgamp=ampstats.mean();
    }
    else
    {
      avgamp=ampstats.median();
    }

    /* restore to a value instead of natural log*/
    avgamp=exp(avgamp);
    double dscale=level/avgamp;
    /* Now scale the data and apply calib */
    for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
    {
      if(dptr->live())
      {
        double calib;
        (*dptr) *= dscale;
        if(dptr->is_defined(scale_factor_key))
        {
          calib=dptr->get_double(scale_factor_key);
        }
        else
        {
          calib=1.0;
        }
        calib/=dscale;
        dptr->put(scale_factor_key,calib);
      }
    }
    return avgamp;
  }catch(...){throw;};
}
} // namespace end
#endif
