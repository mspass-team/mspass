#ifndef _MSPASS_ALGORITHMS_H_ 
#define _MSPASS_ALGORITHMS_H_
/* This file should contain function protypes for all simple function
   algorithms that are to be part of mspass.   
   *
   */
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
namespace mspass{
/* \brief Apply agc operator to three component seismogram data.

   Automatic gain control (agc) is a standard operation in seismic
reflection processing.  The algorithm used her is a variant of that in
seismic unix but applied to vector data.   That is scaling is no 
determined by absolute value of each sample but th vector amplitude of 
each sample.  Scaling is determined by the average vector amplitude
over a specified time window length.  There isa  ramp in and ramp off 
range of size equal to the window length.   agc was notorious in the 
early days of seismic processing for making it impossible to recover 
true amplitude.   We remove that problem here by returning a TimeSeries
object whose contents contain the gain applied to each sample of the 
original data.   

\param d - data to apply the operator to.  Note it is altered.  
\param twin - length of the agc operator in seconds

\return TimeSeries object with the same number of samples as d. The 
  value of each sample is the gain applied at the comparable sample in d.

This function does not throw an exception, but can post errors to the 
ErrorLogger object that is a member of Seismogram.  
*/
TimeSeries agc(Seismogram& d,const double twin) noexcept;
}//End mspass namespace encapsulation
#endif
