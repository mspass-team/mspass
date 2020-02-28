#ifndef _MSPASS_ALGORITHMS_H_ 
#define _MSPASS_ALGORITHMS_H_
/* This file should contain function protypes for all simple function
   algorithms that are to be part of mspass.   
   *
   */
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
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
true amplitude.   We remove that problem here by returning a CoreTimeSeries
object whose contents contain the gain applied to each sample of the 
original data.   

\param d - data to apply the operator to.  Note it is altered.  
\param twin - length of the agc operator in seconds

\return CoreTimeSeries object with the same number of samples as d. The 
  value of each sample is the gain applied at the comparable sample in d.

This function does not throw an exception, but can post errors to the 
ErrorLogger object that is a member of CoreSeismogram.  
*/
CoreTimeSeries agc(CoreSeismogram& d,const double twin) noexcept;
/*! \brief Extracts a requested time window of data from a parent CoreSeismogram object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicCoreTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreSeismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
CoreSeismogram WindowData3C(const CoreSeismogram& parent, const TimeWindow& tw);
/*! \brief Extracts a requested time window of data from a parent CoreTimeSeries object.

It is common to need to extract a smaller segment of data from a larger 
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicCoreTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreTimeSeries object to be windowed
\param tw defines the data range to be extracted from parent.
*/
CoreTimeSeries WindowData(const CoreTimeSeries& parent, const TimeWindow& tw);
/*! Sparse convolution routine.

  Sometimes a time series is made up initially of only a relatively 
  small number of impulses.  A case in point is some simple synthetic
  In that case, standard convolution methods are unnecessarily slow.
  This specialized function can sometimes be useful in such a context.

  \param wavelet is assumed to be the nonsparse wavelet that will
    be replicated with appropriate lags for each impulse in d
  \param d is the sparse CoreSeismogram object that to which
    the wavelet function is to be convolved.  The contents of this 
    object are assumed to be mostly zeros or the algorithm is not 
    very efficient. It will work for data that is not sparse, but it will
    be slow compared to convolution by Fourier transforms.
  \return CoreSeismogram object that is the convolution of
    wavelet with d.  Result will have more samples than d by 2 times
    the length of wavelet 
*/
CoreSeismogram sparse_convolve(const CoreTimeSeries& wavelet, 
        const CoreSeismogram& d);
}//End mspass namespace encapsulation
#endif
