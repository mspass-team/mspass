#ifndef _MSPASS_ALGORITHMS_H_
#define _MSPASS_ALGORITHMS_H_
/* This file should contain function protypes for all simple function
   algorithms that are to be part of mspass.
   *
   */
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Ensemble.h"
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
TimeSeries agc(Seismogram& d,const double twin);
/*! \brief Extracts a requested time window of data from a parent Seismogram object.

It is common to need to extract a smaller segment of data from a larger
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
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
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger CoreTimeSeries object to be windowed
\param tw defines the data range to be extracted from parent.
*/
TimeSeries WindowData(const TimeSeries& parent, const TimeWindow& tw);
/* This set of procedures are ancessors of seismogram_helpers.   They 
 * were moved to algorithms June 2020 for mspass */
/*! \brief Return a new Seismogram in an arrival time (relative) refernce frame.

 An arrival time reference means that the time is set to relative and
 zero is defined as an arrival time extracted from the metadata area of
 the object.  The key used to extract the arrival time used for the
 conversion is passed as a variable as this requires some flexibility.
 To preserve the absolute time standard in this conversion the 0 time
 computed from the arrival time field is used to compute the absolute
 time of the start of the output seismogram as atime+t0.  This result
 is stored in the metadata field keyed by the word "time".  This allows
 one to convert the data back to an absolute time standard if they so
 desire, but it is less flexible than the input key method.

\exception SeisppError for errors in extracting required information from metadata area.

\param din  is input seismogram
\param key is the metadata key used to find the arrival time to use as a reference.
\param tw is a TimeWindow object that defines the window of data to extract around
    the desired arrival time.
**/
std::shared_ptr<Seismogram> ArrivalTimeReference(Seismogram& din,
	std::string key, mspass::TimeWindow tw);
/*!
 Extract one component from a Seismogram and
 create a TimeSeries object from it.

 Copies all Metadata from parent Seismogram to build a TimeSeries
 object.  Note that process will often leave relics like transformation
 matrix components.

\param tcs is the Seismogram to convert.
\param component is the component to extract (0, 1, or 2)

\return TimeSeries of component requested
**/
mspass::CoreTimeSeries ExtractComponent(const Seismogram& tcs,
		const unsigned int component);
/* Enemble algorithms */
/*! \brief  Returns a gather of Seismograms in an arrival time reference fram.

 An arrival time reference means that the time is set to relative and
 zero is defined as an arrival time extracted from the metadata area of
 each member object.

\exception SeisppError for errors in extracting required information from metadata area.

\param din  is input gather
\param key is the metadata key used to find the arrival time to use as a reference.
\param tw is a TimeWindow object that defines the window of data to extract around
    the desired arrival time.
**/
std::shared_ptr<ThreeComponentEnsemble> ArrivalTimeReference
  (ThreeComponentEnsemble& din,std::string key, TimeWindow tw);
}//End mspass namespace encapsulation
#endif
