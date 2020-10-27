#ifndef _MSPASS_ALGORITHMS_H_
#define _MSPASS_ALGORITHMS_H_
/* This file should contain function protypes for all simple function
   algorithms that are to be part of mspass.
   *
   */
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Ensemble.h"
namespace mspass::algorithms{
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
mspass::seismic::TimeSeries agc(mspass::seismic::Seismogram& d,const double twin);
/*! \brief Extracts a requested time window of data from a parent Seismogram object.

It is common to need to extract a smaller segment of data from a larger
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger Seismogram object to be windowed
\param tw defines the data range to be extracted from parent.
*/
mspass::seismic::Seismogram WindowData3C(const mspass::seismic::Seismogram& parent, 
  const mspass::seismic::TimeWindow& tw);
/*! \brief Extracts a requested time window of data from a parent TimeSeries object.

It is common to need to extract a smaller segment of data from a larger
time window of data.  This function accomplishes this in a nifty method that
takes advantage of the methods contained in the BasicTimeSeries object for
handling time.

\return new Seismgram object derived from  parent but windowed by input
      time window range.

\exception MsPASSError object if the requested time window is not inside data range

\param parent is the larger TimeSeries object to be windowed
\param tw defines the data range to be extracted from parent.
*/
mspass::seismic::TimeSeries WindowData(const mspass::seismic::TimeSeries& parent, 
  const mspass::seismic::TimeWindow& tw);
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
std::shared_ptr<mspass::seismic::Seismogram> ArrivalTimeReference(mspass::seismic::Seismogram& din,
	std::string key, mspass::seismic::TimeWindow tw);
/*! \brief Extract one component from a Seismogram and create a TimeSeries object from it.

 Copies all Metadata from parent Seismogram to build a TimeSeries
 object.  This will often leave relics of the transformation matrix
 components in the header so be aware.  If the processing history section
 is not marked as empty new_map will be called to record this algorithm
 was involked.   The algid saved is internally set to the form
 "component=n" where n is 0, 1, or 2.  With one simple argument the baggage of
 maintaining that detail seems unnecessary.   This is a variant that
 may cause problems downstream so I'm noting that to be aware as this
 system develops.


\param tcs is the Seismogram to convert.
\param component is the component to extract (0, 1, or 2)

\expeption This function will throw a MsPASSError if the component number is
  illegal

\return TimeSeries of component requested
**/
mspass::seismic::TimeSeries ExtractComponent(const mspass::seismic::Seismogram& tcs,
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
std::shared_ptr<mspass::seismic::ThreeComponentEnsemble> ArrivalTimeReference
  (mspass::seismic::ThreeComponentEnsemble& din, std::string key, mspass::seismic::TimeWindow tw);
/*! \brief Extract one component from a 3C ensemble.
 *
 This function creates an ensemble of TimeSeries objects that are
 a specified component extracted from an ensemble of 3C objects.
 It clones the metadata of the parent for the output ensemble metadata.
 Each member is created by a call to the (overloaded) function that
 extracts a component from each member of the parent.   That function
 currently also clones the metadata.  That is notable as there are
 metadata components that make sense only on each side of the transformation.
 A notable problem at this writing is that the Seismogram converter
 does not set hang and vang in the output.  This might cause downstream
 problems - REMOVE THIS COMMENT WHEN THAT IS FIXED.

 \param d - is the input ensemble.
 \param comp - is the component number to extract.

 \return Ensemble<TimeSeries> of component comp data.
 \exception Will throw a MsPASSError exception if the ensemble
   input is incompatible or the component number is not 0,1, or 2.
   */
mspass::seismic::Ensemble<mspass::seismic::TimeSeries> ExtractComponent(
  const mspass::seismic::Ensemble<mspass::seismic::Seismogram>& d,
	const unsigned int comp);
/*! \brief Sparse time domain convolution.
Sometimes with modeling we have an data series (d) that is sparse
that we want to convolve with a wavelet to produce a simulation data
for deconvolution.   This small function implements a sparse convolution
algorithm in the time domain.  It uses a daxpy sum only summing components
of d testing nonzero.
Note if d is not sparse this reduces to normal convolution with a daxpy
algorithm.  The cost is marginally higher than a dense time domain
convolution, especially if the size of the wavelet is larger since then
the sum over the size of the wavelet will dominate over the single test
for zeros in d.
\param wavelet is the wavelet to be convolved with d (not sparse)
\param d is the sparse data vector (dominated by zeros).
*/
mspass::seismic::CoreSeismogram sparse_convolve(
    const mspass::seismic::CoreTimeSeries& wavelet,
		const mspass::seismic::CoreSeismogram& d);
}//End mspass::algorithms namespace encapsulation
#endif
