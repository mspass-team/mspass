#ifndef _MSPASS_ALGORITHMS_H_
#define _MSPASS_ALGORITHMS_H_
/* This file should contain function protypes for all simple function
   algorithms that are to be part of mspass.
   *
   */
#include "mspass/algorithms/TimeWindow.h"
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
  const mspass::algorithms::TimeWindow& tw);
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
  const mspass::algorithms::TimeWindow& tw);
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
	std::string key, mspass::algorithms::TimeWindow tw);
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

\exception This function will throw a MsPASSError if the component number is
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
  (mspass::seismic::ThreeComponentEnsemble& din, std::string key, mspass::algorithms::TimeWindow tw);
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
/*! \brief Combine a grouped set of TimeSeries into one Seismogram.

A Seismogram object is a bundle of TimeSeries objects that define a
nonsingular tranformation matrix that can be used to reconstruct vector
group motion.   That requires three TimeSeries objects that have
define directions that are linearly independent.   This function does not
directly test for linear independence but depends upon channel codes
to assemble one or more bundles needed to build a Seismogram.  The
algorithm used here is simple and ONLY works if the inputs have been
sorted so the channels define a group of three unique channel codes.
For example,
  HHE, HHN, HHZ
would form a typical seed channel grouping.

The function will attempt to handle duplicates.  By that I mean
if the group has two of the same channel code like these sequences:
  HHE, HHE, HHN, HHZ  or HHE, HHN, HHN, HHZ, HHZ
If the duplicates are pure duplicates there is no complication and
the result will be clean.   If the time spans of the duplicate
channels are different the decision of which to use keys on a simple
idea that is most appropriate for data assembled by event with
mistakes in associations.  That is, it attempts to scans the group for
the earliest start time.  When duplicates are found it uses the one
with a start time closest to the minimum as the one merged to make the
output Seismogram.

The output will be marked as dead data with no valid data in one of
two conditions:  (1)  less than 3 unique channel names or (2) more than
three inputs with an inconsistent set of SEED names.   That "inconsistent"
test is obscure and yet another example that SEED is a four letter word.
Commentary aside, the rules are:
1.  The net code must be defined and the same in all TimeSeries passed
2.  The station (sta) code must also be the same for all inputs
3.  Similarly the loc code must be the same in all inputs.
4.  Finally, there is a more obscure test on channel names.  They must
all have the same first two characters.   That is, BHE, BHN, BHN, BHZ
is ok but BHE, BHN, BHZ, HHE will cause an immediate exit with no
attempt to resolve the ambiguity - that is viewed a usage error.

In all cases where the bundling is not possible the function does not
throw an exception but does four things:
1.  Merges the Metadata of all inputs (uses the += operator so only the
last values of duplicate keys will be preserved in the return)
2.  If ProcessingHistory is defined in the input they history records  are
posted to the returned Seismogram using as if the data were live but the
number of input will always be a number different from 3.
3.  The return is marked dead.
4.  The function posts a (hopefully) informative message to elog of the
returned Seismogram.

\param d is the vector of TimeSeries containing data to be bundled.
(This could be a members of a sorted Ensemble of a python array in the python
api).
\param i0 is the first component of the vector to view as part of the group.
\param iend is the last component of the vector to view as part of the group.
(inclusive not as in a vector size numbger)

\exception This routine should only throw system generated exceptions.
Errors from attempting to construct a Seismogram generate elog messages
and kills of an output components.
*/

mspass::seismic::Seismogram BundleSEEDGroup
    (const std::vector<mspass::seismic::TimeSeries>& d,
                const size_t i0, const size_t iend);
/*! \brief Assemble a SeismogramEnsemble from a sorted TimeSeriesEnsemble.

This function can be used to take an (unordered) input ensemble of
TimeSeries objects generated from miniseed data and produce an output
ensemble of Seismograms produced by bundles linked to the seed name
codes net, sta, chan, and loc.   An implicit assumption of the algorithm
used here is that the data are a variant of a shot gather and the
input ensemble defines one net:sta:chan:loc:time_interval for each
record that is to be bundled.   It can only properly handle pure
duplicates for a given net:sta:chan:loc combination.  (i.e. if
the input has the same TimeSeries defined by net:sta:chan:loc AND
a common start and end time).   Data with gaps broken into multiple
net:sta:chan:loc TimeSeries with different start and end times
will produce incomplete results.   That is, Seismograms in the output
associated with such inputs will either be killed with an associated
error log entry or in the best case truncated to the overlap range of
one of the segments with the gap(s) between.

Irregular start times of any set of TimeSeries forming a single
bundle are subject to the same truncation or discard rules described
in the related function Bundle3C.

Finally, it is VERY IMPORTANT to realize that the input ensemble will
almost certainly be altered by this algorithm as the very first thing
it does is a sequential sort on the seed keys: net, sta, loc, chan
(in that order).   Not for convenience in working with MongoDB the
null net or loc codes are handled cleanly and treated more or less as
if they were an empty string.   The sort order, however, may not be the same
the as empty and null are not actually treated identically.

\param d is the input ensemble of TimeSeries to be processed.

\exception This function will throw a MsPASSError if any member of d
does not have sta or chan defined (as noted above null net or loc are
handled.)

*/
mspass::seismic::Ensemble<mspass::seismic::Seismogram> bundle_seed_data
    (mspass::seismic::Ensemble<mspass::seismic::TimeSeries>& d);
/*! \brief Sort a TimeSeriesEnsemble with a natural order with seed name codes.

The seed standard tags every single miniseed record with four string keys
that seed uses to uniquely define a single data channel.  In MsPASS the keys
used for these name keys are:  net, sta, chan, and loc.  This function
applies the same sort algorithm used in the bundle_seed_data algorithm to
allow clean grouping into channels that can be assembled into
three component (Seismogram) bundles.  That means we sort the ensemble
data with the four keys in this order:  net, sta, loc, chan.

We provide this function because the process of doing such a sort is far
from trivial to do in a robust way.   A python programmer has easier tools
for sorting BUT those standard tools cannot handle a common data problem
that can be encountered with real data.  That is, there is a high
probability not all the seed keys are defined.   In particular, data
coming from a system based on the css3.0 relational data base (e.g. Antelope)
may not have net or loc set.   The sorting algorith here handle null net or
loc codes cleanly by treating the null case as a particular value.   Without
those safeties the code would throw an error if net or loc were null.

Note this algorithm alters the ensemble it receives in place.

\param d is the ensemble to be sorted.
*/
void seed_ensemble_sort(mspass::seismic::Ensemble<mspass::seismic::TimeSeries>& d);
}//End mspass::algorithms namespace encapsulation
#endif
