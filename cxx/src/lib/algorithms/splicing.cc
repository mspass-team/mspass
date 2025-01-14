#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/TimeSeriesWGaps.h"
#include "mspass/utility/MsPASSError.h"
#include <float.h>
#include <math.h>
#include <sstream>
namespace mspass::algorithms {
using namespace mspass::seismic;
using namespace mspass::utility;
using namespace std;
using mspass::algorithms::WindowData;

/* A main idea of the functions in this file is to handle time
   tears created by a range of possibilities.  We have to handle
   overlaps and gaps.   Both are defined as existing when
   there is a time mismatch larger than the following fraction
   of a sample interval.  i.e. all tests are done using a tolerance
   of some variant of the sample interval in seconds time this
   multiplier.
Note:   if ever changed this constant should be consistent with
 the same constant used by the minised indexing function,. */
const double TIME_TEAR_TOLERANCE(0.5);
/*! File scope class to enscapsulate set of possible data problems.

Merging multiple data segments to a single time series, which is
a common need with continous data, is prone to a number of practical problems.
Clock issues and the design of modern digitizers can cause a mismatch in
time computed by dt*nsamp and data time tags with formats like miniseed.
This can create apparent gaps or overlaps.   This class is intended to be
used to scan a vector of time-sorted segment to flag issues that need to
be handled downstream in algorithms later in this file. Issues it addresses
at present are:
1.  irregular dt
2.  segments marked dead
3.  overlaps
4.  gaps
5.  Computes the length of the data being merged.  Callers should test that
   that number is not absurd and could cause a memory alloc fault.

All attributes of this class are intentionally public as it should be
thought of as a struct with convenient constructors.
*/
class SegmentVectorProperties {
public:
  bool dt_constant;
  bool has_dead_components;
  bool is_sorted;
  bool has_overlaps;
  bool has_gaps;
  int number_live;
  int first_live;
  /* This is set to the earliest start time of all segments. */
  double t0;
  /* This is set to lastest endtime of all segments */
  double endtime;
  double dt;
  size_t spliced_nsamp;
  /* This one holds messages best formed while scanning data that can be
  passed downstream. */
  ErrorLogger elog;
  SegmentVectorProperties();
  SegmentVectorProperties(const std::vector<TimeSeries> &segments);
  SegmentVectorProperties(const SegmentVectorProperties &parent);
};
SegmentVectorProperties::SegmentVectorProperties() : elog() {
  dt_constant = true;
  has_dead_components = false;
  is_sorted = true;
  has_overlaps = false;
  has_gaps = false;
  number_live = 0;
  first_live = 0;
  t0 = 0.0;
  endtime = 0.0;
  dt = 0.0;
  spliced_nsamp = 0;
}
SegmentVectorProperties::SegmentVectorProperties(
    const SegmentVectorProperties &parent)
    : elog(parent.elog) {
  dt_constant = parent.dt_constant;
  has_dead_components = parent.has_dead_components;
  is_sorted = parent.is_sorted;
  has_overlaps = parent.has_overlaps;
  has_gaps = parent.has_gaps;
  number_live = parent.number_live;
  first_live = parent.first_live;
  t0 = parent.t0;
  endtime = parent.endtime;
  dt = parent.dt;
  spliced_nsamp = parent.spliced_nsamp;
}
SegmentVectorProperties::SegmentVectorProperties(
    const std::vector<TimeSeries> &segments) {
  /* Return a type conversion of the input when there is only one
  segment - nothing to splice in that case.  Error if it is empty */
  if (segments.size() == 1) {
    dt_constant = true;
    has_dead_components = false;
    is_sorted = true;
    has_overlaps = false;
    has_gaps = false;
    if (segments[0].live()) {
      number_live = 1;
      first_live = 0;
      t0 = segments[0].t0();
      endtime = segments[0].endtime();
      dt = segments[0].dt();
      this->spliced_nsamp = segments[0].npts();
    } else {
      number_live = 0;
      first_live = -1;
      t0 = 0.0;
      dt = 0.0;
      endtime = 0.0;
      this->spliced_nsamp = 0;
    }
  } else if (segments.size() == 0) {
    dt_constant = true;
    has_dead_components = false;
    is_sorted = true;
    has_overlaps = false;
    has_gaps = false;
    number_live = 0;
    first_live = -1;
    t0 = 0.0;
    dt = 0.0;
    endtime = 0.0;
    this->spliced_nsamp = 0;
  } else {
    double test_dt, dtfrac;
    double first_t0, previous_t0, previous_endtime;
    /* This algorithm requires the test above that guarantees number of segments
    is more than 1.  Some complexity to handle a dead member 0 is needed. */
    bool this_is_first(true);
    has_overlaps = false;
    is_sorted = true; // initialization
    has_dead_components = false;
    /* Declare a sample mismatch if nondimensional sample interval mismatch is
    less than this constant. */
    const double dt_fraction_mismatch(0.001);
    for (size_t i = 0; i < segments.size(); ++i) {
      if (segments[i].dead()) {
        has_dead_components = true;
        continue;
      }
      if (this_is_first) {
        this->dt_constant = true;
        this->first_live = i;
        first_t0 = segments[i].t0();
        previous_t0 = first_t0;
        test_dt = segments[i].dt();
        previous_endtime = segments[i].endtime();
        this_is_first = false;
        this->dt = test_dt;
        this->t0 = first_t0;
        this->number_live = 1;
      } else {
        /* This could be true multiple times if data were not sorted.
        Purpose of this boolean is to define a serious error condition
        that functions using this class must handle */
        if (segments[i].t0() < previous_t0)
          is_sorted = false;
        /* Test for overlaps - note sign is important */
        if ((previous_endtime + test_dt * TIME_TEAR_TOLERANCE) >
            segments[i].t0())
          has_overlaps = true;
        /* test for gaps */
        if ((segments[i].t0() - previous_endtime - test_dt) / test_dt >
            TIME_TEAR_TOLERANCE)
          has_gaps = true;
        /* Check for sample interval mismatch */
        dtfrac = fabs(segments[i].dt() - test_dt) / test_dt;
        if (dtfrac > dt_fraction_mismatch)
          dt_constant = false;
        previous_endtime = segments[i].endtime();
        previous_t0 = segments[i].t0();
        ++this->number_live;
      }
    }
    this->endtime = previous_endtime;
    this->spliced_nsamp = lround((previous_endtime - this->t0) / this->dt);
    ++this->spliced_nsamp; // nsamp is always one sample longer than intervals
  }
}
/*! Convenience operator to dump content of SegmentVectorProperties object.

Writes a verbose dump of the content with labels.  This overloaded
function is for debugging and will not be in the MsPASS python bindings.
In fact, neither is SegmentVectorProperties.
Note this does not need to be declared friend because all attributes of the
class are declared public.
*/
ostream &operator<<(ostream &os, SegmentVectorProperties &svp) {
  const string sep(
      "=============================================================");
  os << sep << endl;
  if (svp.dt_constant)
    os << "Segments have constant sample interval" << endl;
  if (svp.has_dead_components)
    os << "Segments has one or more segments marked dead" << endl;
  if (!svp.is_sorted)
    os << "Data do not appear to be sorted - this cause an exception to be "
          "thrown in the splice_segments function"
       << endl;
  if (svp.has_overlaps)
    os << "Segments have one or more overlaps" << endl;
  if (svp.has_gaps)
    os << "Segments have one or more gaps" << endl;
  os << "Sample interval of these data=" << svp.dt << endl;
  os << "spliced_nsamp attribute value=" << svp.spliced_nsamp << endl;
  os << "number of live segments=" << svp.number_live << endl;
  os << "vector component number of first live segment=" << svp.first_live
     << endl;
  os << std::setprecision(15);
  os << "Start time of group=" << svp.t0 << endl;
  os << "endtime of group=" << svp.endtime << endl;
  os << "time span of all segments=" << svp.endtime - svp.t0 << endl;
  os << "Cross check of computed nsamp as a float value="
     << (svp.endtime + svp.dt - svp.t0) / svp.dt << endl;
  os << sep << endl;
  return os;
}
/* Splice TimeSeries data stored in a std::vector container.   Assume segments
are sorted in time and otherwise consistent.

This is a big memory algorithm and returns the result as a single
time series with zeros in the array filling the gaps. A few sanity checks
are made on the inputs to avoid memory faults from invalid data.
The function expects input to not have overlaps.   It will kill any
segments in the return for which the endtime of the segment overlaps the
next segment in the vector received.

This function will abort with an exception thrown in either of two situations:
1.  segments are not ordered by t0
2.  not all segments have the same sample rate.
Both are considered fatal errors as they would indicate a mistake in
usage that cannot be recovered.

The functionr returns a copy of the input if there is only one segment.

If the segments array is empty it returns a null length, dead TimeSeriesWGaps
object contain an error message.
*/
TimeSeriesWGaps splice_segments(std::vector<TimeSeries> &segments,
                                bool save_history) {
  /* This class is created to look for potential data problems to handle. */
  SegmentVectorProperties issues(segments);
  // DEBUG
  // cout << issues;
  /* Return a type conversion of the input when there is only one
  segment - nothing to splice in that case.  Error if it is empty */
  if (issues.number_live == 1) {
    return TimeSeriesWGaps(segments[issues.first_live]);
  } else if (issues.number_live == 0) {
    TimeSeriesWGaps nodata_result;
    nodata_result.elog.log_error(
        "slice_segments",
        "Function received a zero length container of TimeSeries segments",
        ErrorSeverity::Invalid);
    return nodata_result;
  }
  if (!issues.is_sorted) {
    stringstream ss;
    ss << "splice_segments:  input segments do not appear to have been sorted "
          "into time order"
       << endl
       << "Fatal condition indicating a coding error.  Data must be time "
          "sorted for this algorithm to work"
       << endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
  } else if (!issues.dt_constant) {
    stringstream ss;
    ss << "splice_segments:  "
       << "segments do not have constant sample rate." << endl
       << "Live data sample intervals received:  " << endl;
    for (size_t i = 0; i < segments.size(); ++i) {
      if (segments[i].live()) {
        ss << segments[i].dt() << endl;
      }
    }
    throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
  }

  /* This assembles the bones of the result but initializes the sample
  data to a empty container.   We fix that later when we do splicing.
  It is convenient to do it here, however, to provide a skeleton to return
  if there are errors. Note the pts attibute in this result will probably
  be a nonzero value even though the data vector has zero length.  We
  make sure it is initially marked dead to prevent downstream problem in
  mishandling error returns.  The correct history using multiple inputs is
  always loaded here when enabled*/

  std::vector<double> null_vector;
  /* This is a bit of a weird/complicated construct using the WGaps constructor
  built from a TimeSeries.  That initializes the gaps container to empty*/
  TimeSeriesWGaps result(
      TimeSeries(dynamic_cast<BasicTimeSeries &>(segments[issues.first_live]),
                 dynamic_cast<Metadata &>(segments[issues.first_live]),
                 ProcessingHistory(), null_vector));

  if (save_history) {
    std::vector<ProcessingHistory *> inputs;
    /* We assume history is initially empty - careful if the logic above
    changes */
    for (size_t i = 0; i < segments.size(); ++i) {
      ProcessingHistory *hptr;
      if (segments[i].live()) {
        hptr = dynamic_cast<ProcessingHistory *>(&segments[i]);
        inputs.push_back(hptr);
      }
    }
    result.add_many_inputs(inputs);
  }

  const long int MAX_DATA_VECTOR_LENGTH(100000000); // generous size allowance
  if (issues.has_overlaps) {
    std::stringstream ss;
    ss << "Segments array has sections with overlaps of more than 1/2 sample"
       << std::endl
       << "Preprocess your data to remove overlaps.   This algorithm assumes "
          "overlaps were repaired previously."
       << endl;
    result.elog.log_error("splice_segments", ss.str(), ErrorSeverity::Invalid);
    /* force a clear buffer*/
    result.set_npts(0);
    result.kill();
  } else if (issues.spliced_nsamp > MAX_DATA_VECTOR_LENGTH) {
    std::stringstream ss;
    ss << "Computed vector length is huge=" << issues.spliced_nsamp
       << " and exceed internal safety net size=" << MAX_DATA_VECTOR_LENGTH
       << std::endl
       << "This datum will be killed" << std::endl;
    result.elog.log_error("splice_segments", ss.str(), ErrorSeverity::Invalid);
    /* force a clear buffer*/
    result.set_npts(0);
    result.kill();
  } else {
    /* Land here when it appears ok to proceed.*/
    result.set_t0(issues.t0);
    result.set_dt(issues.dt);
    /* Note the algorithm used here assumes this method initializes the
    data vector (symbol s) to spliced_nsamp zeros.  Gaps will then
    not need to be zero filled.*/
    result.set_npts(issues.spliced_nsamp);
    double previous_endtime, delta;
    previous_endtime = segments[issues.first_live].endtime();
    size_t ii;
    for (size_t i = issues.first_live; i < segments.size(); ++i) {
      if (segments[i].dead())
        continue;
      for (size_t j = 0; j < segments[i].s.size(); ++j, ++ii) {
        double t;
        t = segments[i].time(j);
        ii = result.sample_number(t);
        if (ii >= result.npts()) {
          stringstream ss;
          ss << "splice_segments:  computed sample index is outside merge data "
                "vector"
             << endl
             << "Computed index for segment number " << i << " sample number "
             << j << " is " << ii
             << " which is larger than array length=" << result.npts() << endl
             << "This should not happen and is a bug that needs to be repaired"
             << endl;
          throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
        }
        result.s[ii] = segments[i].s[j];
      }
      /* We use a signed test here because we can be sure there are no
      overlaps that create negative delta values larger than TIME_TEAR_TOLERANCE
      value (currently const 0.5).*/
      delta = (segments[i].t0() - issues.dt - previous_endtime) / issues.dt;
      if (delta > TIME_TEAR_TOLERANCE) {
        TimeWindow gap;
        gap.start = previous_endtime;
        gap.end = segments[i].t0();
        result.add_gap(gap);
      }
      previous_endtime = segments[i].endtime();
    }
    result.set_live();
  }
  return result;
}
/* helper for repair_overlaps.   Acts a bit like numpy is_close. */
bool samples_match(std::vector<double> &v1, std::vector<double> &v2) {
  /* Use 32 bit float eps because no data at present has a greater precision
  than 24 bits.  We scale by 10 to be a bit cautious.  Better to return
  true than false for one or two sampls.  10 may be too large - this probably
  should be tested with data */
  const double SCALED_EPS(10.0 * FLT_EPSILON);
  /* Because of internal use we don't test if v1 and v2 are the same length
  but assume logic used to create them guarantees that is so. */
  std::vector<double>::iterator v1ptr, v2ptr;
  for (v1ptr = v1.begin(), v2ptr = v2.begin(); v1ptr != v1.end();
       ++v1ptr, ++v2ptr) {
    double dtest;

    dtest = *v1ptr - *v2ptr;
    dtest = fabs(dtest / (*v1ptr));
    // DEBUG
    // cout << *v1ptr << " "<<*v2ptr<<" "<<dtest<<endl;
    if (dtest > SCALED_EPS)
      return false;
  }
  return true;
}
/*! Function to handle overlapping data segments.

Overlapping data is a common raw data issue.  Data retrieved from data
centers has normally been purged from archives, but overlaps can still
occur.  The most common example is two miniseed files may contain the
same data and the user did not realize that data was present in the files.

The approach of this algorithm is to try to repair overlaps that have
matching sample data.   It looks for matches in a way that should work
even if the data have been scaled AS LONG AS the scaling is the same
for the two overlappign sections.   Whenever an overlap is found the
first segment of the pair is truncated and the second is retained.
If the overlapping segment data do not compare sample for sample the
first segmet of the pair (in time order) is killed, and error message is
posted, and the killed datum is pushed to the output.   The first is
always killed because with current generation data that situation always
means there has been a time jump backward to create the apparent overlap.
It is assumed that the segment killed had a timing problem.

*/
std::vector<TimeSeries> repair_overlaps(std::vector<TimeSeries> &segments) {
  SegmentVectorProperties issues(segments);
  // DEBUG
  /*
  cout << "In repair_overlaps"<<endl;
  cout << issues;
  */
  if (!issues.is_sorted) {
    stringstream ss;
    ss << "repair_overlaps:  input segments do not appear to have been sorted "
          "into time order"
       << endl
       << "Fatal condition indicating a workflow error.  Data must be time "
          "sorted for this algorithm to work"
       << endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
  } else if (!issues.dt_constant) {
    stringstream ss;
    ss << "repair_overlaps:  "
       << "segments do not have constant sample rate." << endl
       << "Live data sample intervals received:  " << endl;
    for (size_t i = 0; i < segments.size(); ++i) {
      if (segments[i].live()) {
        ss << segments[i].dt() << endl;
      }
    }
    throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
  } else if (issues.number_live <= 1) {
    /* By definition overlaps are not possible with zero or 1 segment.*/
    return segments;
  }

  /* This is a simple test to do nothing if there are no overlaps - that is
  the else clause with this logic */
  if (issues.has_overlaps) {
    std::vector<TimeSeries> repaired_segments;
    int i_previous;
    i_previous = issues.first_live;
    for (size_t i = issues.first_live + 1; i < segments.size(); ++i) {
      /* Note this logic just ignores all dead data like if(dead) ->continue*/
      if (segments[i].live()) {
        double ttest;
        ttest = segments[i_previous].endtime() + segments[i].dt() -
                segments[i].t0();
        if (ttest < (TIME_TEAR_TOLERANCE * segments[i].dt())) {
          repaired_segments.push_back(segments[i_previous]);
          i_previous = i;
        } else {
          // DEBUG
          // cout << "Handling overlap with ttest="<<ttest<<" at end of segment
          // number "<<i_previous<<endl;
          /* We use these two vectors to hold overlapping section and
          pass them to comparison function */
          std::vector<double> vec1, vec2;
          double tstart;
          size_t w_npts;
          tstart = segments[i].t0();
          for (size_t iw = segments[i_previous].sample_number(tstart);
               iw < segments[i_previous].npts(); ++iw) {
            vec1.push_back(segments[i_previous].s[iw]);
          }
          w_npts = vec1.size();
          // DEBUG
          // cout << "Testing overlap window of length="<<w_npts<<endl;
          for (size_t iw = 0; iw < w_npts; ++iw) {
            /* This is necessary for overlaps larger than the span of
             * segments[i]*/
            if (iw >= segments[i].npts()) {
              w_npts = vec2.size();
              break;
            } else {
              vec2.push_back(segments[i].s[iw]);
            }
          }
          if (samples_match(vec1, vec2)) {
            // DEBUG
            // cout << "samples_match returned true.  In repair section"<<endl;
            TimeSeries repaired_datum;
            TimeWindow repair_window;
            repair_window.start = segments[i_previous].t0();
            repair_window.end = segments[i].t0() - issues.dt;
            // DEBUG
            // cout << "time span of repair window="<<
            // repair_window.end-repair_window.start<<endl;
            /* This condition occurs when the current is a pure duplicate
            of the last or at least the start times match */
            if (repair_window.end < repair_window.start)
              continue;
            repaired_datum = WindowData(segments[i_previous], repair_window);
            repaired_segments.push_back(repaired_datum);
          } else {
            /* We assume there is a serious problem with the i_previous
            segment if the overlapping data do not match.  The only time
            I (glp) have seen this situation is if the timing system on
            an instrument as failed.   When the instrument acquires time
            it create a time jump to create an overlap where the data do
            not match.   For that reason we kill the i_prevoius segments,
            post a message, and push it to the output.   That allows
            caller to retrieve the error message if they wish. */
            TimeSeries bad_data(segments[i_previous]);
            bad_data.kill();
            bad_data.elog.log_error("repaired_segments",
                                    "Overlapping sample data do not match - "
                                    "assuming timing on this segment was bad",
                                    ErrorSeverity::Invalid);
            repaired_segments.push_back(bad_data);
          }
          i_previous = i;
        }
      }
    }
    /* Push the last live segment - defined by i_previous*/
    repaired_segments.push_back(segments[i_previous]);
    return repaired_segments;
  } else {
    return segments;
  }
}
} // namespace mspass::algorithms
