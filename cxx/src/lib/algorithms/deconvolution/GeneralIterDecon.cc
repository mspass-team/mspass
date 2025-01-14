#include "mspass/algorithms/deconvolution/GeneralIterDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <math.h>
#include <vector>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using namespace mspass::algorithms;

IterDeconType parse_for_itertype(const AntelopePf &md) {
  string sval = md.get_string("deconvolution_type");
  if (sval == "water_level")
    return WATER_LEVEL;
  else if (sval == "least_square")
    return LEAST_SQ;
  else if (sval == "multi_taper")
    return MULTI_TAPER;
  else
    throw MsPASSError("GeneralIterDecon: unknown or illegal value of "
                      "deconvolution_type parameter=" +
                          sval,
                      ErrorSeverity::Invalid);
}
double Linf(dmatrix &d) {
  int nc, nr;
  nr = d.rows();
  nc = d.columns();
  double *dmax;
  /* This C++ generic algorithm works on raw pointers in a vector so
  because a dmatrix stores the data as a contiguous memory block we
  can use it in one call.*/
  dmax = max_element(d.get_address(0, 0), d.get_address(nr - 1, nc - 1));
  return *dmax;
}
/* Similar function for L2 norm to Linf but here we use dnrm2. */
double L2(dmatrix &d) {
  int nd;
  nd = d.rows() * d.columns();
  double dl2;
  dl2 = cblas_dnrm2(nd, d.get_address(0, 0), 1);
  return dl2;
}
GeneralIterDecon::GeneralIterDecon(AntelopePf &mdtoplevel) : ScalarDecon() {
  const string base_error("GeneralIterDecon AntelopePf contructor:  ");
  stringstream ss; // used for constructing error messages
  /* The pf used for initializing this object has Antelope Arr section
  for each algorithm.   Since the generalized iterative method is a
  two-stage algorithm we have a section for the iterative algorithm
  and a (variable) sectin for the preprocessor algorithm.  We use
  the AntelopePf to parse this instead of raw antelope pfget
  C calls. */
  try {
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgiter = md.get_branch("generalized_iterative_deconvolution");
    IterDeconType dct = parse_for_itertype(mdgiter);
    this->decon_type = dct;
    double ts, te;
    ts = mdgiter.get<double>("full_data_window_start");
    te = mdgiter.get<double>("full_data_window_end");
    dwin = TimeWindow(ts, te);
    ts = mdgiter.get<double>("deconvolution_data_window_start");
    te = mdgiter.get<double>("deconvolution_data_window_end");
    fftwin = TimeWindow(ts, te);
    ts = mdgiter.get<double>("noise_window_start");
    te = mdgiter.get<double>("noise_window_end");
    nwin = TimeWindow(ts, te);
    /* We need to make sure the noise and decon windows are inside the
     * full_data_window*/
    if (fftwin.start < dwin.start || fftwin.end > dwin.end) {
      stringstream ss;
      ss << base_error << "decon window error" << endl
         << "Wavelet inversion window is not inside analysis window" << endl
         << "full_data_window (analysis) range=" << dwin.start << " to "
         << dwin.end << endl
         << "decon_window (wavelet inversion) range=" << fftwin.start << " to "
         << fftwin.end << endl;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    noise_component = mdgiter.get<int>("noise_component");
    double target_dt = mdgiter.get<double>("target_sample_interval");
    int maxns = static_cast<int>((fftwin.end - fftwin.start) / target_dt);
    ++maxns; // Add one - points not intervals
    nfft = nextPowerOf2(maxns);
    /* This should override this even if it was previously set */
    mdgiter.put("operator_nfft", nfft);
    this->ScalarDecon::changeparameter(mdgiter);
    shapingwavelet = ShapingWavelet(mdgiter, nfft);
    /* Note minus sign here */
    time_shift = -(dwin.start) / target_dt;
    AntelopePf mdleaf;
    /* We make sure the window parameters in each algorithm mactch what
    is set for this algorithm.  Abort if they are not consistent.  The
    test code is a bit repetitious but a necessary evil to allow the message
    to be clearer. */
    int n1, n2; // temporaries used below -  needed because declrations illegal
                // inside case
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "water level method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "water level parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new WaterLevelDecon(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "least square method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "least square parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new LeastSquareDecon(mdleaf);
      break;
    case MULTI_TAPER:
    default:
      mdleaf = md.get_branch("multi_taper");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "mulittaper method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "multitaper parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      /* Here we also have to test the noise parameters, but the gid
      window can be different fromt that passed to the mulittaper method.
      Hence we test only that the mulittaper noise window is within the bounds
      of the gid noise window */
      n1 = static_cast<int>((fftwin.end - fftwin.start) / target_dt) + 1;
      n2 = static_cast<int>((nwin.end - nwin.start) / target_dt) + 1;
      if (n1 > n2) {
        ss << base_error << "inconsistent noise window specification" << endl
           << "multitaper parameters specify taper length=" << n1 << " samples"
           << endl
           << "GID noise window parameters define noise_window_start="
           << nwin.start << " and noise_window_end=" << nwin.end << endl
           << "The GID window has a length of " << n2 << " samples" << endl
           << "GID implementation insists multitaper noise window be smaller "
              "or equal to GID noise window"
           << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new MultiTaperXcorDecon(mdleaf);
    };
    /* Because this may evolve we make this a private method to
    make changes easier to implement. */
    this->construct_weight_penalty_function(mdgiter);
    /* Set convergencd parameters from md keys */
    iter_max = mdgiter.get<int>("maximum_iterations");
    lw_linf_floor = mdgiter.get<double>("lag_weight_Linf_floor");
    lw_l2_floor = mdgiter.get<double>("lag_weight_rms_floor");
    resid_linf_prob =
        mdgiter.get<double>("residual_noise_rms_probability_floor");
    resid_l2_tol = mdgiter.get<double>("residual_fractional_improvement_floor");
  } catch (...) {
    throw;
  };
}
GeneralIterDecon::~GeneralIterDecon() { delete preprocessor; }

vector<double> wtf;
int nwtf;
void GeneralIterDecon::construct_weight_penalty_function(const Metadata &md) {
  try {
    const string base_error(
        "GeneralIterDecon::construct_weight_penalty_function:  ");
    int i;
    /* All options use this scale factor */
    double wtf_scale = md.get<double>("lag_weight_penalty_scale_factor");
    if ((wtf_scale <= 0) || (wtf_scale > 1.0))
      throw MsPASSError(
          base_error +
              "Illegal value for parameter lag_weight_penalty_scale_factor\n" +
              "Must be a number in the interval (0,1]",
          ErrorSeverity::Invalid);
    /* This keyword defines the options for defining the penalty function */
    string wtf_type = md.get_string("lag_weight_penalty_function");
    /* Most options use this parameter so we set it outside the
    conditional.  With usual use of pffiles this should not be a big issue.
    */
    nwtf = md.get<int>("lag_weight_function_width");
    /* nwtf must be forced to be an odd number to force the function to
    be symmetric. */
    if ((nwtf % 2) == 0)
      ++nwtf;
    if (wtf_type == "boxcar") {
      for (i = 0; i < nwtf; ++i)
        wtf.push_back(wtf_scale);
    } else if (wtf_type == "cosine_taper") {
      /* This creates one cycle of cosine function with wavelength(period)
      of nwtf, offset by 0.5 and scaled in amplitude.   That means it tapers to
      1 at edges and has a minimum value of 1-wtf_scale. */
      double period = (double)(nwtf + 1); // set period so points one left and
                                          // right (1) can be dropped
      for (i = 0; i < nwtf; ++i) {
        double f;
        f = 0.5 * (-cos(2.0 * M_PI * ((double)(i + 1)) / period));
        f += 0.5;
        f = 1.0 - wtf_scale * f; // This makes minimum f=1-wtf_scale
        /* Avoid negatives */
        if (f < 0)
          f = 0.0;
        wtf.push_back(f);
      }
    } else if (wtf_type == "shaping_wavelet") {
      /* In this method we use the points in the wavelet at 1/2 max.
      We extract it from the shaping wavelet - the TimeSeries is baggage
      but not an efficiency issue since this is called only once */
      CoreTimeSeries ir = this->preprocessor->actual_output();
      /* assume the wavelet is symmetric and get the half max sample position
      from positive side only */
      int i0 = ir.sample_number(0.0);
      double peakirval = ir.s[i0];
      double halfmax = peakirval / 2.0;
      for (i = i0; i < ir.npts(); ++i) {
        if (ir.s[i] < halfmax)
          break;
      }
      int halfwidth = i;
      int peakwidth = 2 * i + 1; // guaranteed odd numbers
      for (i = i0 - halfwidth; i < (i0 + peakwidth); ++i) {
        double f;
        f = ir.s[i];
        f /= peakirval; // make sure scaled so peaks is 1.0
        f *= wtf_scale;
        f = 1.0 - f;
        wtf.push_back(f);
      }
    } else {
      throw MsPASSError(
          base_error +
              "illegal value for parameter lag_weight_penalty_function=" +
              wtf_type,
          ErrorSeverity::Invalid);
    }
  } catch (...) {
    throw;
  };
}
/* Some helpers for new implementation.*/
/* This procedure returns a vector of 3c amplitudes from a dmatrix extracted
from a Seismogram. */
vector<double> amp3c(dmatrix &d) {
  vector<double> result;
  int ncol = d.columns();
  int i, k;
  for (i = 0; i < ncol; ++i) {
    double mag;
    for (k = 0, mag = 0.0; k < 3; ++k) {
      mag += d(k, i) * d(k, i);
    }
    result.push_back(sqrt(mag));
  }
  return result;
}
int GeneralIterDecon::load(const CoreSeismogram &draw, TimeWindow dwin_in) {
  try {
    dwin = dwin_in;
    /* First we load the requested window.  Note we MUST always make this window
    a bit larger than the range of desired lags as the iterative algorithm will
    not allow lags at the edges (defined by a construction parameter
    wavelet_pad)
    */
    d_all = WindowData(draw, dwin);
    ndwin = d_all.npts();
    return 0;
  } catch (...) {
    throw;
  };
}
int GeneralIterDecon::loadnoise(const CoreSeismogram &draw,
                                TimeWindow nwin_in) {
  try {
    nwin = nwin_in;
    n = WindowData(draw, nwin);
    nnwin = n.npts();
    double ret = this->compute_resid_linf_floor();
    if (ret > 0)
      return 0;
    else
      return 1;
  } catch (...) {
    throw;
  };
}
int GeneralIterDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                           TimeWindow nwin) {
  try {
    int iretn, iret;
    iretn = this->loadnoise(draw, nwin);
    iret = this->load(draw, dwin);
    return (iretn + iret);
  } catch (...) {
    throw;
  };
}
/* These are the set of private methods called from the process method */
void GeneralIterDecon::update_residual_matrix(ThreeCSpike spk) {
  try {
    const string base_error("GeneralIterDecon::update_residual_matrix:  ");
    int ncol = this->r.u.columns();
    int col0 = spk.col - actual_o_0;
    ;
    /* Avoid seg faults and test range here.  This is an exception condition
     * because if the lag weights are created correctly the edge conditions
     * here should not be possible */
    if (col0 < 0)
      throw MsPASSError(base_error +
                            "Coding problem - computed lag is negative.  "
                            "lag_weights array is probably incorrect",
                        ErrorSeverity::Fatal);
    if ((col0 + actual_o_fir.size()) >= ncol)
      throw MsPASSError(base_error +
                            "Coding problem - computed lag is too large and "
                            "would overflow residual matrix and seg fault.\n"

                            + "lag_weights array is probably incorrect",
                        ErrorSeverity::Fatal);
    for (int k = 0; k < 3; ++k) {
      /*Use the gsl version of daxpy hre to avoid type collisions with perf.h.
       */
      cblas_daxpy(actual_o_fir.size(), -spk.u[k], &(actual_o_fir[0]), 1,
                  this->r.u.get_address(k, col0), 3);
    }
  } catch (...) {
    throw;
  };
}
/* This method adds (not multiply add) the weighting function created by
the constructor centered at lag = col.  Because a range can be hit multiple
times we test for negatives and zero them in the loop.   This is also
we we use an explicit loop instead ofa call to daxpy as in the residual
update method.  Note like update_residual_matrix we assume nwtf is
correct and don't test for memory faults for efficiency  */

void GeneralIterDecon::update_lag_weights(int col) {
  try {
    int i, ii;
    for (i = 0, ii = col; i < nwtf; ++i, ++ii) {
      lag_weights[ii] -= wtf[i];
      if (lag_weights[ii] < 0.0)
        lag_weights[ii] = 0;
    }
  } catch (...) {
    throw;
  };
}
double GeneralIterDecon::compute_resid_linf_floor() {
  try {
    /*Note - this needs an enhancement.   We should not include points
    in a padd region accounting for the inverse filter padding. */
    vector<double> amps(amp3c(n.u));
    sort(amps.begin(), amps.end());
    int floor_position;
    floor_position = static_cast<int>(resid_linf_prob * ((double)amps.size()));
    resid_linf_floor = amps[floor_position];
    return resid_linf_floor;
  } catch (...) {
    throw;
  };
}
/*! \brief Trim impulse response function for efficiency.

This helper trims a fir filter signal to reduce the computational cost of
time domain subtraction of the expected output signal in the generalized
iterative method.   It first computes an envlope function.   It uses a cruder
algorithm than the more conventional hilbert-transform based envelope using
smoothing of the absolute values of the fir filter amplitudes.  This was
done because the hilbert envelope is a complicated calculation and I (glp)
didn't want debug the required combination of a hilbert transform code
and the secondary problem of using that to compute an envelope function.
May want to retrofit that eventually, but for the initial version I am
assuming the smoothing method will work fine on deconvolution impulse
response functions because the are mostly a near spike with ringing with a
period near that of twice the sample interval.   Hence a simple smoother
a few samples wide should create a pretty effective envelope estimate.

\param d - fir filter to be trimmed
\param floor - length will be determined from sample here the envelope
 amplitude is peak amplitude times this value. (1/floor is kind of an
 snf floor).

\return  a copy of d shortened on both ends.
 */
CoreTimeSeries trim(const CoreTimeSeries &d, double floor = 0.005) {
  try {
    vector<double> work;
    /* First till work with absolute values of d.s from t=0 to end */
    int i, ii, k, kk;
    int i0 = d.sample_number(0.0);
    for (i = i0; i < d.npts(); ++i)
      work.push_back(fabs(d.s[i]));
    /* Establish a smoother width from first zero crossing or small
     * absolute amplitude.*/
    double peakamp = work[0];
    for (i = i0; i < d.npts(); ++i) {
      if (d.s[i] < 0.0)
        break;
      if ((fabs(d.s[i]) / peakamp) < 0.001)
        break;
    }
    /* This should never happen, but is an escape valve. */
    if (i == (d.npts() - 1))
      return d;
    ii = i - i0;
    const int minimum_smoother_width(5);
    int smoother_width = ii - 1;
    if (smoother_width < minimum_smoother_width)
      smoother_width = minimum_smoother_width;
    /* Make sure smoother_width is odd */
    if (smoother_width % 2 == 0)
      ++smoother_width;
    /* This assumes work[0] is the peak amplitude */
    double ampfloor = work[0] * floor;
    /* We compute a crude envelope with smoothed fabs amplitudes.   Start at
    point smoother_width/2.   */
    int soffset = smoother_width / 2;
    int half_width(0);
    double avg;
    for (i = soffset, ii = soffset; ii < work.size(); ++i, ++ii) {
      for (k = 0, kk = i - soffset, avg = 0.0; k < smoother_width; ++k, ++kk)
        avg += work[kk];
      avg /= static_cast<double>(smoother_width); // mean calculation
      if (avg < ampfloor) {
        half_width = ii;
        break;
      }
    }
    if (half_width == 0) {
      /* Consider deleting this message if we confirm the assumption about
      this algorithm stated above is valid - smoother algorithm works because
      the oscillations in are high frequency */
      cerr << "GeneralIterDecon::trim method (WARNING):  "
           << "trim algorithm failed. " << endl
           << "Minimum amplitude at ends of impulse response function=" << avg
           << endl
           << "Trim floor argument specified=" << floor << endl;
      return d; // In this situation just return the original
    }
    double winsize = (static_cast<double>(half_width)) * d.dt();
    TimeWindow cutwin(-winsize, winsize);
    return (WindowData(d, cutwin));
  } catch (...) {
    throw;
  };
}
void GeneralIterDecon::process() {
  const string base_error("GeneralIterDecon::process method:  ");
  try {
    /* We first have to run the signal processing style deconvolution.
    This is defined by the base pointer available through the symbol
    preprocessor.   All those altorithms require load methods to be called
    to initate the computation.  A complication is that the multitaper is
    different and requires a noise signal to also be loaded through loadnoise.
    That complicates this a bit below, but the flow of the algorithm should
    still be clear.   Outer loop is over the three components were we assemble
    a full 3c record.   Note this is the same algorithm use in trace_decon
    for anything but this iterative algorithm.
    */
    /* d_decon will hold the preprocessor output.  We normally expect to
    derive it by windowing of t_all.  We assume WindowData will be
    successful - constructor should guarantee that. */
    d_decon = WindowData(d_all, fftwin);
    dmatrix uwork(d_decon.u);
    uwork.zero();
    /* We assume loadnoise has been called previously to set put the
    right data here. We need a scalar function to pass to the multtitaper
    algorithm though. */
    if (decon_type == MULTI_TAPER) {
      CoreTimeSeries nts(ExtractComponent(n, noise_component));
      dynamic_cast<MultiTaperXcorDecon *>(preprocessor)->loadnoise(nts.s);
    }
    /* For this case of receiver function deconvolution we always get the
    wavelet from component 2 - assumed here to be Z or L. */
    CoreTimeSeries srcwavelet(ExtractComponent(d_decon, 2));
    for (int k = 0; k < 3; ++k) {
      CoreTimeSeries dcomp(ExtractComponent(d_decon, k));
      /* Need the qualifier or we get the wrong overloaded
       * load method */
      preprocessor->ScalarDecon::load(srcwavelet.s, dcomp.s);
      preprocessor->process();
      vector<double> deconout(preprocessor->getresult());
      int copysize = deconout.size();
      if (copysize > d_decon.npts())
        copysize = d_decon.npts();
      cblas_dcopy(copysize, &(deconout[0]), 1, uwork.get_address(k, 0), 3);
    }
    d_decon.u = uwork;
    /* The inverse wavelet and the actual output signals are determined in all
    current algorithms from srcwavelet.   Hence, what is now stored will work.
    If this is extended make sure that condition is satisfied.

    We extract the inverse filter and use a time domain convolution with
    data in the longer time window.   Note for efficiency may want to
    convert this to a frequency domain convolution if it proves to be
    a bottleneck */
    // double dt;
    // dt=this->shapingwavelet.sample_interval();
    // TimeSeries winv=this->preprocessor->inverse_wavelet(tshift,d_decon.t0());
    // DEBUG
    // cerr << "inverse wavelet tshift="<<tshift/5.0<<"
    // d_decon.to="<<d_decon.t0()<<endl; TimeSeries
    // winv=this->preprocessor->inverse_wavelet(tshift/5.0,d_decon.t0());
    // TimeSeries winv=this->preprocessor->inverse_wavelet(0.0,d_decon.t0());
    CoreTimeSeries winv = this->preprocessor->inverse_wavelet(d_decon.t0());
    // This is a test - need a more elegant solution if it works.  Remove me
    // when finished with this test
    // if(d_decon.t0!=0) winv.t0 -= d_decon.t0;
    // DEBUG
    /*
    cerr << "Inverse wavelet"<<endl
         << winv
         <<endl;
*/
    /* The actual output signal is used in the iterative
     * recursion of this algorithm.  For efficiency it is important
     * to trim the fir filter.  The call to trim does that.*/
    CoreTimeSeries actual_out(this->preprocessor->actual_output());
    // DEBUG
    /*
    cerr << "Actual output raw"<<endl;
    cerr << actual_out<<endl;
*/
    actual_out = trim(actual_out);
    actual_o_fir = actual_out.s;
    actual_o_0 = actual_out.sample_number(0.0);
    double peak_scale = actual_o_fir[actual_o_0];
    vector<double>::iterator aoptr;
    for (aoptr = actual_o_fir.begin(); aoptr != actual_o_fir.end(); ++aoptr)
      (*aoptr) /= peak_scale;
    /* This is the size of the inverse wavelet convolution transient
    we use it to prevent iterations in transient region of the deconvolved
    data */
    wavelet_pad = winv.s.size();
    if (2 * wavelet_pad > ndwin) {
      stringstream ss;
      ss << base_error << "Inadequate data window size" << endl
         << "trimmed FIR filter size for actual output signal=" << wavelet_pad
         << endl
         << "Data window length=" << ndwin << endl
         << "Window size must be larger than two times FIR filter size" << endl;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    /* These two signals should be trimmed by winv.npts() on both ends to remove
    sections that are pure edge transients.
    REMOVE me when that is done*/

    r = sparse_convolve(winv, d_all);
    /* Replace n by convolution with inverse wavelet to get the levels correct
     */
    n = sparse_convolve(winv, n);
    TimeWindow trimwin;
    trimwin.start = n.t0() + (n.dt()) * ((double)(winv.npts()));
    trimwin.end = n.endtime() - (n.dt()) * ((double)(winv.npts()));
    n = WindowData(n, trimwin);
    // double nfloor;
    // nfloor=compute_resid_linf_floor();
    // DEBUG - for debug always print this.  Should be a verbose option
    // cerr << "Computed noise floor="<<nfloor<<endl;

    /* d_all now contains the deconvolved data.  Now enter the
    generalized iterative method recursion */
    int i, k;
    lag_weights.clear();
    vector<double> amps, wamps; // raw and weighted amplitudes
    amps.reserve(r.npts());
    wamps.reserve(r.npts());
    /* We need these iterators repeatedly in the main loop below */
    vector<double>::iterator amax;
    for (i = 0; i < r.npts(); ++i)
      lag_weights.push_back(1.0);
    // DEBUG - temporarily disabled for testing
    // for(i=0; i<wavelet_pad; ++i) lag_weights[i]=0.0;
    // for(i=0; i<wavelet_pad; ++i) lag_weights[r.npts()-i-1]=0.0;
    /* These are initial values of convergence parameters */
    lw_linf_initial = 1.0;
    lw_l2_initial = 1.0;
    resid_linf_initial = Linf(r.u);
    resid_l2_initial = L2(r.u);
    iter_count = 0;
    // DEBUG - remove after testing
    lw_linf_history.push_back(lw_linf_initial);
    lw_l2_history.push_back(lw_l2_initial);
    resid_l2_history.push_back(resid_l2_initial);
    resid_linf_history.push_back(resid_linf_initial);
    do {
      /* Compute the vector of amplitudes and find the maximum */
      amps = amp3c(r.u);
      wamps.clear();
      for (k = 0; k < amps.size(); ++k)
        wamps.push_back(amps[k] * lag_weights[k]);
      amax = max_element(wamps.begin(), wamps.end());
      /* The generic distance algorithm used here returns an integer
      that would work to access amps[imax] so we can use the same index
      for the column of the data in d.u. */
      int imax = distance(wamps.begin(), amax);
      /* Save the 3c amplitude at this lag to the spike condensed
      respresentation of the output*/
      ThreeCSpike spk(r.u, imax);
      spikes.push_back(spk);
      // DEBUG
      cerr << iter_count << " col=" << spk.col << " " << "t=" << r.time(spk.col)
           << " amps=" << spk.u[0] << ", " << spk.u[1] << ", " << spk.u[2]
           << endl;
      /* This private method defines how the lag_weights vector is changed
      in the vicinity of this spike.  The tacit assumption is the weight is
      made smaller (maybe even zero) at the spike point and a chosen recipe
      for points in the vicinity of that spike */
      this->update_lag_weights(imax);
      /* Subtract the actual output from the data at lag imax.  Note
      We don't test validity of the lag in spk, but depend on dmatrix to throw
      an exception of the range is invalid */
      this->update_residual_matrix(spk);
      ++iter_count;
    } while (this->has_not_converged());
    if (iter_count >= iter_max)
      throw MsPASSError("GeneralIterDecon::process did not converge",
                        ErrorSeverity::Suspect);
  } catch (...) {
    throw;
  };
}

bool GeneralIterDecon::has_not_converged() {
  try {
    double lw_linf_now, lw_l2_now, resid_linf_now, resid_l2_now;
    vector<double>::iterator vptr;
    vptr = max_element(lag_weights.begin(), lag_weights.end());
    lw_linf_now = (*vptr);
    lw_l2_now = cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
    resid_linf_now = Linf(r.u);
    resid_l2_now = L2(r.u);
    /* DEBUG - saving the convergence vector - after testing delete*/
    lw_linf_history.push_back(lw_linf_now);
    lw_l2_history.push_back(lw_l2_now);
    resid_linf_history.push_back(resid_linf_now);
    resid_l2_history.push_back(resid_l2_now);
    // DEBUG
    cerr << "Iteration count=" << iter_count << " lw_linf=" << lw_linf_now
         << "lw_l2=" << lw_l2_now << " resid_linf=" << resid_linf_now;
    if (iter_count > iter_max)
      return false;
    if (lw_linf_now < lw_linf_floor)
      return false;
    if (lw_l2_now < lw_l2_floor)
      return false;
    if (resid_linf_now < resid_linf_floor)
      return false;
    /* We use a standard calculation for residual l2 as fractional rms change */
    double eps;
    eps = (resid_l2_now - resid_l2_prev) / resid_l2_initial;
    // DEBUG
    cerr << "epsilon=" << eps << endl;
    if (eps < resid_l2_tol)
      return false;
    lw_linf_prev = lw_linf_now;
    lw_l2_prev = lw_l2_now;
    resid_linf_prev = resid_linf_now;
    resid_l2_prev = resid_l2_now;
    return true;
  } catch (...) {
    throw;
  };
}
CoreSeismogram GeneralIterDecon::getresult() {
  try {
    string base_error("GeneralIterDecon::getresult:  ");
    CoreSeismogram result(d_all);
    /* We will make the output the size of the processing window for the
    iteration.  May want to alter this to trim the large lag that would not
    be allowed due to wavelet duration anyway, BUT for GID method the
    wavelet should be compact enough that should be a small factor.  Hence
    for now I omit that complexity until proven to be an issue. */
    result = WindowData(result, dwin);
    result.u.zero();
    /* The spike sequences uses the time reference of the data in the
    private copy r.   This is the computed offset in samples to correct
    lags in the spikes list container to be at correct time in result */
    double dt0;
    int delta_col;
    dt0 = result.t0() - r.t0();
    delta_col = round(dt0 / r.dt());
    list<ThreeCSpike>::iterator sptr;
    int k, resultcol;
    for (sptr = spikes.begin(); sptr != spikes.end(); ++sptr) {
      if (((sptr->col) < 0) || ((sptr->col) >= result.npts()))
        throw MsPASSError(
            base_error +
                "Coding error - spike lag is outside output data range",
            ErrorSeverity::Fatal);
      resultcol = (sptr->col) - delta_col;
      for (k = 0; k < 3; ++k)
        result.u(k, resultcol) = sptr->u[k];
    }
    string wtype = this->shapingwavelet.type();
    if (wtype != "none") {
      CoreTimeSeries w(this->shapingwavelet.impulse_response());
      result = sparse_convolve(w, result);
    }
    return result;
  } catch (...) {
    throw;
  };
}
Metadata GeneralIterDecon::QCMetrics() {
  cerr << "QCMetrics method not yet implemented" << endl;
  exit(-1);
}
} // namespace mspass::algorithms::deconvolution
