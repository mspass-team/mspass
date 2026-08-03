#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/algorithms/deconvolution/GSLFFTResources.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/algorithms/deconvolution/dpss.h"
#include "mspass/utility/utility.h"
#include <limits>
#include <new>
#include <sstream>
#include <vector>
/* This C function is defined in FFTDeconOperator.h but it has a lot of
other baggage that could create mysterious problems so we just define it
again here.  Maintenanc issue if the api changes.*/
extern "C" {
unsigned int nextPowerOf2(unsigned int n);
}
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

MTPowerSpectrumEngine::MTPowerSpectrumEngine() {
  taperlen = 0;
  ntapers = 0;
  nfft = 0;
  tbp = 0.0;
  deltaf = 1.0;
  operator_dt = 1.0;
  wavetable = NULL;
  workspace = NULL;
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(const int winsize,
                                             const double tbpin,
                                             const int ntpin, const int nfftin,
                                             const double dtin)
    : taperlen(winsize), ntapers(ntpin), nfft(0), tbp(tbpin),
      operator_dt(dtin), deltaf(1.0), wavetable(NULL), workspace(NULL) {
  const string caller("MTPowerSpectrumEngine constructor");
  if (winsize <= 0)
    throw MsPASSError(caller + ": winsize must be positive",
                      ErrorSeverity::Fatal);
  if (!isfinite(tbpin) || tbpin <= 0.0)
    throw MsPASSError(
        caller + ": time-bandwidth product must be finite and positive",
        ErrorSeverity::Fatal);
  if (ntpin <= 0)
    throw MsPASSError(caller + ": number of tapers must be positive",
                      ErrorSeverity::Fatal);
  if (!isfinite(dtin) || dtin <= 0.0)
    throw MsPASSError(caller + ": sample interval must be finite and positive",
                      ErrorSeverity::Fatal);
  if (tbpin > static_cast<double>(numeric_limits<int>::max()) / 2.0)
    throw MsPASSError(caller + ": time-bandwidth product is too large",
                      ErrorSeverity::Fatal);
  if (nfftin < winsize) {
    unsigned int rounded_nfft = nextPowerOf2(winsize);
    if (rounded_nfft == static_cast<unsigned int>(winsize)) {
      if (rounded_nfft >
          static_cast<unsigned int>(numeric_limits<int>::max() / 2))
        throw MsPASSError(caller + ": computed fft length is too large",
                          ErrorSeverity::Fatal);
      rounded_nfft *= 2;
    }
    if (rounded_nfft == 0 || rounded_nfft >
                                 static_cast<unsigned int>(
                                     numeric_limits<int>::max()))
      throw MsPASSError(caller + ": computed fft length is invalid",
                        ErrorSeverity::Fatal);
    nfft = static_cast<int>(rounded_nfft);
  } else
    nfft = nfftin;
  if (nfft <= 0)
    throw MsPASSError(caller + ": fft length must be positive",
                      ErrorSeverity::Fatal);
  this->set_df(dtin);
  int nseq = static_cast<int>(2.0 * tbp);
  if (nseq <= 0)
    throw MsPASSError(
        caller + ": time-bandwidth product permits no positive taper count",
        ErrorSeverity::Fatal);
  if (ntapers > nseq) {
    cerr << "MTPowerSpectrumEngine (WARNING):  requested number of tapers="
         << ntpin << endl
         << "is inconsistent with requested time time bandwidth product ="
         << tbp << endl
         << "Automatically reset number tapers to max allowed=" << nseq << endl;
    ntapers = nseq;
  }
  if (ntapers <= 0)
    throw MsPASSError(caller + ": clamped taper count is not positive",
                      ErrorSeverity::Fatal);
  int seql(0);
  int sequ = ntapers - 1;
  std::vector<double> work(static_cast<size_t>(ntapers) *
                               static_cast<size_t>(taperlen),
                           0.0);
  dpss_calc(taperlen, tbp, seql, sequ, &(work[0]));
  tapers = dmatrix(ntapers, taperlen);
  int i, ii, j;
  for (i = 0, ii = 0; i < ntapers; ++i) {
    for (j = 0; j < taperlen; ++j) {
      tapers(i, j) = work[ii];
      ++ii;
    }
  }
  /* To be consistent with Prieto we use this algorithm to convert to
  what he calls the "positive standard".   That means we assure the
  center point is positive.
  */
  for (i = 0; i < ntapers; ++i) {
    int lh; // matches Prieto algorithm name - see multitaper module
    if (taperlen % 2)
      lh = static_cast<int>((taperlen + 1) / 2);
    else
      lh = static_cast<int>(taperlen / 2);
    if (tapers(i, lh) < 0.0) {
      for (j = 0; j < taperlen; ++j)
        tapers(i, j) = -tapers(i, j);
    }
  }
  auto resources = detail::AllocateGSLFFTResources(nfft, caller);
  wavetable = resources.wavetable.release();
  workspace = resources.workspace.release();
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(
    const MTPowerSpectrumEngine &parent)
    : taperlen(parent.taperlen), ntapers(parent.ntapers), nfft(parent.nfft),
      tbp(parent.tbp), operator_dt(parent.operator_dt), tapers(parent.tapers),
      deltaf(parent.deltaf), wavetable(NULL), workspace(NULL) {
  /* A default engine has no FFT resources.  For a configured engine the raw
   * GSL pointers must remain null until each allocation succeeds because this
   * object's destructor is not run if its constructor throws. */
  if (nfft > 0) {
    auto resources = detail::AllocateGSLFFTResources(
        nfft, "MTPowerSpectrumEngine copy constructor");
    wavetable = resources.wavetable.release();
    workspace = resources.workspace.release();
  }
}

MTPowerSpectrumEngine::~MTPowerSpectrumEngine() {
  if (wavetable != NULL)
    gsl_fft_complex_wavetable_free(wavetable);
  if (workspace != NULL)
    gsl_fft_complex_workspace_free(workspace);
}
MTPowerSpectrumEngine &
MTPowerSpectrumEngine::operator=(const MTPowerSpectrumEngine &parent) {
  if (&parent != this) {
    /* Copy all allocating state before committing.  In particular, never
     * overwrite the cached GSL pointers: doing so leaked both allocations on
     * every CNRDeconEngine assignment. */
    dmatrix new_tapers(parent.tapers);
    detail::GSLFFTResources resources;
    if (parent.nfft > 0) {
      resources = detail::AllocateGSLFFTResources(
          parent.nfft, "MTPowerSpectrumEngine assignment");
    }
    try {
      tapers = new_tapers;
    } catch (...) {
      throw;
    }
    if (wavetable != NULL)
      gsl_fft_complex_wavetable_free(wavetable);
    if (workspace != NULL)
      gsl_fft_complex_workspace_free(workspace);
    taperlen = parent.taperlen;
    ntapers = parent.ntapers;
    nfft = parent.nfft;
    tbp = parent.tbp;
    operator_dt = parent.operator_dt;
    deltaf = parent.deltaf;
    wavetable = resources.wavetable.release();
    workspace = resources.workspace.release();
  }
  return *this;
}
PowerSpectrum MTPowerSpectrumEngine::apply(const TimeSeries &d) {
  try {
    int k;
    /* Used to test for operator sample interval against data sample interval.
    We don't use a epsilon comparison as slippery clock data sometime shave
    sample rates small percentage difference from nominal.*/
    const double DT_FRACTION_TOLERANCE(0.001);
    const string algorithm("MTPowerSpectrumEngine");
    /* We need to define this here to allow posting problems to elog.*/
    PowerSpectrum result;
    int dsize = d.npts();
    vector<double> work;
    work.reserve(this->nfft);
    double dtfrac = fabs(d.dt() - this->operator_dt) / this->operator_dt;
    if (dtfrac > DT_FRACTION_TOLERANCE) {
      stringstream ss;
      ss << "Date sample interval=" << d.dt()
         << " does not match operator sample interval=" << this->operator_dt
         << endl
         << "Cannot proceed.  Returning a null result";
      result.elog.log_error("MTPowerSpectrumEngine::apply", ss.str(),
                            ErrorSeverity::Invalid);
      result.kill();
      return result;
    }

    if (dsize < taperlen) {
      stringstream ss;
      ss << "Received data window of length=" << d.npts() << " samples" << endl
         << "Operator length=" << taperlen << endl
         << "Results may be unreliable" << endl;
      result.elog.log_error(algorithm, string(ss.str()),
                            ErrorSeverity::Suspect);
      for (k = 0; k < taperlen; ++k)
        work.push_back(0.0);
      for (k = 0; k < dsize; ++k)
        work[k] = d.s[k];
    } else {
      if (dsize > taperlen) {
        stringstream ss;
        ss << "Received data window of length=" << d.npts() << " samples"
           << endl
           << "Operator length=" << taperlen << endl
           << "Results may be unreliable because data will be truncated to "
              "taper length"
           << endl;
        result.elog.log_error(algorithm, ss.str(), ErrorSeverity::Suspect);
      }
      for (k = 0; k < taperlen; ++k)
        work.push_back(d.s[k]);
    }
    /* intentionally omit try catch here because the above logic assures Sizes
    must match here. This overloaded method will throw an exception in that
    case. Note in this implementation the result returned by apply is scaled to
    assumed properly scaled to power spectrum and normalized for multitapers.*/
    vector<double> spec(this->apply(work));

    result = PowerSpectrum(dynamic_cast<const Metadata &>(d), spec, deltaf,
                           string("Multitaper"), 0.0, d.dt(), d.npts());
    /* We post these to metadata for the generic PowerSpectrum object. */
    result.put<double>("time_bandwidth_product", tbp);
    result.put<long>("number_tapers", ntapers);
    return result;
  } catch (...) {
    throw;
  };
}
std::vector<double>
MTPowerSpectrumEngine::apply(const std::vector<double> &d) {
  /* This function must be dogmatic about d size = taperlen*/
  if (d.size() != this->taperlen) {
    stringstream ss;
    ss << "MTPowerSpectrumEngine::apply method:  input data vector length of "
       << d.size() << endl
       << "does not match operator taper length length=" << this->taperlen
       << endl
       << "Sizes must match to use this implementation of this algorithm"
       << endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
  /* Need this for parseval theorem scaling */
  double ssq(0.0);
  for (auto ptr = d.begin(); ptr != d.end(); ++ptr)
    ssq += (*ptr) * (*ptr);
  /* This is the only function in this entire object that does anything
  but housework.   Computes the power spectrum by average DFT of d^*d where
  the average is over the tapes. First taper data and store tapered data in
  tdata container*/
  int i, j;
  vector<ComplexArray> tdata;
  tdata.reserve(ntapers);
  vector<double> work;
  work.reserve(nfft);
  for (i = 0; i < ntapers; ++i) {
    work.clear();
    /* This will assure part of vector between end of
     * data and nfft is zero padded */
    for (j = 0; j < taperlen; ++j)
      work.push_back(tapers(i, j) * d[j]);
    for (j = taperlen; j < nfft; ++j)
      work.push_back(0.0);
    ComplexArray cwork(nfft, &(work[0]));
    tdata.push_back(cwork);
  }
  /* Now apply DFT to each of tapered arrays */
  for (i = 0; i < ntapers; ++i) {
    gsl_fft_complex_forward(tdata[i].ptr(), 1, nfft, wavetable, workspace);
  }
  /* New version - delete this comment if it works*/
  vector<double> result;
  result.reserve(this->nf());
  for (j = 0; j < this->nf(); ++j)
    result.push_back(0.0);
  for (i = 0; i < ntapers; ++i) {
    for (j = 0; j < this->nf(); ++j) {
      mspass::algorithms::deconvolution::Complex64 z;
      double rp, ip;
      z = tdata[i][j];
      rp = z.real();
      ip = z.imag();
      result[j] += rp * rp + ip * ip;
    }
  }
  /* Scale using Parseval's theorem - this is adapted from Prieto's
  multitaper python implementation.   We have to explicitly add the
  divide by nfft that is implicit in Prieto's code because he uses
  numpy's var function to compoute sum of squares of data that includes a
  divide by data vector length.  Not sure his formula is right as seems to
  me it shouild be nfft not npts.
  */
  double specssq(0.0), scale;
  for (auto p = result.begin(); p != result.end(); ++p)
    specssq += (*p);
  /* test for zeros to avoid divide by zero or a NaN. 
   * result will be all zeros this way.  Without we get all NaNs
   * in the spectrum*/
  if((ssq<=0.0) || (specssq<=0.0))
  {
    scale = 1.0;
  }
  else
  {
    scale = ssq / (specssq * this->df());
    /* Scaling for fft implementation - Established from zero pad tests it has
    to be this factor.   */
    scale /= static_cast<double>(d.size());
  }
  for (j = 0; j < this->nf(); ++j)
    result[j] *= scale;
  return result;
}
vector<double> MTPowerSpectrumEngine::frequencies() {
  vector<double> f;
  /* If taperlen is odd this still works according to gsl documentation.*/
  for (int i = 0; i < this->nf(); ++i) {
    /* Here we assume i=0 frequency is 0 */
    f.push_back(deltaf * ((double)i));
  }
  return f;
}
} // namespace mspass::algorithms::deconvolution
