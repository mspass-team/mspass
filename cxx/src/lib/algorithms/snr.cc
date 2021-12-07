#include <vector>
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/utility/VectorStatistics.h"
#include "mspass/utility/Metadata.h"
namespace mspass::algorithms::amplitudes{
using mspass::seismic::PowerSpectrum;
using mspass::utility::Metadata;
using mspass::utility::VectorStatistics;
/* This function once was a part of the CNR3CDecon.cc file, but it was moved
here as it was found to have a more generic purpose - bandwidth estimation.
It is used in snr python wrapper functions of mspass and in CNR3cDecon to
estimate bandwidth from power spectrum estimates.   */
BandwidthData EstimateBandwidth(const double signal_df,
  const PowerSpectrum& s, const PowerSpectrum& n,
    const double snr_threshold, const double tbp)
{
  /* Set the starting search points at low (based on noise tbp) and high (80% fny)
  sides */
  double flow_start, fhigh_start;
  flow_start=(n.df)*tbp;
  fhigh_start=s.Nyquist()*0.8;
  double df_test_range=2.0*tbp*(n.df);
  int s_range=s.nf();
  BandwidthData result;
  /* First search from flow_start in increments of signal_df to find low
  edge.*/
  double f, sigamp, namp, snrnow;
  double f_mark;
  int istart=s.sample_number(flow_start);
  bool searching(false);
  int i;
  for(i=istart;i<s_range;++i)
  {
    f=s.frequency(i);
    /* We use amplitude snr not power snr*/
    sigamp=sqrt(s.spectrum[i]);
    namp=n.amplitude(f);
    snrnow=sigamp/namp;
    if(snrnow>snr_threshold)
    {
      if(searching)
      {
        if((f-f_mark)>=df_test_range)
        {
          result.low_edge_f=f_mark;
          break;
        }
      }
      else
      {
        f_mark=f;
        result.low_edge_snr=snrnow;
        searching=true;
      }
    }
    else
    {
      if(searching)
      {
        searching=false;
        f_mark=f;
      }
    }
  }
  /* Return the zeroed result object if no data exceeded the snr threshold.*/
  if(i>=(s_range-1))
  {
    result.low_edge_f=0.0;
    result.high_edge_f=0.0;
    result.low_edge_snr=0.0;
    result.high_edge_snr=0.0;
    /* This is the most important one to set 0.0*/
    result.f_range=0.0;
    return result;
  }
  /* Now search from the high end to find upper band edge - same algorithm
  reversed direction. */
  searching=false;
  istart=s.sample_number(fhigh_start);
  for(i=istart;i>=0;--i)
  {
    f=s.frequency(i);
    sigamp=sqrt(s.spectrum[i]);
    namp=n.amplitude(f);
    snrnow=sigamp/namp;
    if(snrnow>snr_threshold)
    {
      if(searching)
      {
        if((f_mark-f)>=df_test_range)
        {
          result.high_edge_f=f_mark;
          break;
        }
      }
      else
      {
        f_mark=f;
        result.high_edge_snr=snrnow;
        searching=true;
      }
    }
    else
    {
      if(searching)
      {
        searching=false;
        f_mark=f;
      }
    }
  }
  result.f_range=result.high_edge_f-result.low_edge_f;
  return result;
}
Metadata BandwidthStatistics(const PowerSpectrum& s, const PowerSpectrum& n,
                               const BandwidthData& bwd)
{
  std::vector<double> bandsnr;
  double f;
  for(f=bwd.low_edge_f;f<bwd.high_edge_f && f<s.Nyquist();f+=s.df)
  {
    double signal_amp,noise_amp,snr;
    signal_amp = s.amplitude(f);
    noise_amp = n.amplitude(f);
    if(signal_amp<=0.0)
    {
      snr=0.0;
    }
    else if(noise_amp<=0)
    {
      /*Set to a large number in this condition because we can't get past
      the previous if for the (worse) case of 0/0.   We only get here if
      signal_amp is nonzero but noise is zero.*/
      snr = 999999.0;
    }
    else
    {
      snr = signal_amp/noise_amp;
    }
    bandsnr.push_back(snr);
  }
  VectorStatistics<double> stats(bandsnr);
  Metadata result;
  /* stats contains multiple methods that return other metrics but we only
  return typical box plot values */
  result.put_double("median_snr",stats.median());
  result.put_double("maximum_snr",stats.upper_bound());
  result.put_double("minimum_snr",stats.lower_bound());
  result.put_double("q1_4_snr",stats.q1_4());
  result.put_double("q3_4_snr",stats.q3_4());
  result.put_double("mean_snr",stats.mean());
  return result;
}
} // end namespace
