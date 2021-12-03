#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/algorithms/amplitudes.h"
namespace mspass::algorithms::amplitudes{
using mspass::seismic::PowerSpectrum;
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
} // end namespace
