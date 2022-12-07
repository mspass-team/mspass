#include <vector>
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/utility/VectorStatistics.h"
#include "mspass/utility/Metadata.h"
namespace mspass::algorithms::amplitudes{
using mspass::seismic::PowerSpectrum;
using mspass::utility::Metadata;
using mspass::utility::VectorStatistics;
/* These are used to flag 0/0 and x/0 respectively */
const double INDETERMINATE(-1.0),NOISE_FREE(9999999.9);
/* This function once was a part of the CNR3CDecon.cc file, but it was moved
here as it was found to have a more generic purpose - bandwidth estimation.
It is used in snr python wrapper functions of mspass and in CNR3cDecon to
estimate bandwidth from power spectrum estimates.   */

BandwidthData EstimateBandwidth(const double signal_df,
  const PowerSpectrum& s, const PowerSpectrum& n,
    const double snr_threshold, const double tbp,const double fhs,
     const bool fix_high_edge_to_fhs)
{
  /* This number defines a scaling to correct for difference in window length
  for signal and noise windows.   It assumes the noise process is stationary
  which pretty is pretty much essential for this entire algorithm to make
  sense anyway. */
  double window_length_correction=static_cast<double>(s.nf())/static_cast<double>(n.nf());
  /* Set the starting search points at low (based on noise tbp) and high (80% fny)
  sides */
  double flow_start, fhigh_start;
  flow_start=(n.df)*tbp;
  /* Silently set to 80% of Nyquist if the passed value is negative or
  above nyquist - illegal values.  Assume python wrappers can post warning
  if deemeed essential.   This is a very harmless error so being silent is
  probably normally ok */
  if( (fhs<=0.0) || (fhs>s.Nyquist()) )
    fhigh_start=s.Nyquist()*0.8;
  else
    fhigh_start = fhs;
  double df_test_range=2.0*tbp*(n.df);
  int s_range = s.sample_number(fhigh_start);
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
    if(namp>0.0)
      snrnow=window_length_correction*sigamp/namp;
    else
    {
      if(sigamp>0.0)
        snrnow=NOISE_FREE;
      else
        snrnow = INDETERMINATE;
    }
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
  reversed direction.  Note option to disable  */
  if(fix_high_edge_to_fhs)
  {
    result.high_edge_f = fhigh_start;
    sigamp=s.amplitude(fhigh_start);
    namp=n.amplitude(fhigh_start);
    if(namp>0.0)
      snrnow=window_length_correction*sigamp/namp;
    else
    {
      if(sigamp>0.0)
        snrnow=NOISE_FREE;
      else
        snrnow = INDETERMINATE;
    }
    result.high_edge_snr=snrnow;
  }
  else
  {
    searching=false;
    istart=s.sample_number(fhigh_start);
    for(i=istart;i>=0;--i)
    {
      f=s.frequency(i);
      sigamp=sqrt(s.spectrum[i]);
      namp=n.amplitude(f);
      if(namp>0.0)
        snrnow=window_length_correction*sigamp/namp;
      else
      {
        if(sigamp>0.0)
          snrnow=NOISE_FREE;
        else
          snrnow = INDETERMINATE;
      }
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
  }
  result.f_range = s.Nyquist() - s.f0;
  return result;
}
Metadata BandwidthStatistics(const PowerSpectrum& s, const PowerSpectrum& n,
                               const BandwidthData& bwd)
{
  /* As noted above this correction is needed for an irregular window size*/
  double window_length_correction=static_cast<double>(s.nf())/static_cast<double>(n.nf());
  Metadata result;
  /* the algorithm below will fail if either of these conditions is true so
  we trap that and return a null result.   Caller must handle the null
  return correctly*/
  if( ( bwd.f_range <= 0.0 ) || ( (bwd.high_edge_f-bwd.low_edge_f)<s.df) )
  {
    result.put_double("median_snr",0.0);
    result.put_double("maximum_snr",0.0);
    result.put_double("minimum_snr",0.0);
    result.put_double("q1_4_snr",0.0);
    result.put_double("q3_4_snr",0.0);
    result.put_double("mean_snr",0.0);
    result.put_bool("stats_are_valid",false);
    return result;
  }
  std::vector<double> bandsnr;
  double f;
  for(f=bwd.low_edge_f;f<bwd.high_edge_f && f<s.Nyquist();f+=s.df)
  {
    double signal_amp,noise_amp,snr;
    signal_amp = s.amplitude(f);
    noise_amp = n.amplitude(f);
    if(noise_amp <= 0.0)
    {
      if(signal_amp>0.0)
        snr = NOISE_FREE;
      else
        snr = INDETERMINATE;
    }
    else
    {
      snr = window_length_correction*signal_amp/noise_amp;
    }
    bandsnr.push_back(snr);
  }
  VectorStatistics<double> stats(bandsnr);
  /* stats contains multiple methods that return other metrics but we only
  return typical box plot values */
  result.put_double("median_snr",stats.median());
  result.put_double("maximum_snr",stats.upper_bound());
  result.put_double("minimum_snr",stats.lower_bound());
  result.put_double("q1_4_snr",stats.q1_4());
  result.put_double("q3_4_snr",stats.q3_4());
  result.put_double("mean_snr",stats.mean());
  result.put_bool("stats_are_valid",true);
  return result;
}
} // end namespace
