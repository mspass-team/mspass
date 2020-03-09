#include <sstream>
#include "mspass/deconvolution/PowerSpectrum.h"
using namespace std;
using namespace mspass;
namespace mspass{
PowerSpectrum::PowerSpectrum(): Metadata(), elog()
{
  df=1.0;
  f0=0.0;
  spectrum_type=string("UNDEFINED");
}
PowerSpectrum::PowerSpectrum(const PowerSpectrum& parent)
  : Metadata(parent),elog(parent.elog)
{
  df=parent.df;
  f0=parent.f0;
  spectrum_type=parent.spectrum_type;
  spectrum_type=parent.spectrum_type;
  spectrum=parent.spectrum;
  elog=parent.elog;
}
PowerSpectrum& PowerSpectrum::operator=(const PowerSpectrum& parent)
{
  if(this!=(&parent))
  {
    this->Metadata::operator=(parent);
    df=parent.df;
    f0=parent.f0;
    spectrum_type=parent.spectrum_type;
    spectrum=parent.spectrum;
    elog=parent.elog;
  }
  return *this;
}
PowerSpectrum& PowerSpectrum::operator+=(const PowerSpectrum& other)
{
  /* Allow self accumulation - double values */
  if(this==(&other))
  {
    for(int k=0;k<spectrum.size();++k) spectrum[k]*=2.0;
  }
  else
  {
    if(this->spectrum.size() != other.spectrum.size())
    {
      stringstream ss;
      ss << "operator+=(accumulation) size mismatch of spectrum arrays"<<endl
        << "right hade side spectrum size="<<spectrum.size()<<endl
        << "left had side spectrum size="<<other.spectrum.size()<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    for(int k=0;k<spectrum.size();++k)
      spectrum[k] += other.spectrum[k];
  }
  return *this;
}
vector<double> PowerSpectrum::amplitude()
{
  vector<double> result;
  result.reserve(spectrum.size());
  for(int k=0;k<spectrum.size();++k)
    result.push_back(sqrt(spectrum[k]));
  return result;
}
}  // End naespace
