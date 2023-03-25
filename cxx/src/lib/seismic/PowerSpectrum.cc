#include <sstream>
#include <cmath>
#include "mspass/seismic/BasicSpectrum.h"
#include "mspass/seismic/PowerSpectrum.h"
namespace mspass::seismic
{
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;

PowerSpectrum::PowerSpectrum(): BasicSpectrum(),Metadata(), elog()
{
  spectrum_type=string("UNDEFINED");
}
PowerSpectrum::PowerSpectrum(const PowerSpectrum& parent)
  : BasicSpectrum(parent),Metadata(parent),elog(parent.elog)
{
  spectrum_type=parent.spectrum_type;
  spectrum_type=parent.spectrum_type;
  spectrum=parent.spectrum;
  nyquist_frequency=parent.nyquist_frequency;
  elog=parent.elog;
}
PowerSpectrum& PowerSpectrum::operator=(const PowerSpectrum& parent)
{
  if(this!=(&parent))
  {
    this->Metadata::operator=(parent);
    dfval=parent.dfval;
    f0val=parent.f0val;
    spectrum_type=parent.spectrum_type;
    spectrum=parent.spectrum;
    nyquist_frequency=parent.nyquist_frequency;
    elog=parent.elog;
  }
  return *this;
}
PowerSpectrum& PowerSpectrum::operator+=(const PowerSpectrum& other)
{
  /* Allow self accumulation - double values */
  if(this==(&other))
  {
    for(int k=0;k<this->nf();++k) spectrum[k]*=2.0;
  }
  else
  {
    if(this->nf() != other.nf())
    {
      stringstream ss;
      ss << "operator+=(accumulation) size mismatch of spectrum arrays"<<endl
        << "right hade side spectrum size="<<this->nf()<<endl
        << "left had side spectrum size="<<other.nf()<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    for(int k=0;k<this->nf();++k)
      spectrum[k] += other.spectrum[k];
  }
  return *this;
}
vector<double> PowerSpectrum::amplitude() const
{
  vector<double> result;
  result.reserve(spectrum.size());
  for(int k=0;k<spectrum.size();++k)
    result.push_back(sqrt(spectrum[k]));
  return result;
}
double PowerSpectrum::power(const double f) const
{
  if(f<0.0) throw MsPASSError("PowerSpectrum::amplitude:  requested amplitude for a negative frequency which is assumed to be an erorr",
                ErrorSeverity::Invalid);
  int filow;
  filow=static_cast<int>((f-this->f0val)/this->dfval);
  /* Force 0 at Nyquist and above - this allows simple interpolation in else */
  if(filow>=(spectrum.size()-1))
     return 0.0;
  else
  {
    double slope=(spectrum[filow+1]-spectrum[filow])/this->dfval;
    double flow=this->f0val+((double)filow)*this->dfval;
    return spectrum[filow]+slope*(f-flow);
  }
}
std::vector<double> PowerSpectrum::frequencies() const
{
  vector<double> f;
  f.reserve(this->nf());
  for(auto i=0;i<this->nf();++i) f.push_back(this->f0val+this->dfval*static_cast<double>(i));
  return f;
}
}  // End namespace
