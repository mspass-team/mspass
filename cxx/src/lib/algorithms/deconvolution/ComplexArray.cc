#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::utility;

ComplexArray::ComplexArray()
{
    nsamp=0;
    /* Note declaration of shared_ptr initializes it with a NULL
       pointer equivalent - no initialization is needed */
}
ComplexArray::ComplexArray(vector<Complex64> &d)
{
    nsamp=d.size();
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i].real();
        data[i].imag=d[i].imag();
    }
}
ComplexArray::ComplexArray(vector<Complex32> &d)
{
    nsamp=d.size();
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i].real();
        data[i].imag=d[i].imag();
    }
}
ComplexArray::ComplexArray(int n, FortranComplex32 *d)
{
    nsamp=n;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i].real;
        data[i].imag=d[i].imag;
    }
}
ComplexArray::ComplexArray(int n, FortranComplex64 *d)
{
    nsamp=n;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i].real;
        data[i].imag=d[i].imag;
    }
}
ComplexArray::ComplexArray(int n, float *d)
{
    nsamp=n;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i];
        data[i].imag=0.0;
    }
}
ComplexArray::ComplexArray(int n, double *d)
{
    nsamp=n;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=d[i];
        data[i].imag=0.0;
    }
}
ComplexArray::ComplexArray(int n)
{
    nsamp=n;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=0.0;
        data[i].imag=0.0;
    }
}

ComplexArray::ComplexArray(vector<double> mag,vector<double> phase)
{
    nsamp=mag.size();
    if(nsamp==phase.size())
    {
        data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
        for(std::size_t i=0; i<nsamp; i++)
        {
            Complex64 temp=polar(mag[i],phase[i]);
            data[i].real=temp.real();
            data[i].imag=temp.imag();
        }
    }
    else
    {
        cout<<"Length of magnitude vector and phase vector doesn't match"<<endl;
        throw MsPASSError("ComplexArray::ComplexArray(vector<double> mag,vector<double> phase): Length of magnitude vector and phase vector do not match",
              ErrorSeverity::Invalid);
    }
}
ComplexArray::ComplexArray(const ComplexArray &parent)
{
    nsamp=parent.nsamp;
    data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real=parent.data[i].real;
        data[i].imag=parent.data[i].imag;
    }
}
ComplexArray& ComplexArray::operator=(const ComplexArray &parent)
{
    if(&parent != this)
    {
        this->nsamp=parent.nsamp;
        this->data = std::shared_ptr<FortranComplex64[]>(new FortranComplex64[nsamp]);
        for(std::size_t i=0; i<nsamp; i++)
        {
            this->data[i].real=parent.data[i].real;
            this->data[i].imag=parent.data[i].imag;
        }
    }
    return *this;
}
ComplexArray::~ComplexArray()
{
    /*Original implementation used a raw pointer for data array.   Changed
      Dec. 2024 to shared_ptr so this destructor now does nothing.*/
    //delete[] data;
}
double *ComplexArray::ptr()
{
    return reinterpret_cast<double*>(&data[0].real);
}
double *ComplexArray::ptr(int sample)
{
    return reinterpret_cast<double*>(&data[sample].real);
}
Complex64 ComplexArray::operator[](int sample)
{
    return *reinterpret_cast<Complex64*>(&data[sample].real);
}
ComplexArray& ComplexArray::operator +=(const ComplexArray& other)
{
    if(nsamp != other.nsamp)
    {
      stringstream sserr;
      sserr << "ComplexArray::operator+=:  Inconsistent array sizes"<<endl
        << "left hand side array size="<<this->nsamp<<endl
	<< "right hand side array size="<<other.nsamp<<endl
	<< "Sizes must match to use this operator"<<endl;
      throw MsPASSError(sserr.str(),ErrorSeverity::Invalid);
    }
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real+=other.data[i].real;
        data[i].imag+=other.data[i].imag;
    }
    return *this;
}
ComplexArray& ComplexArray::operator -=(const ComplexArray& other)
{
    if(nsamp != other.nsamp)
    {
      stringstream sserr;
      sserr << "ComplexArray::operator-=:  Inconsistent array sizes"<<endl
        << "left hand side array size="<<this->nsamp<<endl
	<< "right hand side array size="<<other.nsamp<<endl
	<< "Sizes must match to use this operator"<<endl;
      throw MsPASSError(sserr.str(),ErrorSeverity::Invalid);
    }
    for(std::size_t i=0; i<nsamp; i++)
    {
        data[i].real-=other.data[i].real;
        data[i].imag-=other.data[i].imag;
    }
    return *this;
}
ComplexArray& ComplexArray::operator *= (const ComplexArray& other)
{
  if(nsamp != other.nsamp)
  {
      stringstream sserr;
      sserr << "ComplexArray::operator*=:  Inconsistent array sizes"<<endl
        << "left hand side array size="<<this->nsamp<<endl
	<< "right hand side array size="<<other.nsamp<<endl
	<< "Sizes must match to use this operator"<<endl;
      throw MsPASSError(sserr.str(),ErrorSeverity::Invalid);
  }
  for(std::size_t i=0; i<nsamp; i++)
  {
    Complex64 z1(data[i].real,data[i].imag);
    Complex64 z2(other.data[i].real,other.data[i].imag);
    Complex64 z3(z1*z2);
    data[i].real=z3.real();
    data[i].imag=z3.imag();
  }
  return(*this);
}
ComplexArray& ComplexArray::operator /= (const ComplexArray& other)
{
  if(nsamp != other.nsamp)
  {
      stringstream sserr;
      sserr << "ComplexArray::operator/=:  Inconsistent array sizes"<<endl
        << "left hand side array size="<<this->nsamp<<endl
	<< "right hand side array size="<<other.nsamp<<endl
	<< "Sizes must match to use this operator"<<endl;
      throw MsPASSError(sserr.str(),ErrorSeverity::Invalid);
  }
  for(std::size_t i=0; i<nsamp; i++)
  {
    Complex64 z1(data[i].real,data[i].imag);
    Complex64 z2(other.data[i].real,other.data[i].imag);
    Complex64 z3(z1/z2);
    data[i].real=z3.real();
    data[i].imag=z3.imag();
  }
  /* For efficiency we scan this array for nans rather than
  put that in the loop above - standard advice for efficiency in
  vector operators on modern computers */
  for(std::size_t i=0;i<nsamp;++i)
  {
    if(gsl_isnan(data[i].real) || gsl_isnan(data[i].imag) )
    {
      stringstream ss;
      ss << "ComplexArray::operator /=:  Division yielded a NaN "
        << "at sample number "<<i<<endl
        << "Denominator used for right hand side="<<other.data[i].real
	<< " + "<<other.data[i].imag<<" i "<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Suspect);
    }
  }
  return(*this);
}
/* We implement the binary operators using op= following Myers book on
 * C++ programming */
const ComplexArray ComplexArray::operator+(const ComplexArray& other)const noexcept(false)
{
  try{
    ComplexArray result(*this);
    result += other;
    return result;
  }catch(...){throw;};
}
const ComplexArray ComplexArray::operator-(const ComplexArray& other)const noexcept(false)
{
  try{
    ComplexArray result(*this);
    result -= other;
    return result;
  }catch(...){throw;};
}
const ComplexArray ComplexArray::operator*(const ComplexArray& other)const noexcept(false)
{
  try{
    ComplexArray result(*this);
    result *= other;
    return result;
  }catch(...){throw;};
}
const ComplexArray ComplexArray::operator/(const ComplexArray& other)const noexcept(false)
{
  try{
    ComplexArray result(*this);
    result /= other;
    return result;
  }catch(...){throw;};
}
void ComplexArray::conj()
{
    for(std::size_t i=0; i<nsamp; i++)
        data[i].imag=-data[i].imag;
}
vector<double> ComplexArray::abs() const
{
    vector<double> result;
    result.reserve(nsamp);
    for(std::size_t i=0; i<nsamp; i++)
        result.push_back(sqrt((double)data[i].real*data[i].real+data[i].imag*data[i].imag));
    return result;
}
double ComplexArray::rms() const
{
    double result=0;
    for(std::size_t i=0; i<nsamp; i++)
        result+=((double)data[i].real*data[i].real+data[i].imag*data[i].imag)/nsamp/nsamp;
    return sqrt(result);
}
double ComplexArray::norm2() const
{
    double result=0;
    for(std::size_t i=0; i<nsamp; i++)
        result+=((double)data[i].real*data[i].real+data[i].imag*data[i].imag);
    return sqrt(result);
}
vector<double> ComplexArray::phase() const
{
    vector<double> result;
    result.reserve(nsamp);
    for(std::size_t i=0; i<nsamp; i++)
        result.push_back(atan2((double)data[i].imag,(double)data[i].real));
    return result;
}
int ComplexArray::size() const
{
    return nsamp;
}
} //End namespace
