#ifndef __COMPLEX_ARRAY_H__
#define __COMPLEX_ARRAY_H__

#include <complex>
#include <vector>
#include <iostream>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>

#define REAL(z,i) ((z)[2*(i)])
#define IMAG(z,i) ((z)[2*(i)+1])
namespace mspass{
typedef std::complex<double> Complex64;
typedef std::complex<float> Complex32;

typedef struct FortranComplex32 {
    float real;
    float imag;
} FortranComplex32;
typedef struct FortranComplex64 {
    double real;
    double imag;
} FortranComplex64;

/* \brief Interfacing object to ease conversion between FORTRAN and C++ complex.

   */
class ComplexArray
{
public:
    /*! Empty constructor. */
    ComplexArray();
    /*! Construct from stl vector container of complex. */
    ComplexArray(std::vector<Complex64> &d);
    /*! Similar for 32 bit version */
    ComplexArray(std::vector<Complex32> &d);
    /*! Construct from a FORTRAN data array.

      Fortran stores complex numbers in a mulitplexed array
      structure (real(1), imag(1), real(2), imag(2), etc.).
      The constructors below provide a mechanism for building
      this object from various permutations of this.
      \param nsamp is the number of elements in the C vector
      \param d is the pointer to the first compoment of the
         fortran vector.
      */
    ComplexArray(int nsamp, FortranComplex32 *d);
    ComplexArray(int nsamp, FortranComplex64 *d);
    ComplexArray(int nsamp, float *d);
    ComplexArray(int nsamp, double *d);
    ComplexArray(int nsamp);
    /*! Construct from different length of vector, adds zoeros to it
    	And construct a constant arrays
    	*/
    template<class T> ComplexArray(int nsamp, std::vector<T> d);
    template<class T> ComplexArray(int nsamp, T d);

    /*! Construct from magnitude and phase arrays.*/
    ComplexArray(std::vector<double> mag,std::vector<double> phase);

    /* These will need to be implemented.  Likely cannot
       depend on the compiler to generate them correctly */
    ComplexArray(const ComplexArray &parent);
    ComplexArray& operator=(const ComplexArray &parent);
    ~ComplexArray();

    /* These are kind of the inverse of the constructor.
    Independent of what the internal representation is they
    will return useful interface representations. */
    /*! Return a pointer to a fortran array containing
      the data vector.

      The array linked to the returned pointer should be
      created with the new operator and the caller should
      be sure to use delete [] to free this memory when
      finished. */
    template<class T> T *FortranData();
    /* This is same for what I think fortran calls
       double complex */
//        double *FortranData();
    /* C representation.  This can be templated easily.
    See below.  The syntax is weird and should probably
    be wrapped with a typedef */
    template<class T> std::vector<std::complex<T> > CPPData();


    /* Operators are the most important elements of this
       thing to make life easier. */
    /*! Index operator.
    	Cannot make it work by getting the address from reference.
    	Have to call the ptr() function to get the address.
      \param sample is the sample number to return.
      \return contents of vector at position sample.
      */
    Complex64 operator[](int sample);
    double *ptr();
    double *ptr(int sample);
    ComplexArray& operator +=(const ComplexArray& other) noexcept(false);
    ComplexArray& operator -=(const ComplexArray& other) noexcept(false);
    /* This actually is like .* in matlab - sample by sample multiply not
    a dot product */
    ComplexArray& operator *= (const ComplexArray& other)noexcept(false);
    /* This is like *= but complex divide element by element */
    ComplexArray& operator /= (const ComplexArray& other)noexcept(false);
    const ComplexArray operator +(const ComplexArray& other)const noexcept(false);
    //template<class T> ComplexArray operator +(const vector<T> &other);
    //template<class T> ComplexArray operator +(const T &other);
    const ComplexArray operator -(const ComplexArray& other)const noexcept(false);
    //template<class T> ComplexArray operator -(const vector<T> &other);
    //template<class T> ComplexArray operator -(const T &other);
    const ComplexArray operator *(const ComplexArray& other)const noexcept(false);
    const ComplexArray operator /(const ComplexArray& other)const noexcept(false);
    /*! product of complex and real vectors */
    //template<class T> ComplexArray operator *(const vector<T> &other);
    //template<class T> friend ComplexArray operator *(const vector<T> &lhs,const ComplexArray &rhs);
    /*! product of complex and a number */
    //template<class T> ComplexArray operator *(const T &other);
    //template<class T> friend ComplexArray operator *(const T &lhs,const ComplexArray &rhs);
    /*! Change vector to complex conjugates. */
    void conj();
    /* Return stl vector of amplitude spectrum.  */
    std::vector<double> abs() const;
    /* Return rms value.  */
    double rms() const;
    /* Return 2-norm value.  */
    double norm2() const;
    /* Return stl vector of phase */
    std::vector<double> phase() const;
    /* Return size of the array*/
    int size() const;
private:
    /* Here is an implementation detail.   There are three ways
       I can think to do this.  First, we could internally store
       data as fortran array of 32 bit floats.   That is probably
       the best because we can use BLAS routines (if you haven't
       heard of this - likely - I need to educate you.)  to do
       most of the numerics fast. Second, we could use stl
       vector container of std::complex.  The third is excessively
       messy but technically feasible - I would not recommend it.
       That is, one could store pointers to either representation
       and internally convert back and forth.  Ugly and dangerous
       I think.

       I suggest we store a FORTRAN 32 bit form since that is
       what standard numeric libraries (e.g. most fft routines)
       use.  */
    /*I decided to use 64 bit, since the GSL's fft routine is using that.*/
    FortranComplex64 *data;
    int nsamp;
};
/* This would normally be in the .h file and since I don't think
   you've used templates worth showing you how it would work. */
template <class T> std::vector<std::complex<T> > ComplexArray::CPPData()
{
    std::vector<std::complex<T> > result;
    result.reserve(nsamp);
    int i;
    for(i=0; i<nsamp; ++i)
    {
        std::complex<T> z(data[i].real, data[i].imag);
        result.push_back(z);
    }
    return result;
}

template<class T> T* ComplexArray::FortranData()
{
    T* result=new T[nsamp];
    for(int i=0; i<nsamp; i++)
        result[i]=data[i];
    return result;
}

template<class T> ComplexArray::ComplexArray(int n, std::vector<T> d)
{
    nsamp=n;
    if(nsamp>d.size())
    {
        data=new FortranComplex64[nsamp];
        for(int i=0; i<d.size(); i++)
        {
            data[i].real=d[i];
            data[i].imag=0.0;
        }
        for(int i=d.size(); i<nsamp; i++)
        {
            data[i].real=0.0;
            data[i].imag=0.0;
        }
    }
    else
    {
        data=new FortranComplex64[nsamp];
        for(int i=0; i<nsamp; i++)
        {
            data[i].real=d[i];
            data[i].imag=0.0;
        }
    }
}
template<class T> ComplexArray::ComplexArray(int n, T d)
{
    nsamp=n;
    data=new FortranComplex64[nsamp];
    for(int i=0; i<nsamp; i++)
    {
        data[i].real=d;
        data[i].imag=0.0;
    }
}
/*
template<class T> ComplexArray ComplexArray::operator +(const vector<T> &other)
{
    ComplexArray result(*this);
    int n;
    if(nsamp>other.size())
        n=other.size();
    else
        n=nsamp;
    for(int i=0; i<n; i++)
    {
        result.data[i].real=data[i].real+other[i];
    }
    return result;
}
template<class T> ComplexArray ComplexArray::operator +(const T &other)
{
    ComplexArray result(*this);
    for(int i=0; i<nsamp; i++)
    {
        result.data[i].real=data[i].real+other;
    }
    return result;
}
template<class T> ComplexArray ComplexArray::operator -(const vector<T> &other)
{
    ComplexArray result(*this);
    int n;
    if(nsamp>other.size())
        n=other.size();
    else
        n=nsamp;
    for(int i=0; i<n; i++)
    {
        result.data[i].real=data[i].real-other[i];
    }
    return result;
}
template<class T> ComplexArray ComplexArray::operator -(const T &other)
{
    ComplexArray result(*this);
    for(int i=0; i<nsamp; i++)
    {
        result.data[i].real=data[i].real-other;
    }
    return result;
}
template<class T> ComplexArray ComplexArray::operator *(const vector<T> &other)
{
    ComplexArray result(*this);
    int n;
    if(nsamp>other.size())
        n=other.size();
    else
        n=nsamp;
    for(int i=0; i<n; i++)
    {
        result.data[i].real=data[i].real*other[i];
        result.data[i].imag=data[i].imag*other[i];
    }
    return result;
}
template<class T> ComplexArray operator *(const vector<T>& lhs,const ComplexArray& rhs)
{
    return rhs*lhs;
}
template<class T> ComplexArray ComplexArray::operator *(const T &other)
{
    ComplexArray result(*this);
    for(int i=0; i<nsamp; i++)
    {
        result.data[i].real=data[i].real*other;
        result.data[i].imag=data[i].imag*other;
    }
    return result;
}
template<class T> ComplexArray operator *(const T& lhs,const ComplexArray& rhs)
{
    return rhs*lhs;
}
*/
}
#endif
