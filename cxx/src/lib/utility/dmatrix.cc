#include <math.h>
#include "mspass/dmatrix.h"
#include "FC.h"

extern "C" double ddot (
  const int& n,
  const double* x, const int& incx,
  const double* y, const int& incy);

namespace mspass{
dmatrix::dmatrix()
{
  nrr=0;
  ncc=0;
  length=0;
  ary.reserve(0);
}
dmatrix::dmatrix(const int nr, const int nc)
{
  nrr=nr;
  ncc=nc;
  length=nr*nc;
  if(length<1)
  {
      length=1;
      nrr=ncc=0;
  }
  /* This std::vector method allocates space so zero method can just
   * use indexing. */
  ary.resize(length);
  this->zero();
}

dmatrix::dmatrix(const dmatrix& other)
  {
  nrr=other.nrr;
  ncc=other.ncc;
  length=other.length;
  ary=other.ary;
  }

dmatrix::~dmatrix()
{
//if(ary!=NULL) delete [] ary;
}

double dmatrix::operator()(const int rowindex, const int colindex) const
{
  int out_of_range=0;
  if (rowindex>=nrr) out_of_range=1;
  if (rowindex<0) out_of_range=1;
  if (colindex>=ncc) out_of_range=1;
  if (colindex<0) out_of_range=1;
  if (out_of_range)
	throw dmatrix_index_error(nrr,ncc,rowindex,colindex);
  double result=ary[rowindex+(nrr)*(colindex)];
  return result;
}
double& dmatrix::operator()(const int rowindex, const int colindex) 
{
  int out_of_range=0;
  if (rowindex>=nrr) out_of_range=1;
  if (rowindex<0) out_of_range=1;
  if (colindex>=ncc) out_of_range=1;
  if (colindex<0) out_of_range=1;
  if (out_of_range)
	throw dmatrix_index_error(nrr,ncc,rowindex,colindex);
  return (ary[rowindex+(nrr)*(colindex)]);
}
//
// subtle difference here.  This one returns a pointer to the 
// requested element
//
double* dmatrix::get_address(int rowindex, int colindex) 
{
  double *ptr;
  int out_of_range=0;
  if (rowindex>=nrr) out_of_range=1;
  if (rowindex<0) out_of_range=1;
  if (colindex>=ncc) out_of_range=1;
  if (colindex<0) out_of_range=1;
  if (out_of_range)
        throw dmatrix_index_error(nrr,ncc,rowindex,colindex);
  ptr=&(ary[rowindex+(nrr)*(colindex)]);
  return(ptr);
}

dmatrix& dmatrix::operator=(const dmatrix& other)
{
    if(&other!=this) 
    {
	ncc=other.ncc;
	nrr=other.nrr;
	length=other.length;
        ary=other.ary;
    } 
    return *this;
}

void dmatrix::operator+=(const dmatrix& other)
 {
int i;
  if ((nrr!=other.nrr)||(length!=other.length))
	throw dmatrix_size_error(nrr, ncc, other.nrr, other.length);
for(i=0;i<length;i++)
  ary[i]+=other.ary[i];
 }

void dmatrix::operator-=(const dmatrix& other)
 {
int i;
  if ((nrr!=other.nrr)||(length!=other.length))
	throw dmatrix_size_error(nrr, ncc, other.nrr, other.length);
for(i=0;i<length;i++)
  ary[i]-=other.ary[i];
 }

dmatrix dmatrix::operator+(const dmatrix &other) 
{
    try{
        dmatrix result(*this);
        result += other;
        return result;
    }catch(...){throw;};
}
dmatrix dmatrix::operator-(const dmatrix &other)
{
    try{
        dmatrix result(*this);
        result -= other;
        return result;
    }catch(...){throw;};
}


dmatrix operator*(const dmatrix& x1,const dmatrix& b) 
{
	int i,j;
        /* The computed length in last arg to the error object is a relic*/
	if(x1.columns()!=b.rows())
		throw dmatrix_size_error(x1.rows(), x1.columns(), 
                        b.rows(), b.rows()*b.columns());
	dmatrix prod(x1.rows(),b.columns());
	for(i=0;i<x1.rows();i++)
	  for(j=0;j<b.columns();j++)
	  {
              double *x1ptr,*bptr;
              x1ptr=const_cast<dmatrix&>(x1).get_address(i,0);
              bptr=const_cast<dmatrix&>(b).get_address(0,j);
              /* This temporary seems necessary */
              double *dptr;
              dptr=prod.get_address(i,j);
              *dptr=ddot(x1.columns(),x1ptr,x1.rows(),bptr,1);
	  }
	return prod;
}

dmatrix operator*(const double& x, const dmatrix &zx) noexcept
{
  int i;
  dmatrix tempmat(zx.rows(),zx.columns());
  int lenary=zx.rows()*zx.columns();
  double *zptr,*dptr;
  zptr=const_cast<dmatrix&>(zx).get_address(0,0);
  dptr=tempmat.get_address(0,0);
  for(i=0;i<lenary;++i)
  {
      (*dptr)=x*(*zptr);
      ++dptr;
      ++zptr;
  }
  return tempmat;
}
/*
dmatrix dmatrix::operator* (const double& x) noexcept
{
    double *ptr;
    dmatrix result(*this);
    ptr=result.get_address(0,0);
    dscal(length,x,ptr,1);
    return result;
}
*/



dmatrix tr(const dmatrix& x1) noexcept
{
  int i,j;
  dmatrix temp(x1.columns(),x1.rows());
  for(i=0; i<x1.rows(); i++)
     for(j=0; j<x1.columns();j++)
     {
  	temp(j,i)=x1(i,j);
     }
  return temp;
}


ostream& operator<<(ostream& os, dmatrix& x1)
{
  int i,j;
  for(i=0;i<x1.rows();i++)
  {
  for(j=0;j<x1.columns();j++) os << " "<< x1(i,j);
    os<<"\n";
  }
  return os;
}


void dmatrix::zero()
{
    for(int i=0;i<length;++i) ary[i]=0.0;
}
vector<int> dmatrix::size() const
{
	vector<int> sz;
        sz.push_back(nrr);
        sz.push_back(ncc);
	return(sz);
}
// simpler versions of same

int dmatrix::rows() const
{
	return(nrr);
}
int dmatrix::columns() const
{
	return(ncc);
}

// vector methods
dvector& dvector::operator=(const dvector& other)
{
	if(this != &other)
	{
		ncc=1;
		nrr=other.nrr;
		length=other.length;
                ary=other.ary;
	} 
	return *this;
}
dvector::dvector(const dvector& other)
{
	ncc=1;
	nrr=other.nrr;
	length=other.length;
        ary=other.ary;
}
double &dvector::operator()(const int rowindex) 
{
  if (rowindex>=nrr)
	throw dmatrix_index_error(nrr,1,rowindex,1);
  return (ary[rowindex]);
}		
dvector operator*(const dmatrix& x1,const dvector& b)
{
	int i;
        int nrx1=x1.rows();
        int ncx1=x1.columns();
        int nrb=const_cast<dvector&>(b).rows();
	if(ncx1!=nrb)
		throw dmatrix_size_error(nrx1, ncx1, nrb, 1);
	dvector prod(nrx1);
	for(i=0;i<nrx1;i++)
		prod(i)=ddot(nrb,
			const_cast<dmatrix&>(x1).get_address(i,0),nrx1,
			const_cast<dvector&>(b).get_address(0,0),1);
	return prod;
}
}  // end mspass namespace 
