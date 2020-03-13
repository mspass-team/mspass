#include <mspass/utility/dmatrix.h>
#include "misc/blas.h"
namespace mspass{
using std::vector;
using mspass::dmatrix;
vector<double> normalize_rows(const dmatrix& d)
{
  int nrows=d.rows();
  int ncols=d.columns();
  double nrm,scl;
  vector<double> allnrms;
  allnrms.reserve(nrows);
  for(int i=0;i<nrows;++i)
  {
    nrm=dnrm2(ncols,d.get_address(i,0),ncols);
    scl=1.0/nrm;
    dscal(ncols,scl,d.get_address(i,0),ncols);
    allnrms.push_back(nrm);
  }
  return allnrms;
}
vector<double> normalize_columns(const dmatrix& d)
{
  int nrows=d.rows();
  int ncols=d.columns();
  double nrm,scl;
  vector<double> allnrms;
  allnrms.reserve(ncols);
  for(int i=0;i<ncols;++i)
  {
    nrm=dnrm2(nrows,d.get_address(0,i),1);
    scl=1.0/nrm;
    dscal(ncols,scl,d.get_address(0,i),1);
    allnrms.push_back(nrm);
  }
  return allnrms;
}
}
