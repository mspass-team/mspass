#include "misc/blas.h"
#include <cfloat>
#include <math.h>
#include <mspass/utility/MsPASSError.h>
#include <mspass/utility/dmatrix.h>
namespace mspass::utility {
using namespace std;

vector<double> normalize_rows(const dmatrix &d) {
  int nrows = d.rows();
  int ncols = d.columns();
  double nrm, scl;
  vector<double> allnrms;
  allnrms.reserve(nrows);
  for (int i = 0; i < nrows; ++i) {
    nrm = dnrm2(ncols, d.get_address(i, 0), nrows);
    if (fabs(nrm) < DBL_EPSILON)
      throw MsPASSError("normalize_rows:  cannot normalize a row of all zeros",
                        ErrorSeverity::Invalid);
    scl = 1.0 / nrm;
    dscal(ncols, scl, d.get_address(i, 0), nrows);
    allnrms.push_back(nrm);
  }
  return allnrms;
}
vector<double> normalize_columns(const dmatrix &d) {
  int nrows = d.rows();
  int ncols = d.columns();
  double nrm, scl;
  vector<double> allnrms;
  allnrms.reserve(ncols);
  for (int i = 0; i < ncols; ++i) {
    nrm = dnrm2(nrows, d.get_address(0, i), 1);
    if (fabs(nrm) < DBL_EPSILON)
      throw MsPASSError(
          "normalize_columns:  cannot normalize a column of all zeros",
          ErrorSeverity::Invalid);
    scl = 1.0 / nrm;
    dscal(ncols, scl, d.get_address(0, i), 1);
    allnrms.push_back(nrm);
  }
  return allnrms;
}
} // namespace mspass::utility
