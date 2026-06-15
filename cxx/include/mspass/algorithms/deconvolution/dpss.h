#ifndef __DPSS_H__
#define __DPSS_H__
#include <cmath>
#include <string.h>
namespace mspass::algorithms::deconvolution {

/*! \brief Base error container used by DPSS/LAPACK helper routines.

This lightweight class stores a short message for numerical helper failures
raised while computing discrete prolate spheroidal sequences.
*/
class ERR {
public:
  /*! Construct an empty error message. */
  ERR() {};
  /*! Construct from a null-terminated error message. */
  ERR(const char *msg);
  /*! Copy the stored message into caller-provided storage. */
  void getmsg(char *errmsg);
  /*! Return the stored message. */
  const char *getmsg();

protected:
  /*! Fixed-size message buffer used by the original DPSS implementation. */
  char msg[30];
};

/*! \brief Error raised for failures reported by LAPACK routines. */
class LAPACK_ERROR : public ERR {
public:
  /*! Construct an empty LAPACK error. */
  LAPACK_ERROR() {};
  /*! Construct from a LAPACK error message. */
  LAPACK_ERROR(const char *errmsg);
};

/*! \brief Compute DPSS energy concentration values.

\param h Input DPSS sequence array.
\param n Number of samples in each sequence.
\param NW Time-bandwidth product.
\param lambda Output energy concentration values.
\param nseq Number of sequences in h.
*/
void compute_energy_concentrations(double *h, int n, double NW, double *lambda,
                                   int nseq);

/*! \brief Compute selected eigenvectors of the DPSS tridiagonal system.

\param n Matrix dimension.
\param D Diagonal of the tridiagonal matrix.
\param E Off-diagonal of the tridiagonal matrix.
\param il First selected eigenvalue index.
\param iu Last selected eigenvalue index.
\param eig_val Output eigenvalues.
\param eig_vec Output eigenvectors.
\param vec_length Leading dimension of eig_vec.
*/
void eig_iit(int n, double *D, double *E, int il, int iu, double *eig_val,
             double *eig_vec, int vec_length);

/*! \brief Normalize a vector in place. */
void normalize_vec(double *h, int n);

/*! \brief Apply the DPSS sign convention to one sequence. */
void polarize_dpss(double *h, int n, int iseq);

/*! \brief Compute DPSS tapers with even/odd splitting.

The implementation exploits double symmetry to reduce the eigenproblem.
*/
void dpss_calc(int n, double NW, int seql, int sequ, double *h);
} // namespace mspass::algorithms::deconvolution
#endif
