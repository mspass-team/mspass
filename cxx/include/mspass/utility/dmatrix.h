#ifndef _DMATRIX_H_
#define _DMATRIX_H_
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
/* Either text or binary can be specified here, but we use binary
 * to emphasize this class is normally serialized binary for
 * speed*/
#include "mspass/utility/MsPASSError.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
namespace mspass {
namespace utility {
//==================================================================
/*! \brief special convenience class for matrix indexing errors.
 *
 Thrown by a dmatrix if a requested index is outside the bounds
 of the matrix dimension.

\author Gary L. Pavlis
//==================================================================
*/
class dmatrix_index_error : public MsPASSError {
public:
  /*!
  Basic constructor for this error object.
  \param nrmax number of rows in matrix
  \param ncmax number of columns in matrix
  \param ir row index requested
  \param ic column index requested
  */
  dmatrix_index_error(const size_t nrmax, const size_t ncmax, const size_t ir,
                      const size_t ic) {
    row = ir;
    column = ic;
    nrr = nrmax;
    ncc = ncmax;
    std::ostringstream oss;
    oss << "dmatrix object:  indexing error" << std::endl
        << "Matrix index (" << row << "," << column
        << ")is outside range = " << nrr << "," << ncc << std::endl;
    message = oss.str();
    badness = ErrorSeverity::Invalid;
  };
  /* necessary baggage for some compilers - empty destructor */
  ~dmatrix_index_error() throw() {};

private:
  size_t row, column;
  size_t nrr, ncc;
};
/*! \brief Convenience class for dmatrix use errors.

Thrown by a dmatrix when two matrices have a size mismatch.

\author Gary L. Pavlis
*/
class dmatrix_size_error : public MsPASSError {
public:
  /*!
  Basic constructor for this error object.
  \param nr1 number of rows in matrix 1
  \param nc1 number of columns in matrix 1
  \param nr1 number of rows in matrix 2
  \param nc1 number of columns in matrix 2
  */
  dmatrix_size_error(const size_t nr1, const size_t nc1, const size_t nr2,
                     const size_t nc2) {
    nrow1 = nr1;
    ncol1 = nc1;
    nrow2 = nr2;
    ncol2 = nc2;
    std::ostringstream oss;
    oss << "dmatrix class:   size mismatch error in binary operator"
        << std::endl
        << "matrix on left is " << nrow1 << "X" << ncol1
        << " while matrix on right is " << nrow2 << "X" << ncol2 << std::endl;
    message = oss.str();
    badness = ErrorSeverity::Invalid;
  };
  /* necessary baggage for some compilers - empty destructor */
  ~dmatrix_size_error() throw() {};

private:
  size_t nrow1, ncol1, nrow2, ncol2;
};
/*! \brief Lightweight, simple matrix object.

This class defines a lightweight, simple double precision matrix.
Provides basic matrix functionality. Note that elements of the
matrix are stored internally in FORTRAN order but using
C style indexing.  That is, all indices begin at 0, not 1 and
run to size - 1.  Further, FORTRAN order means the elements are
actually ordered in columns as in FORTRAN in a continuous,
logical block of memory.  This allow one to use the BLAS functions
to access the elements of the matrix.  As usual be warned this
is useful for efficiency and speed, but completely circumvents the
bounds checking used by methods in the object.

 \author Robert R and Gary L. Pavlis
*/
class dmatrix {
public:
  /*! Default constructor.  Produces a 1x1 matrix as a place holder.*/
  dmatrix();
  /*! Basic constructor.  Allocates space for nr x nc array and initializes to
  zeros.

  \param nr number of rows to allocate for this matrix.
  \param nc number of columns to allocate for this matrix.
  */
  dmatrix(const size_t nr, const size_t nc);
  /*! Standard copy constructor.  */
  dmatrix(const dmatrix &other);
  /*! Destructor - releases any matrix memory. */
  ~dmatrix();
  /*! Indexing operator to fetch an array element.

  Can also be used to set an element as a left hand side (e.g. A(2,4)=2.0;).

  \param rowindex row to fetch
  \param colindex column to fetch.
  \returns value of matrix element at position (rowindex,colindex)
  \exception dmatrix_index_error is thrown if request is out of range
  */
  double operator()(const size_t rowindex, const size_t colindex) const;
  double &operator()(size_t r, size_t c);
  /*! Standard assignment operator */
  dmatrix &operator=(const dmatrix &other);
  /*! \brief Add one matrix to another.

  Matrix addition is a standard operation but demands the two matrices
  to be added are the same size. Hence, an exception will happen if you use
  this operator with a size mismatch.

  \param  A is the matrix to be added to this.
  \exception throws a dmatrix_size_error if other and this are not the same
  size.
  */
  dmatrix &operator+=(const dmatrix &other);
  /*! \brief Subtract one matrix to another.

  Matrix subtraction is a standard operation but demands the two matrices
  to be added are the same size. Hence, an exception will happen if you use
  this operator with a size mismatch.

  \param  other is the matrix to be subracted from to this.
  \exception throws a dmatrix_size_error if other and this are not the same
  size.
  */
  dmatrix &operator-=(const dmatrix &other);
  /*! Operator to add two matrices.

  This operator is similar to += but is the operator used in constructs
  like X=A+B.  Like += other and this must be the same size or an
  exception will be thrown.

  \param other matrix to be added
  \exception throws a dmatrix_size_error if other and this are not the same
  size.
  */
  dmatrix operator+(const dmatrix &other) const;
  /*! Operator to add two matrices.

  This operator is similar to -= but is the operator used in constructs
  like X=A-B.  Like -= other and this must be the same size or an
  exception will be thrown.

  \param other matrix to be added
  \exception throws a dmatrix_size_error if other and this are not the same
  size.
  */
  dmatrix operator-(const dmatrix &other) const;
  // friend class dvector;
  /*! \brief

    Procedure to multiply two matrices.  This could be implemented with
    a dmatrix::operator but this was an existing procedure known to work
    that I didn't choose to mess with.   Sizes must be compatible or an
    exception will be thrown.

    \param A is the left matrix for the multiply.
    \param B is the right matrix for the multiply.
    \exception dmatrix_size_error will be thrown if the columns in A are not
       equal to the rows in B.
    \return A*B
       */
  friend dmatrix operator*(const dmatrix &A, const dmatrix &B);
  //@{
  // Scale a matrix by a constant.  X=c*A where c is a constant.
  //@}
  /*! \brief Scale a matrix by a constant

  This procedure will multiply all elements of a matrix by a constant.
  The linear algebra concept of scaling a matrix.

  \param s is the scaling factor
  \param A is the matrix to be scaled
  \return sA
  */
  friend dmatrix operator*(const double &s, const dmatrix &A) noexcept;
  dmatrix operator*(double s) const noexcept;
  /*! \brief Transpose a matrix
   *
   A standard matrix operation is to transpose a matrix (reversing rows
   and columns).   This takes input A and returns A^T.

   \param A - matrix to transpose.
   \return A transposed
   */
  friend dmatrix tr(const dmatrix &A) noexcept;
  /* \brief Get a pointer to the location of a matrix component.

  Although a sharp knife it is useful at times to get a raw pointer to
  the data in a dmatrix.   A common one is using the BLAS to do vector
  operations for speed.   Users of this method must note that the data
  for a dmatrix is stored as a single rowXcolumn std::vector container.
  The matrix is stored in Fortran order (column1, column2, ...).
  The contiguous memory guarantee of std::vector allows vector operations
  with the BLAS to work by rows or columns.

  \param r is the row index of the desired address
  \param c is the column index of the desired memory address.
  \return pointer to component at row r and column c.
  \exception dmatrix_size_error will be throw if r or c are outside
    matrix dimensions.
    */
  double *get_address(size_t r, size_t c) const;
  /*! \brief Text output operator.

  Output is ascii data written in the matrix layout.  Note this can create
  huge lines and a lot of output for a large matrix so use carefully.
  \param os is the std::ostream to contain data.
  \param A is the data to be written
  */
  friend std::ostream &operator<<(std::ostream &os, dmatrix &A);
  /*! Return number of rows in this matrix. */
  size_t rows() const;
  /*! Return number of columns in this matrix. */
  size_t columns() const;
  /*! \brief Return a vector with 2 elements giving the size.

  This function returns an std::vector with 2 elements with size information.
  first component is rows, second is columns.  This simulates
  the matlab size function. */
  std::vector<size_t> size() const;
  /*! Initialize a matrix to all zeros. */
  void zero();

protected:
  std::vector<double> ary; // initial size of container 0
  size_t length;
  size_t nrr, ncc;

private:
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & nrr & ncc & length;
    ar & ary;
  }
};
/*! \brief A vector compatible with dmatrix objects.

A vector is a special case of a matrix with one row or column.   In this
implementation, however, it always means a column vector.   Hence, it is
possible to multiply a vector x and a matrix A as Ax provided they are
compatible sizes.  This differs from matlab where row and columns vectors
are sometimes used interchangably.
*/
class dvector : public dmatrix {
public:
  /*! Default constructor creates an empty vector. */
  dvector() : dmatrix() {};
  /*! Create a (zero initialized) vector of length nrv. */
  dvector(size_t nrv) : dmatrix(nrv, 1) {};
  /*! Copy constructor. */
  dvector(const dvector &other);
  /*! Standard assignment operator. */
  dvector &operator=(const dvector &other);
  /*! Extract component rowindex. */
  double &operator()(size_t rowindex);
  /*! Matrix vector multiple operator.

  This operator is used for constructs like y=Ax where x is a
  vector and A is a matrix.   y is the returned vector.
  \param A - matrix on right in multiply
  \param x - vector on left of multiply operation

  \return product A*x
  \exception dmatrix_size_error thrown if size of A and x do not match.
  */
  friend dvector operator*(const dmatrix &A, const dvector &x);
};

} // namespace utility
} // end namespace mspass
#endif
