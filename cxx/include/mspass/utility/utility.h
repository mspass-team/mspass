/* This header is to contain assorted useful C and C++ procedures 
   appropriate for the tag utility.h.   */
#include <string>
#include <mspass/utility/dmatrix.h>
namespace mspass{
/*! Standard method returns a string defining the top level data directory for mspass. 

Programs often need a standard set of initialization files.  In mspass we 
group these under a "data" directory.  This procedure returns the top of the
chain of data directories.  Note this is the top of a directory chain and
most application will need to add a subdirectory.   e.g.

string datadir=mspass::data_directory();
string mydatafile=datadir+"/pf";

  */
std::string data_directory();
/*! \brief Normalize rows of a matrix to unit L2 length.

Sometimes it is necessary to normalize a matrix by rows or columns.
This function normalizes the rows of a matrix. 

\param d is the matrix to be normalized
\return vector of the computed L2 norms of each row used for normalization.
*/
vector<double> normalize_rows(const mspass::dmatrix& d);
/*! \brief Normalize columns of a matrix to unit L2 length.

Sometimes it is necessary to normalize a matrix by rows or columns.
This function normalizes the columns of a matrix. 

\param d is the matrix to be normalized
\return vector of the computed L2 norms of each column used for normalization.
*/
vector<double> normalize_columns(const mspass::dmatrix& d);
}
