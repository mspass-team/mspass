/* This header is to contain assorted useful C and C++ procedures 
   appropriate for the tag utility.h.   */
#include <string>
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
}
