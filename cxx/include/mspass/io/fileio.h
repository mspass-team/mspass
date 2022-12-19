#ifndef _MSEED_INDEX_H_
#define _MSEED_INDEX_H_
#include <string>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::io
{
/*! \brief Fast file writer for native TimeSeries save to a file.

When saving data to a file system there is no standard way to do so we
know of that is faster than the low level C fwrite function.  This function
uses fwrite to write ONLY the sample data in the input TimeSeries object to
a file specified by a directory (dir) and leaf file name (dfile).  It can
do so because the std::vector container is required by the standard to define
a contiguous block of memory. This function is expected to be used in MsPASS
only under the hood of the python database writer for native saves.

The function constructs a unix path file name as dir+"/"+dfile.  If that
file does not exist it will be created.  If it does exist the write will
append to the existing file.   The attributes dir, dfile, and foff are
always saved in the data's Metadata container so they can be saved to
the MonogoDB database in MsPASS AFTER calling this writer.  For that reason
d is not const because it simply isn't.

\param d data to be saved (altered on return as noted above)
\param dir directory name (assumed to not contain a trailing /)
\param dfile leaf file name for save

\return file position in output file of the first byte written.
the same number is saved in the "foff" metadata attribute.

\expection This function will throw a MsPASS error for one of several
common io related issues. Caller should always include the call to this
function in a try block.

*/
long int fwrite_to_file(mspass::seismic::TimeSeries& d,
  const std::string dir,const std::string dfile);
/*! \brief Fast file writer for native Seismogram save to a file.

When saving data to a file system there is no standard way to do so we
know of that is faster than the low level C fwrite function.  This function
uses fwrite to write ONLY the sample data in the input Seismogram object to
a file specified by a directory (dir) and leaf file name (dfile).  It can
do so because the std::vector container is required by the standard to define
a contiguous block of memory. This function is expected to be used in MsPASS
only under the hood of the python database writer for native saves.  This
function works for Seismogram objects it this form only because the dmatrix
container puts all the sample data for the 3xnpts matrix in a contiguous
block of memory fetched interally with the get_address method.

The function constructs a unix path file name as dir+"/"+dfile.  If that
file does not exist it will be created.  If it does exist the write will
append to the existing file.   The attributes dir, dfile, and foff are
always saved in the data's Metadata container so they can be saved to
the MonogoDB database in MsPASS AFTER calling this writer.  For that reason
d is not const because it simply isn't.

\param d data to be saved (altered on return as noted above)
\param dir directory name (assumed to not contain a trailing /)
\param dfile leaf file name for save

\return file position in output file of the first byte written.
the same number is saved in the "foff" metadata attribute.

\expection This function will throw a MsPASS error for one of several
common io related issues. Caller should always include the call to this
function in a try block.

*/
long int fwrite_to_file(mspass::seismic::Seismogram& d,
  const std::string dir,const std::string dfile);
/*! \brief Fast file writer for native TimeSeries save to a file.

When saving data to a file system there is no standard way to do so we
know of that is faster than the low level C fwrite function.  This function
uses fwrite to write ONLY the sample data of all TimeSeries objects in the 
input Ensemble<TimeSeries> object to a file specified by a directory (dir) 
and leaf file name (dfile). It can do so because the std::vector container is 
required by the standard to define a contiguous block of memory. This function 
is expected to be used in MsPASS only under the hood of the python database 
writer for native saves. This function works for Ensemble<TimeSeries> objects 
it this form only because the dmatrix container puts all the sample data for 
the 3xnpts matrix in a contiguous block of memory fetched interally with the 
get_address method.

The function constructs a unix path file name as dir+"/"+dfile.  If that
file does not exist it will be created.  If it does exist the write will
append to the existing file.   The attributes dir, dfile, and foff are
always saved in the data's Metadata container so they can be saved to
the MonogoDB database in MsPASS AFTER calling this writer.  For that reason
d is not const because it simply isn't.

\param d data to be saved (altered on return as noted above)
\param dir directory name (assumed to not contain a trailing /)
\param dfile leaf file name for save

\return file position in output file of the first byte written.
the same number is saved in the "foff" metadata attribute.

\expection This function will throw a MsPASS error for one of several
common io related issues. Caller should always include the call to this
function in a try block.

*/
std::vector<long int> fwrite_to_file(mspass::seismic::Ensemble<mspass::seismic::TimeSeries>& d,
  const std::string dir,const std::string dfile);

/*! \brief Fast file writer for native Ensemble<Seismogram> save to a file.

When saving data to a file system there is no standard way to do so we
know of that is faster than the low level C fwrite function.  This function
uses fwrite to write ONLY the sample data of all Seismogram objects in the 
input Ensemble<Seismogram> object to a file specified by a directory (dir) 
and leaf file name (dfile). It can do so because the std::vector container is 
required by the standard to define a contiguous block of memory. This function 
is expected to be used in MsPASS only under the hood of the python database 
writer for native saves. This function works for Ensemble<Seismogram> objects 
it this form only because the dmatrix container puts all the sample data for 
the 3xnpts matrix in a contiguous block of memory fetched interally with the 
get_address method.

The function constructs a unix path file name as dir+"/"+dfile.  If that
file does not exist it will be created.  If it does exist the write will
append to the existing file.   The attributes dir, dfile, and foff are
always saved in the data's Metadata container so they can be saved to
the MonogoDB database in MsPASS AFTER calling this writer.  For that reason
d is not const because it simply isn't.

\param d data to be saved (altered on return as noted above)
\param dir directory name (assumed to not contain a trailing /)
\param dfile leaf file name for save

\return file position in output file of the first byte written.
the same number is saved in the "foff" metadata attribute.

\expection This function will throw a MsPASS error for one of several
common io related issues. Caller should always include the call to this
function in a try block.

*/
std::vector<long int> fwrite_to_file(mspass::seismic::Ensemble<mspass::seismic::Seismogram>& d,
  const std::string dir,const std::string dfile);

/*! \brief Use C fread to read sample data from a file.

This low level function is used in the file based reader of mspass to
speed up python readers.  It is intrinsically dangerous because it assumes
the data object has a preconstructed size sufficient to hold the data
loaded with the low-level C fread function.
That buffer in this function is the the dmatrix container, u, used to hold
the sample data of a Seismogram object.  The reader assumes the input
has been initialized on construction or with set_npts to the size
matching the data file contents.  If there is a mismatch the results
are unpredictable.

\param Seismogram object to hold the sample data to be read from the file.
  Note that this function acts like a subroutine with entire purpose being
  to fill the data array of this object.
\param dir is the directory name to use for file name (no trailing slash)
\param dfile is the leaf file name to be openned.
\param foff is a the number of bytes to seek for first byte of data.

\return number of samples read.  Note caller should test this
value as a short read will not cause an error to be thrown.  The value
return should be 3 * d.npts().

\exception This function may throw a MsPASSError exception if anything
goes wrong in the read process (open failure, seek fails, fread fails completely).
If that happens the data result should be killed as the sample contents are
guaranteed to be invalid.
*/

size_t fread_from_file(mspass::seismic::Seismogram& d,const std::string dir, const std::string dfile,
     const long int foff);

/*! \brief Use C fread to read sample data from a file.

This low level function is used in the file based reader of mspass to
speed up python readers.  It is intrinsically dangerous because it assumes
the data object has a preconstructed size sufficient to hold the data
loaded with the low-level C fread function.
That buffer in this function is the std::vector container, s, used to hold
the sample data of a Seismogram object.  The reader assumes the input
has been initialized on construction or with set_npts to the size
matching the data file contents.  If there is a mismatch the results
are unpredictable.

\param TimeSeries object to hold the sample data to be read from the file
  Note that this function acts like a subroutine with entire purpose being
  to fill the data array of this object.
\param dir is the directory name to use for file name (no trailing slash)
\param dfile is the leaf file name to be openned.
\param foff is a the number of bytes to seek for first byte of data.

\return number of samples read.  Note caller should test this
value as a short read will not cause an error to be thrown.  The value
return should be d.npts().

\exception This function may throw a MsPASSError exception if anything
goes wrong in the read process (open failure, seek fails, fread fails completely).
If that happens the data result should be killed as the sample contents are
guaranteed to be invalid.
*/

size_t fread_from_file(mspass::seismic::TimeSeries& d,const std::string dir, const std::string dfile,
     const long int foff);

/*! \brief Use C fread to read multiple TimeSeries from a file.

This low level function is used in the file based reader of mspass to
speed up python readers.  It is intrinsically dangerous because it assumes
the data object has a preconstructed size sufficient to hold the data
loaded with the low-level C fread function.
The reader assumes the input has been initialized on construction or 
with set_npts to the size matching the data file contents.  If there is 
a mismatch the results are unpredictable.

\param Ensemble<TimeSeries> object to hold the sample data to be read from the 
  files. Note that this function acts like a subroutine with entire purpose being
  to fill the data array of this object.
\param dir is the directory name to use for file name (no trailing slash)
\param dfile is the leaf file name to be openned.
\param foffs is a vector of the number of bytes to seek for first byte of all 
  TimeSeries objects to be read in the given file.
\param indexes is a vector of indexes of TimeSeries objects to be read to the 
  ensemble.
\param length is the size of the ensemble. It is used to resize the ensemble.

\return total number of samples read for all TimeSeries objects.  Note caller 
should test this value as a short read will not cause an error to be thrown.  

\exception This function may throw a MsPASSError exception if anything
goes wrong in the read process (open failure, seek fails, fread fails completely).
If that happens the data result should be killed as the sample contents are
guaranteed to be invalid.
*/

size_t fread_from_files(mspass::seismic::Ensemble<mspass::seismic::TimeSeries> &d, const std::string dir, 
    const std::string dfile, std::vector<long int> foffs, std::vector<long int> indexes, const long int length);

/*! \brief Use C fread to read multiple Seismogram from a file.

This low level function is used in the file based reader of mspass to
speed up python readers.  It is intrinsically dangerous because it assumes
the data object has a preconstructed size sufficient to hold the data
loaded with the low-level C fread function.
The reader assumes the input has been initialized on construction or 
with set_npts to the size matching the data file contents.  If there is 
a mismatch the results are unpredictable.

\param Ensemble<Seismogram> object to hold the sample data to be read from the 
  files. Note that this function acts like a subroutine with entire purpose being
  to fill the data array of this object.
\param dir is the directory name to use for file name (no trailing slash)
\param dfile is the leaf file name to be openned.
\param foffs is a vector of the number of bytes to seek for first byte of all 
  Seismogram objects to be read in the given file.
\param indexes is a vector of indexes of Seismogram objects to be read to the 
  ensemble.
\param length is the size of the ensemble. It is used to resize the ensemble.

\return total number of samples read for all Seismogram objects.  Note caller 
should test this value as a short read will not cause an error to be thrown.  

\exception This function may throw a MsPASSError exception if anything
goes wrong in the read process (open failure, seek fails, fread fails completely).
If that happens the data result should be killed as the sample contents are
guaranteed to be invalid.
*/

size_t fread_from_files(mspass::seismic::Ensemble<mspass::seismic::Seismogram> &d, const std::string dir, 
    const std::string dfile, std::vector<long int> foffs, std::vector<long int> indexes, const long int length);

}
#endif
