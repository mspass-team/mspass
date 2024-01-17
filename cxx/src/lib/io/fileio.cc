#include <memory>
#include <vector>
#include <stdio.h>
#include <string>
#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/keywords.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/io/fileio.h"
namespace mspass::io
{
//using namespace mspass::io;
using namespace mspass::seismic;
using mspass::utility::MsPASSError;
using mspass::utility::ErrorSeverity;
using namespace std;

/*This is a file scope function to allow the overlaoded fwrite_to_file
functions to share this common code to write a contiguous buffer of data.
to a file.

Note I considered adding an old school call to lockf to assure this
function would be thread safe.   However, the man page for the posix
function lockffile says that is no longer necessary and stdio is
now tread safe - fopen creates an intrinsic lock that is not released
until fclose is called.  That is important for mspass as multiple threads
writing to the same file can be expected to be common. */
long int fwrite_sample_data(const string dir, const string dfile, double *dptr, const size_t nd)
{
	try{
		FILE *fp;
		long int foff;
		string fname;
		if(dir.length()>0)
		  /* for expected context for use in python we will assume dir does not
			have a trailing path separator so we always insert / */
			fname = dir + "/" + dfile;
		else
		  /* Null as always in unix means use current directory*/
			fname=dfile;
		if((fp=fopen(fname.c_str(),"a")) == NULL)
		  /* use the name of the overloaded parent instead of the actual function - intentional*/
			throw MsPASSError("fwrite_to_file:  Open failed on file "+fname,ErrorSeverity::Invalid);
	  /* Both fseek and ftell can fail in weird circumstances, but I intentionally
		do not trap that condition as if either have issues I am quite sure
		the fwrite will fail */
		fseek(fp,0L,2);
		foff = ftell(fp);
		if( fwrite((void*)dptr,sizeof(double),nd,fp) != nd)
		{
			fclose(fp);
			throw MsPASSError("fwrite_to_file:  fwrite error while writing to file "+fname,ErrorSeverity::Invalid);
		}
		fclose(fp);
		return foff;
	}catch(...){throw;};
}
/*! Write sample data for a TimeSeries to a file with fwrite.  Always
appends and returns foff of the position where fwrite wrote these data.
Returns -1 if it receives a datum marked dead.
*/
long int fwrite_to_file(TimeSeries& d,const string dir,const string dfile)
{
	if(d.dead()) return -1;
	/* Using this function avoids repetitious code with Seismogram version. */
	long int foff;
	try{
	  foff = fwrite_sample_data(dir,dfile,d.s.data(),d.npts());
	}catch(...){throw;};
	/* We always set these 3 attributes in Metadata so they can be properly
	saved to the database after a successful write.  Repetitious with Seismogram
	but a function to do this would be more confusing that helpful */
	d.put_string(SEISMICMD_dir,dir);
	d.put_string(SEISMICMD_dfile,dfile);
	d.put_long(SEISMICMD_foff,foff);
	return(foff);
}
/*! Write sample data for a Seismogram to a file with fwrite.  Always
appends and returns foff of the position where fwrite wrote these data.
Note the data are a raw dump of the contiguous block storing the 3*npts
sample matrix.

Returns -1 if it receives a datum marked dead.
*/
long int fwrite_to_file(Seismogram& d, const string dir,const string dfile)
{
	if(d.dead()) return(-1);
	/* Using this function avoids repetitious code with TimeSeries version.
	Note use of 3*npts as the buffer size*/
	long int foff;
	try{
		foff = fwrite_sample_data(dir,dfile,d.u.get_address(0,0),3*d.npts());
	}catch(...){throw;};
	d.put_string(SEISMICMD_dir,dir);
	d.put_string(SEISMICMD_dfile,dfile);
	d.put_long(SEISMICMD_foff,foff);

	return(foff);
}
/*! Write sample data for an Ensemble of TimeSeries to a single file.

Writing ensemble data with this function is more efficient than writing atomic
data one at time.  The reason is this function writes all the sample data for
the ensemble to a single and only opens and closes the file specfied once.
It returns a vector of foff values.  Dead members have no sample data written
and will generate a -1 entry in the foff vector returned.  Caller should handle
that condition in some way.

If entire ensemble is marked dead it will return an empty vector container.

\param d input ensemble to save sample data
\param dir directory name (if entry use current directory)
\param dfile file name for write

*/
std::vector<long int> fwrite_to_file(
	mspass::seismic::LoggingEnsemble<mspass::seismic::TimeSeries>& d, \
	  const std::string dir,
		  const std::string dfile)
{
	try{
		FILE *fp;
		vector<long int> foffs;
		/* This will return an empty vector if the ensemble is marked dead - callers should handle this condition
		but they normally shouldn't be calling this function if the entire ensemble is marked dead anyway.*/
		if(d.dead()) return(foffs);
		string fname;
		if(dir.length()>0)
		  /* for expected context for use in python we will assume dir does not
			have a trailing path separator so we always insert / */
			fname = dir + "/" + dfile;
		else
		  /* Null as always in unix means use current directory*/
			fname=dfile;
		if((fp=fopen(fname.c_str(),"a")) == NULL)
		  /* use the name of the overloaded parent instead of the actual function - intentional*/
			throw MsPASSError("fwrite_to_file (TimeSeriesEnsemble):  Open failed on file "+fname,ErrorSeverity::Invalid);
		/* This guarantees appending - not essential since we open in "a" mode but
		clearer */
		fseek(fp,0L,2);

		for (int i = 0; i < d.member.size(); ++i) {
<<<<<<< HEAD
			/* Silenetly skip dead data */
			if(d.member[i].dead()) continue;
			long int foff = ftell(fp);
			foffs.push_back(foff);
			TimeSeries& t = d.member[i];
			if (fwrite((void *)t.s.data(), sizeof(double), t.npts(), fp) != t.npts())
=======
			if(d.member[i].dead())
			    foffs.push_back(-1);
			else
>>>>>>> dac6e7f1729a67e87eaa44658263e18e705488aa
			{
				long int foff = ftell(fp);
				foffs.push_back(foff);
				TimeSeries& t = d.member[i];
				if (fwrite((void *)t.s.data(), sizeof(double), t.npts(), fp) != t.npts())
				{
					fclose(fp);
					stringstream ss;
					ss << "fwrite_to_file (TimeSeriesEnsemble):  fwrite error while writing ensemble member "
				   	<< i << " to file="<<fname<<endl;
					throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
				}
				/* We always set these 3 attributes in Metadata so they can be properly
				saved to the database after a successful write.  Repetitious with Seismogram
				but a function to do this would be more confusing that helpful */
				t.put_string(SEISMICMD_dir, dir);
				t.put_string(SEISMICMD_dfile, dfile);
				t.put_long(SEISMICMD_foff, foff);
			}
		}
		fclose(fp);
		return foffs;
	}catch(...){throw;};
}
/*! Write sample data for an Ensemble of Seismogram objects to a single file.

Writing ensemble data with this function is more efficient than writing atomic
data one at time.  The reason is this function writes all the sample data for
the ensemble to a single and only opens and closes the file specfied once.
It returns a vector of foff values.  Dead members have no sample data written
and will generate a -1 entry in the foff vector returned.  Caller should handle
that condition in some way.

If entire ensemble is marked dead it will return an empty vector container.

\param d input ensemble to save sample data
\param dir directory name (if entry use current directory)
\param dfile file name for write

*/
std::vector<long int> fwrite_to_file(mspass::seismic::LoggingEnsemble<mspass::seismic::Seismogram>& d, const std::string dir,const std::string dfile)
{
	try{
		FILE *fp;
		vector<long int> foffs;
		if(d.dead()) return(foffs);
		string fname;
		if(dir.length()>0)
		  /* for expected context for use in python we will assume dir does not
			have a trailing path separator so we always insert / */
			fname = dir + "/" + dfile;
		else
		  /* Null as always in unix means use current directory*/
			fname=dfile;
		if((fp=fopen(fname.c_str(),"a")) == NULL)
		  /* use the name of the overloaded parent instead of the actual function - intentional*/
			throw MsPASSError("fwrite_to_file (SeismogramEnsemble):  Open failed on file "+fname,ErrorSeverity::Invalid);
		/* This guarantees appending - not essential since we open in "a" mode but
		clearer */
		fseek(fp,0L,2);

		for (int i = 0; i < d.member.size(); ++i) {
<<<<<<< HEAD
			/* Silently skip dead data */
			if(d.member[i].dead()) continue;
			long int foff = ftell(fp);
			foffs.push_back(foff);
			Seismogram& t = d.member[i];
			if (fwrite((void *)t.u.get_address(0,0), sizeof(double), 3*t.npts(), fp) != 3*t.npts())
=======
			if(d.member[i].dead())
			    foffs.push_back(-1);
			else
>>>>>>> dac6e7f1729a67e87eaa44658263e18e705488aa
			{
				if(d.member[i].dead()) continue;
				long int foff = ftell(fp);
				foffs.push_back(foff);
				Seismogram& t = d.member[i];
				if (fwrite((void *)t.u.get_address(0,0), sizeof(double), 3*t.npts(), fp) != 3*t.npts())
				{
					fclose(fp);
					stringstream ss;
					ss << "fwrite_to_file (SeismogramEnsemble):  fwrite error while writing ensemble member "
				   << i << " to file="<<fname<<endl;
				  throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
				}
				/* We always set these 3 attributes in Metadata so they can be properly
				saved to the database after a successful write.  Repetitious with Seismogram
				but a function to do this would be more confusing that helpful */
				t.put_string(SEISMICMD_dir, dir);
				t.put_string(SEISMICMD_dfile, dfile);
				t.put_long(SEISMICMD_foff, foff);
			}
		}
		fclose(fp);
		return foffs;
	}catch(...){throw;};
}

size_t fread_sample_data(double *buffer,const string dir, const string dfile,
     const long int foff,const int nsamples)
{
	FILE *fp;
	string fname;
	if(dir.length()>0)
	  /* for expected context for use in python we will assume dir does not
		have a trailing path separator so we always insert / */
		fname = dir + "/" + dfile;
	else
	  /* Null as always in unix means use current directory*/
		fname=dfile;
	if((fp=fopen(fname.c_str(),"r")) == NULL)
		throw MsPASSError("fread_data_from_file:  Open failed on file "+fname,ErrorSeverity::Invalid);
	if(foff>0)
	{
		if(fseek(fp,foff,SEEK_SET))
		{
			fclose(fp);
		  throw MsPASSError("fread_data_from_file:  fseek failure on file="+fname,ErrorSeverity::Invalid);
		}
	}
	size_t retcount;
	retcount = fread((void*)buffer,sizeof(double),nsamples,fp);
	fclose(fp);
	return retcount;
}
size_t fread_from_file(Seismogram& d,const string dir, const string dfile,
     const long int foff)
{
	size_t ns_read;
	try{
		ns_read = fread_sample_data(d.u.get_address(0,0),dir,dfile,foff,3*d.npts());
		return ns_read;
	}catch(...){throw;};
}
size_t fread_from_file(TimeSeries& d,const string dir, const string dfile,
     const long int foff)
{
	size_t ns_read;
	try{
		ns_read = fread_sample_data(&(d.s[0]),dir,dfile,foff,d.npts());
		return ns_read;
	}catch(...){throw;};
}
size_t fread_from_file(mspass::seismic::LoggingEnsemble<mspass::seismic::Seismogram> &de,
 const std::string dir, const std::string dfile, std::vector<long int> indexes)
{
	size_t ns_read_sum;
	int n = indexes.size();
	FILE *fp;
	string fname;
	if(dir.length()>0)
	  /* for expected context for use in python we will assume dir does not
		have a trailing path separator so we always insert / */
		fname = dir + "/" + dfile;
	else
	  /* Null as always in unix means use current directory*/
		fname=dfile;
	if((fp=fopen(fname.c_str(),"r")) == NULL) {
		de.kill();
		stringstream ss;
		ss << "can not open file in " << fname << endl;
		de.elog.log_error(ss.str());
		return -1;
	}

	for (int ind = 0; ind < n; ++ind) {
		size_t ns_read;
		int i = indexes[ind];
		long int foff;
		if (de.member[i].is_defined(SEISMICMD_foff))
		{
		  foff = de.member[i].get_long(SEISMICMD_foff);
		}
		else
		{
		  de.member[i].kill();
		  stringstream ss;
		  ss << "foff not defined for " << i << " member in ensemble" << endl;
		  de.member[i].elog.log_error(ss.str());
		  continue;
		}
		try{
			if(fseek(fp,foff,SEEK_SET))
			{
				fclose(fp);
				de.member[i].kill();
				stringstream ss;
		  		ss << "can not fseek in " << foff << endl;
				     de.member[i].elog.log_error(ss.str());
				continue;
			}
			ns_read = fread((void*)de.member[i].u.get_address(0, 0), sizeof(double), 3 * de.member[i].npts(), fp);
			if (ns_read != 3 * de.member[i].npts())
			{
				de.member[i].elog.log_error(string("read error: npts not equal"));
				de.member[i].kill();
			}
			else
			{
				de.member[i].set_live();
			}
			ns_read_sum += ns_read;
		}catch(...){throw;};
	}
	fclose(fp);
	de.set_live();
	return ns_read_sum;
}
size_t fread_from_file(mspass::seismic::LoggingEnsemble<mspass::seismic::TimeSeries> &de,
 const std::string dir, const std::string dfile, std::vector<long int> indexes)

{
	size_t ns_read_sum;
	int n = indexes.size();
	FILE *fp;
	string fname;
	if(dir.length()>0)
	  /* for expected context for use in python we will assume dir does not
		have a trailing path separator so we always insert / */
		fname = dir + "/" + dfile;
	else
	  /* Null as always in unix means use current directory*/
		fname=dfile;
	if((fp=fopen(fname.c_str(),"r")) == NULL) {
		de.kill();
		stringstream ss;
		ss << "can not open file in " << fname << endl;
		de.elog.log_error(ss.str());
		return -1;
	}
	for (int ind = 0; ind < n; ++ind) {
		size_t ns_read;
		int i = indexes[ind];
		long int foff;
		if (de.member[i].is_defined(SEISMICMD_foff))
		{
		  foff = de.member[i].get_long(SEISMICMD_foff);
		}
		else
		{
		  de.member[i].kill();
		  stringstream ss;
		  ss << "foff not defined for " << i << " member in ensemble" << endl;
		  de.member[i].elog.log_error(ss.str());
		  continue;
		}
		try{
			if(fseek(fp,foff,SEEK_SET))
			{
				fclose(fp);
				de.member[i].kill();
				stringstream ss;
		  		ss << "can not fseek in " << foff << endl;
				de.member[i].elog.log_error(ss.str());
				continue;
			}
			ns_read = fread((void*)(&(de.member[i].s[0])), sizeof(double), de.member[i].npts(), fp);
			if (ns_read != de.member[i].npts())
			{
				de.member[i].elog.log_error(string("read error: npts not equal"));
				de.member[i].kill();
			}
			else
			{
				de.member[i].set_live();
			}
			ns_read_sum += ns_read;
		}catch(...){throw;};
	}
	fclose(fp);
	de.set_live();
	return ns_read_sum;
}
} // Termination of namespace definitions
