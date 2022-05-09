#include <memory>
#include <vector>
#include <stdio.h>
#include <string>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/keywords.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/io/fileio.h"
namespace mspass::io
{
//using namespace mspass::io;
using namespace mspass::seismic;
using mspass::utility::MsPASSError;
using mspass::utility::ErrorSeverity;
using namespace std;
/* This is a file scope function to allow the overlaoded fwrite_to_file
functions to share this common code to write a contiguous buffer of data.
to a file.

Note I considered adding an old school call to flock to assure this
function would be thread safe.   However, the man page for the posix
function flockfile says that is no longer necessary and stdio is
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
			throw MsPASSError("fwrite_to_file:  fwrite error to file "+fname,ErrorSeverity::Invalid);
		}
		fclose(fp);
		return foff;
	}catch(...){throw;};
}
long int fwrite_to_file(TimeSeries& d,const string dir,const string dfile)
{
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
long int fwrite_to_file(Seismogram& d, const string dir,const string dfile)
{
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
} // Termination of namespace definitions
