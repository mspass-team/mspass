#include "sstream"
#include <math.h>
#include "misc/blas.h"
#include "mspass/algorithms/Butterworth.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
namespace mspass::algorithms
{
using mspass::seismic::CoreTimeSeries;
using mspass::seismic::CoreSeismogram;
using mspass::seismic::TimeReferenceType;
using mspass::utility::Metadata;
using mspass::utility::MsPASSError;
using mspass::utility::ErrorSeverity;
using namespace mspass::algorithms::deconvolution;

using namespace std;
Butterworth::Butterworth()
{
	/* This is a translation of a minimum phase antialias filter used in
	the antialias function in seismic unix.  We define it as a default
	because it does almost nothing - almost because it depends upon
	form of antialias applied to get to current state.  */
	double fnyq,fpass,apass,fstop,astop;
	fnyq = 0.5;
	fpass = 0.6*fnyq;
	apass = 0.99;
	fstop = fnyq;
	astop = 0.01;
	this->bfdesign(fpass,apass,fstop,astop,
			&(this->npoles_hi),&(this->f3db_hi));
	dt=1.0;
	use_lo=false;
	use_hi=true;
	f3db_lo=0.0;
	npoles_lo=0;
	zerophase=false;
};
Butterworth::Butterworth(const bool zp,
	const bool lcon, const bool hcon,
		const double fstoplo, const double astoplo,
			const double fpasslo, const double apasslo,
				const double fpasshi, const double apasshi,
					const double fstophi, const double astophi,
                                            const double sample_interval)
{
	dt=sample_interval;
	zerophase=zp;
	use_lo=lcon;
	use_hi=hcon;
	this->bfdesign(fpasslo*dt,apasslo,fstoplo*dt,astoplo,
					&(this->npoles_lo),&(this->f3db_lo));
	this->bfdesign(fpasshi*dt,apasshi,fstophi*dt,astophi,
					&(this->npoles_hi),&(this->f3db_hi));
};
Butterworth::Butterworth(const bool zero, const bool locut, const bool hicut,
	 const int npolelo, const double f3dblo,
		const int npolehi, const double f3dbhi,
			const double sample_interval)
{
	this->dt=sample_interval;
	this->zerophase=zero;
	this->use_lo=locut;
	this->use_hi=hicut;
	this->f3db_hi=f3dbhi;
	this->f3db_lo=f3dblo;
	this->npoles_lo=npolelo;
	this->npoles_hi=npolehi;
	/* Convert the frequencies to nondimensional form */
	this->f3db_lo *= dt;
	this->f3db_hi *= dt;
}
	/*! Construct using tagged valus created from a Metadata container*/
Butterworth::Butterworth(const Metadata& md)
{
	string base_error("Butterworth pf constructor:  ");
	try{
		dt=md.get<double>("sample_interval");
		/* We define these here even if they aren't all used for all options.
		Better for tiny memory cost than declaration in each block */
		double fpass, apass, fstop, astop;
		zerophase=md.get_bool("zerophase");
		/* These options simplify setup but complicate this constructor.  */
		string ftype,fdmeth;
		ftype=md.get_string("filter_type");
		fdmeth=md.get_string("filter_definition_method");
		if(ftype=="bandpass")
		{
			use_lo=true;
			use_hi=true;
			if(fdmeth=="corner_pole")
			{
				/* Note we allow flow>figh to create strongly peaked filters although
				we don't scale these any any rational way */
				npoles_lo=md.get<int>("npoles_low");
				npoles_hi=md.get<int>("npoles_high");
				f3db_lo=md.get<double>("corner_low");
				f3db_hi=md.get<double>("corner_high");
				/* Stored as nondimensional internally*/
				f3db_lo *= dt;
				f3db_hi *= dt;
			}
			else
			{
				/* note the set routines will test validity of input and throw
				an error if the points do not define the right sense of pass versus cut*/
				fpass=md.get<double>("fpass_low");
				apass=md.get<double>("apass_low");
				fstop=md.get<double>("fstop_low");
				astop=md.get<double>("astop_low");
				this->set_lo(fpass,fstop,astop,apass);
				fpass=md.get<double>("fpass_high");
				apass=md.get<double>("apass_high");
				fstop=md.get<double>("fstop_high");
				astop=md.get<double>("astop_high");
				this->set_hi(fpass,fstop,apass,astop);
			}
		}
		else if(ftype=="lowpass")
		{
			/* Note the confusing naming hre - lo means lo corner not low pass*/
			use_lo=false;
			use_hi=true;
			/* Explicity initialization useful here */
			npoles_lo=0;
			f3db_lo=0.0;
			if(fdmeth=="corner_pole")
			{
				npoles_hi=md.get<int>("npoles_high");
				f3db_hi=md.get<double>("corner_high");
				f3db_hi *= dt;
			}
			else
			{
				fpass=md.get<double>("fpass_high");
				apass=md.get<double>("apass_high");
				fstop=md.get<double>("fstop_high");
				astop=md.get<double>("astop_high");
				/* note this handles inconsistencies and can throw an error */
				this->set_hi(fstop,fpass,astop,apass);
			}
		}
		else if(ftype=="highpass")
		{
			use_lo=true;
			use_hi=false;
			/* Explicity initialization useful here */
			npoles_hi=0;
			f3db_hi=0.0;
			if(fdmeth=="corner_pole")
			{
				npoles_hi=md.get<int>("npoles_low");
				f3db_hi=md.get<double>("corner_low");
				f3db_hi *= dt;
			}
			else
			{
				fpass=md.get<double>("fpass_low");
				apass=md.get<double>("apass_low");
				fstop=md.get<double>("fstop_low");
				astop=md.get<double>("astop_low");
				/* note this handles inconsistencies and can throw an error */
				this->set_lo(fstop,fpass,astop,apass);
			}
		}
		else if(ftype=="bandreject")
		{
			use_lo=true;
			use_hi=true;
			if(fdmeth=="corner_pole")
			{
				/* Note we allow flow>figh to create strongly peaked filters although
				we don't scale these any any rational way */
				npoles_lo=md.get<int>("npoles_low");
				npoles_hi=md.get<int>("npoles_high");
				f3db_lo=md.get<double>("corner_low");
				f3db_hi=md.get<double>("corner_high");
				/* Enforce this constraint or we don't have a band reject filtr */
				if(f3db_lo<f3db_hi)
				{
					throw MsPASSError(base_error+"Illegal corner frequencies for band reject filter definition"
				     + "For a band reject low corner must be larger than high corner (pass becomes cut to reject)",
				     ErrorSeverity::Invalid);
				}
				f3db_lo *=dt;
				f3db_hi *=dt;
			}
			else
			{
				/* We should test these values for defining band reject but bypass
				these safeties for now as I don't expect this feature to get much
				use. */
				fpass=md.get<double>("fpass_low");
				apass=md.get<double>("apass_low");
				fstop=md.get<double>("fstop_low");
				astop=md.get<double>("astop_low");
				this->bfdesign(fpass*dt,apass,fstop*dt,astop,&(this->npoles_lo),&(this->f3db_lo));
				fpass=md.get<double>("fpass_high");
				apass=md.get<double>("apass_high");
				fstop=md.get<double>("fstop_high");
				astop=md.get<double>("astop_high");
				this->bfdesign(fpass*dt,apass,fstop*dt,astop,&(this->npoles_hi),&(this->f3db_hi));
			}
		}
		else
		{
			throw MsPASSError(base_error+"Unsupported filtr_type="+ftype,
				ErrorSeverity::Invalid);
		}
	}catch(...){throw;};
}
Butterworth::Butterworth(const Butterworth& parent)
{
	use_lo=parent.use_lo;
	use_hi=parent.use_hi;
	zerophase=parent.zerophase;
	f3db_lo=parent.f3db_lo;
	f3db_hi=parent.f3db_hi;
	npoles_lo=parent.npoles_lo;
	npoles_hi=parent.npoles_hi;
	dt=parent.dt;
}
Butterworth& Butterworth::operator=(const Butterworth& parent)
{
	if(this != &parent)
	{
		use_lo=parent.use_lo;
		use_hi=parent.use_hi;
		zerophase=parent.zerophase;
		f3db_lo=parent.f3db_lo;
		f3db_hi=parent.f3db_hi;
		npoles_lo=parent.npoles_lo;
		npoles_hi=parent.npoles_hi;
		dt=parent.dt;
	}
	return *this;
}
CoreTimeSeries Butterworth::impulse_response(const int n)
{
	CoreTimeSeries result(n);
	/* We use a feature that the above constructor initiallizes the buffer
	to zeros so just a spike at n/2. */
	result.s[n/2]=1.0;
	result.set_t0( ((double)(-n/2))*(this->dt) );
	result.set_dt(this->dt);
	result.set_tref(TimeReferenceType::Relative);
	result.set_live();
	this->apply(result.s);
	return result;
}
/* Fraction of 1/dt used to cause disabling low pass (upper) corner*/
const double FHighFloor(0.45);  //90% of Nyquist
void Butterworth::apply(mspass::seismic::CoreTimeSeries& d)
{
	double d_dt=d.dt();
	if(this->dt != d_dt)
	{
		/* Here we throw an exception if a requested sample rate is
		illegal */
		double fhtest=d_dt/(this->dt);
		if(fhtest>FHighFloor)
		{
			stringstream ss;
			ss << "Butterworth::apply:  automatic dt change error"<<endl
			  << "Current operator dt="<<this->dt<<" data dt="<<d_dt<<endl
				<< "Change would produce a corner too close to Nyquist"
				<< " and create an unstable filter"<<endl
				<< "Use a different filter operator for data with this sample rate"
				<< endl;
			throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
		}
		this->change_dt(d_dt);
	}
	this->apply(d.s);
}
/* We use ErrorLogger and blunder on with TimeSeries objects instead of
throwing an exception when the upper corner is bad. */
void Butterworth::apply(mspass::seismic::TimeSeries& d)
{
	double d_dt=d.dt();
	if(this->dt != d_dt)
	{
		/* Here we throw an exception if a requested sample rate is
		illegal */
		double fhtest=d_dt/(this->dt);
		if(use_hi && (fhtest>FHighFloor))
		{
			/*In this case we temporarily disable the upper corner and
			cache the old dt to restore it before returning. */
			double olddt=this->dt;
			double flow_old=this->f3db_lo;
			use_hi=false;
			this->apply(d.s);
			use_hi=true;
			this->dt=olddt;
			this->f3db_lo=flow_old;
			stringstream ss;
			ss <<"Auto adjust for sample rate change error"<<endl
			  << "Upper corner of filter="<<this->high_corner()
			  << " is near or above Nyquist frequency for requested sample "
				<< "interval="<<this->dt<<endl
				<< "Disabling upper corner (lowpass) and applying filter anyway"
				<<endl;
			d.elog.log_error(string("Butterworth::apply"),
			          ss.str(),ErrorSeverity::Complaint);
			/* With this logic we have separate return here */
		}
		else
		{
			this->change_dt(d_dt);
		}
	}
	this->apply(d.s);
}
/* Reverse a vector of doubles in place using a pointer algorithm
for speed. */
void reverse_vector(int nd, double *d)
{
  double tmp;
  for(auto i=0;i<nd/2;++i)
  {
    tmp=*(d+i);
    *(d+i)=*(d+nd-i-1);
    *(d+nd-i-1)=tmp;
  }
}


/* This is a core method.  The other apply methods just call his one.
It depends on a property of seismic unix implementation of bw Filters
that allow input and output to be the same data vector*/
void Butterworth::apply(vector<double>& d)
{
	if(use_lo)
	{
		this->bflowcut(npoles_hi,f3db_hi,d.size(),&(d[0]),&(d[0]));
		if(zerophase)
		{
			reverse_vector(d.size(),&(d[0]));
			this->bflowcut(npoles_hi,f3db_hi,d.size(),&(d[0]),&(d[0]));
			reverse_vector(d.size(),&(d[0]));
		}
	}
	if(use_hi)
	{
		this->bfhighcut(npoles_lo,f3db_lo,d.size(),&(d[0]),&(d[0]));
		if(zerophase)
		{
			reverse_vector(d.size(),&(d[0]));
			this->bfhighcut(npoles_lo,f3db_lo,d.size(),&(d[0]),&(d[0]));
			reverse_vector(d.size(),&(d[0]));
		}
	}
}

void Butterworth::apply(CoreSeismogram& d)
{
	/* we could just handle a MsPASSError to take care of exceptions
	thrown by apply if the sample rate is illegal, but this is more adaptable. */
	try{
		double d_dt=d.dt();
		if(this->dt != d_dt) this->change_dt(d_dt);
		/* We copy each component to this buffer, filter, and then copy
		back.  Not as efficient as if we used a strike parameter in the filter
		functions, but I did not want to rewrite those functions. */
		vector<double> comp;
		int npts=d.npts();
		comp.reserve(npts);
		/* This initializes the buffer to allow us a simpler loop from 0 to 2
		using the blas function dcopy to handle the skip for a fortran style
		array used for storing the 3c data in  u*/
		for(auto i=0;i<npts;++i)comp.push_back(0.0);
		for(auto k=0;k<3;++k)
		{
			dcopy(npts,d.u.get_address(k,0),3,&(comp[0]),1);
			this->apply(comp);
			dcopy(npts,&(comp[0]),1,d.u.get_address(k,0),3);
		}
	}catch(...){throw;};
}
/* The logic used here is identical to the apply method for TimeSeries to
log errors and with an internal return.  Difference is the need to handle
3 components. */
void Butterworth::apply(mspass::seismic::Seismogram& d)
{


	double d_dt=d.dt();
	if(this->dt != d_dt)
	{
		/* Here we throw an exception if a requested sample rate is
		illegal */
		double fhtest=d_dt/(this->dt);
		if(use_hi && (fhtest>FHighFloor))
		{
			/*In this case we temporarily disable the upper corner and
			cache the old dt to restore it before returning. */
			double olddt=this->dt;
			double flow_old=this->f3db_lo;
			use_hi=false;
			this->apply(d);
			use_hi=true;
			this->dt=olddt;
			this->f3db_lo=flow_old;
			stringstream ss;
			ss <<"Auto adjust for sample rate change error"<<endl
			  << "Upper corner of filter="<<this->high_corner()
			  << " is near or above Nyquist frequency for requested sample "
				<< "interval="<<this->dt<<endl
				<< "Disabling upper corner (lowpass) and applying filter anyway"
				<<endl;
			d.elog.log_error(string("Butterworth::apply"),
			   ss.str(),ErrorSeverity::Complaint);
			/* With this logic we have separate return here */
		}
		else
		{
			this->change_dt(d_dt);
		}
	}
	vector<double> comp;
	int npts=d.npts();
	comp.reserve(npts);
	/* This initializes the buffer to allow us a simpler loop from 0 to 2
	using the blas function dcopy to handle the skip for a fortran style
	array used for storing the 3c data in  u*/
	for(auto i=0;i<npts;++i)comp.push_back(0.0);
	for(auto k=0;k<3;++k)
	{
		dcopy(npts,d.u.get_address(k,0),3,&(comp[0]),1);
		this->apply(comp);
		dcopy(npts,&(comp[0]),1,d.u.get_address(k,0),3);
	}
}
ComplexArray Butterworth::transfer_function(const int nfft)
{
	CoreTimeSeries imp=this->impulse_response(nfft);
	/* the impulse response function uses a time shift and sets t0 to the
	required position.  A disconnect with any fft is that will introduce an
	undesirable phase shift for many algorithms.  We remove it here using our
	circular shift function */
	int ishift=imp.sample_number(0.0);
	imp.s=circular_shift(imp.s,ishift);
	gsl_fft_complex_wavetable *wavetable = gsl_fft_complex_wavetable_alloc (nfft);
	gsl_fft_complex_workspace *workspace = gsl_fft_complex_workspace_alloc (nfft);
	ComplexArray work(nfft,imp.s);
	gsl_fft_complex_forward(work.ptr(), 1, nfft, wavetable, workspace);
	gsl_fft_complex_wavetable_free (wavetable);
	gsl_fft_complex_workspace_free (workspace);
	return work;
}
/* the next 3 functions are nearly idenitical to C code with the same name
sans the Butterworth class tag. The only change is float was changed to
double.  All methods below here are private*/

void Butterworth::bfdesign (double fpass, double apass, double fstop, double astop,
	int *npoles, double *f3db)
{
	double wpass,wstop,fnpoles,w3db;

	/* warp frequencies according to bilinear transform */
	wpass = 2.0*tan(M_PI*fpass);
	wstop = 2.0*tan(M_PI*fstop);

	/* if lowpass filter, then */
	if (fstop>fpass) {
		fnpoles = log((1.0/(apass*apass)-1.0)/(1.0/(astop*astop)-1.0))
			/ log(pow(wpass/wstop,2.0));
		w3db = wpass/pow((1.0/(apass*apass)-1.0),0.5/fnpoles);

	/* else, if highpass filter, then */
	} else {
		fnpoles = log((1.0/(apass*apass)-1.0)/(1.0/(astop*astop)-1.0))
			/ log(pow(wstop/wpass,2.0));
		w3db = wpass*pow((1.0/(apass*apass)-1.0),0.5/fnpoles);
	}

	/* determine integer number of poles */
	*npoles = 1+(int)fnpoles;

	/* determine (unwarped) -3 db frequency */
	*f3db = atan(0.5*w3db)/M_PI;
}

void Butterworth::bflowcut (int npoles, double f3db, int n, double p[], double q[])
{
	int jpair,j;
	double r,scale,theta,a,b1,b2,pj,pjm1,pjm2,qjm1,qjm2;

	r = 2.0*tan(M_PI*fabs(f3db));
	if (npoles%2!=0) {
		scale = r+2.0;
		a = 2.0/scale;
		b1 = (r-2.0)/scale;
		pj = 0.0;
		qjm1 = 0.0;
		for (j=0; j<n; j++) {
			pjm1 = pj;
			pj = p[j];
			q[j] = a*(pj-pjm1)-b1*qjm1;
			qjm1 = q[j];
		}
	} else {
		for (j=0; j<n; j++)
			q[j] = p[j];
	}
	for (jpair=0; jpair<npoles/2; jpair++) {
		theta = M_PI*(2*jpair+1)/(2*npoles);
		scale = 4.0+4.0*r*sin(theta)+r*r;
		a = 4.0/scale;
		b1 = (2.0*r*r-8.0)/scale;
		b2 = (4.0-4.0*r*sin(theta)+r*r)/scale;
		pjm1 = 0.0;
		pj = 0.0;
		qjm2 = 0.0;
		qjm1 = 0.0;
		for (j=0; j<n; j++) {
			pjm2 = pjm1;
			pjm1 = pj;
			pj = q[j];
			q[j] = a*(pj-2.0*pjm1+pjm2)-b1*qjm1-b2*qjm2;
			qjm2 = qjm1;
			qjm1 = q[j];
		}
	}
}

void Butterworth::bfhighcut (int npoles, double f3db, int n, double p[], double q[])
{
	int jpair,j;
	double r,scale,theta,a,b1,b2,pj,pjm1,pjm2,qjm1,qjm2;

	r = 2.0*tan(M_PI*fabs(f3db));
	if (npoles%2!=0) {
		scale = r+2.0;
		a = r/scale;
		b1 = (r-2.0)/scale;
		pj = 0.0;
		qjm1 = 0.0;
		for (j=0; j<n; j++) {
			pjm1 = pj;
			pj = p[j];
			q[j] = a*(pj+pjm1)-b1*qjm1;
			qjm1 = q[j];
		}
	} else {
		for (j=0; j<n; j++)
			q[j] = p[j];
	}
	for (jpair=0; jpair<npoles/2; jpair++) {
		theta = M_PI*(2*jpair+1)/(2*npoles);
		scale = 4.0+4.0*r*sin(theta)+r*r;
		a = r*r/scale;
		b1 = (2.0*r*r-8.0)/scale;
		b2 = (4.0-4.0*r*sin(theta)+r*r)/scale;
		pjm1 = 0.0;
		pj = 0.0;
		qjm2 = 0.0;
		qjm1 = 0.0;
		for (j=0; j<n; j++) {
			pjm2 = pjm1;
			pjm1 = pj;
			pj = q[j];
			q[j] = a*(pj+2.0*pjm1+pjm2)-b1*qjm1-b2*qjm2;
			qjm2 = qjm1;
			qjm1 = q[j];
		}
	}
}
void Butterworth::set_lo(const double fstop, const double fpass,
	const double astop, const double apass)
{
		string base_error("Butterworth::set_lo:  ");
		if(fstop>fpass)
		{
			stringstream ss;
			ss << base_error << "Illegal input for frequency points f_stop="
			<<fstop<<" f_pass="<<fpass<<endl
			<<"fstop frequency must be < fpass frequency"<<endl;
			throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
		}
		if((astop<0.0) || (apass<0.0))
		{
			stringstream ss;
			ss << base_error << "Illegal amplitude values of astop="<<astop
			   << " and apass="<<apass<<endl<<"Amplitudes must be nonnegative"<<endl;
			throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
		}
		this->bfdesign(fpass*(this->dt),apass,fstop*(this->dt),astop,
		     &this->npoles_lo,&this->f3db_lo);
};
void Butterworth::set_hi(const double fstop, const double fpass,
	const double astop, const double apass)
{
	string base_error("Butterworth::set_hi:  ");
	if(fpass>fstop)
	{
		stringstream ss;
		ss << base_error << "Illegal input for frequency points f_stop="
		<<fstop<<" f_pass="<<fpass<<endl
		<<"fstop frequency must be > fpass frequency"<<endl;
		throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
	}
	if((astop<0.0) || (apass<0.0))
	{
		stringstream ss;
		ss << base_error << "Illegal amplitude values of astop="<<astop
			 << " and apass="<<apass<<endl<<"Amplitudes must be nonnegative"<<endl;
		throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
	}
	this->bfdesign(fpass*(this->dt),apass,fstop*(this->dt),astop,
			 &this->npoles_hi,&this->f3db_hi);
};
}  // end namespace
