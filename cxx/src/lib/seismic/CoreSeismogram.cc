#include <float.h>
#include <math.h>
#include <sstream>
#include "misc/blas.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/SphericalCoordinate.h"

namespace mspass
{
using namespace mspass;
/*
 *  Start with all the constructors.
 *
*/
//
// Default constructor for CoreSeismogram could be
// done inline in seispp.h, but it is complication enough I put
// it here
//
CoreSeismogram::CoreSeismogram() : BasicTimeSeries(),Metadata(),u(0,0)
{
  /* mlive and tref are set in BasicTimeSeries so we don't use putters for
  them here.   These three initialize Metadata properly for these attributes*/
    this->set_dt(0.0);
    this->set_t0(0.0);
    this->set_npts(0);
    components_are_orthogonal=true;
    components_are_cardinal=true;
    for(int i=0; i<3; ++i)
        for(int j=0; j<3; ++j)
            if(i==j)
                tmatrix[i][i]=1.0;
            else
                tmatrix[i][j]=0.0;
}
CoreSeismogram::CoreSeismogram(size_t nsamples)
    : BasicTimeSeries(),Metadata(),u(3,nsamples)
{
  this->set_dt(0.0);
  this->set_t0(0.0);
  this->set_npts(0);
  components_are_orthogonal=true;
  components_are_cardinal=true;
  for(int i=0; i<3; ++i)
    for(int j=0; j<3; ++j)
      if(i==j)
        tmatrix[i][i]=1.0;
      else
        tmatrix[i][j]=0.0;
}

CoreSeismogram::CoreSeismogram(const CoreSeismogram& t3c) :
    BasicTimeSeries(dynamic_cast<const BasicTimeSeries&>(t3c)),
    Metadata(dynamic_cast<const Metadata&>(t3c)),
    u(t3c.u)
{
    int i,j;
    components_are_orthogonal=t3c.components_are_orthogonal;
    components_are_cardinal=t3c.components_are_cardinal;
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j) tmatrix[i][j]=t3c.tmatrix[i][j];
}
bool CoreSeismogram::tmatrix_is_cardinal()
{
    /* Test for 0 or 1 to 5 figures - safe but conservative for
       float input although tmatrix is double */
    double scale(10000.0);
    int itest(10000);
    int i,j;
    for(i=0;i<3;++i)
    {
        for(j=0;j<3;++j)
        {
            int ival=static_cast<int>(tmatrix[i][j]*scale);
            if(i==j)
            {
                if(ival!=itest) return false;
            }
            else
            {
                if(ival!=0) return false;
            }
        }
    }
    return true;
}
CoreSeismogram::CoreSeismogram(const Metadata& md,
                const bool load_data) : Metadata(md)
{
    string dfile, dir;
    long foff;
    FILE *fp;
    double *inbuffer;

    components_are_orthogonal=true;
    mlive=false;
    try {
        /* Names used are from mspass defintions as of Jan 2020.
        We don't need to call the set methods for these attributes as they
        would add the overhead of setting delta, startime, and npts to the
        same value passed. */
        this->mdt = this->get_double("delta");
        this->mt0 = this->get_double("starttime");
        this->nsamp = this->get_long("npts");
        /* Assume the data t0 is UTC. */
        this->set_tref(TimeReferenceType::UTC);
        tmatrix[0][0]=this->get_double("U11");
        tmatrix[1][0]=this->get_double("U21");
        tmatrix[2][0]=this->get_double("U31");
        tmatrix[0][1]=this->get_double("U12");
        tmatrix[1][1]=this->get_double("U22");
        tmatrix[2][1]=this->get_double("U32");
        tmatrix[0][2]=this->get_double("U13");
        tmatrix[1][2]=this->get_double("U23");
        tmatrix[2][2]=this->get_double("U33");
        components_are_cardinal=this->tmatrix_is_cardinal();
        if(components_are_cardinal)
          components_are_orthogonal=true;
        else
          components_are_orthogonal=false;  //May be wrong but cost is tiny
        u=dmatrix(3,nsamp);
        if(load_data)
        {
            dir = this->get_string("dir");
            dfile = this->get_string("dfile");
            foff = this->get_long("foff");
            string fname=dir+"/"+dfile;
            if((fp=fopen(fname.c_str(),"r")) == NULL)
                throw(MsPASSError(string("Open failure for file ")+fname,
					             ErrorSeverity::Invalid));
            if (foff>0)fseek(fp,foff,SEEK_SET);
            /* The older seispp code allowed byte swapping here.   For
            efficiency we don't support that here and assume can do a
            raw fread from the file and get valid data.  If support for
            other types is needed this will need to be extended.  Here
            we just point fread at the internal u array. */
            inbuffer = this->u.get_address(0,0);
            unsigned int nt=3*this->nsamp;
            if(fread((void *)(inbuffer),sizeof(double),nt,fp)
                    != nt )
            {
                throw(MsPASSError(string("CoreSeismogram constructor:  fread error on file ")+fname,
                      ErrorSeverity::Invalid));
            }
            fclose(fp);
	          mlive=true;
	    }
        else
        {
          /* Initialize the matrix in this case but leave the object marked
          as dead */
          this->u.zero();
        }
    }
    catch (...)
    {
      throw;
    };
}

CoreSeismogram::CoreSeismogram(const vector<CoreTimeSeries>& ts,
                       const unsigned int component_to_clone)
    : BasicTimeSeries(dynamic_cast<const BasicTimeSeries&>(ts[component_to_clone])),
     Metadata(dynamic_cast<const Metadata&>(ts[component_to_clone])),
      u()
{
    const string base_error("CoreSeismogram constructor from 3 Time Series:  ");
    size_t i,j;
    /* This is needed in case nsamp does not match s.size(0) */
    size_t nstest = ts[component_to_clone].s.size();
    if(nsamp!=nstest) this->nsamp=nstest;
    /* this method allocates u and sets the proper metadata for npts*/
    this->set_npts(this->nsamp);
    /* beware irregular sample rates, but don' be too machevelian.
           Abort only if the mismatch is large defined as accumulated time
           over data range of this constructor is less than half a sample */
    if( (ts[0].dt()!=ts[1].dt()) || (ts[1].dt()!=ts[2].dt()) )
    {
        double ddtmag1=fabs(ts[0].dt()-ts[1].dt());
        double ddtmag2=fabs(ts[1].dt()-ts[2].dt());
        double ddtmag;
        if(ddtmag1>ddtmag1)
            ddtmag=ddtmag1;
        else
            ddtmag=ddtmag2;
        ddtmag1=fabs(ts[0].dt()-ts[2].dt());
        if(ddtmag1>ddtmag)  ddtmag=ddtmag1;
        double ddtcum=ddtmag*((double)ts[0].s.size());
        if(ddtcum>(ts[0].dt())/2.0)
        {
            stringstream ss;
            ss << base_error
               << "Sample intervals of components are not consistent"<<endl;
            for(int ie=0; ie<3; ++ie) ss << "Component "<<ie<<" dt="<<ts[ie].dt()<<" ";
            ss<<endl;
            throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
        }
    }
    // temporaries to hold component values
    double t0_component[3];
    double hang[3];
    double vang[3];
    // Load up these temporary arrays inside this try block and arrange to
    // throw an exception if required metadata are missing
    try {
        // WARNING hang and vang attributes stored in metadata
        // always assumed to be radians
        hang[0]=ts[0].get_double("hang");
        hang[1]=ts[1].get_double("hang");
        hang[2]=ts[2].get_double("hang");
        vang[0]=ts[0].get_double("vang");
        vang[1]=ts[1].get_double("vang");
        vang[2]=ts[2].get_double("vang");
    } catch (MetadataGetError& mde)
    {
        stringstream ss;
        ss << base_error
           << "missing hang or vang variable in component TimeSeries objects received"<<endl;
        ss << "Message posted by Metadata::get_double:  "<<mde.what()<<endl;
    }
    // These are loaded just for convenience
    t0_component[0]=ts[0].t0();
    t0_component[1]=ts[1].t0();
    t0_component[2]=ts[2].t0();

    // Treat the normal case specially and avoid a bunch of work unless
    // it is required
    if( (ts[0].s.size()==ts[1].s.size()) && (ts[1].s.size()==ts[2].s.size())
            && (fabs( (t0_component[0]-t0_component[1])/dt() )<1.0)
            && (fabs( (t0_component[1]-t0_component[2])/dt() )<1.0))
    {
        /* Older code had this.   No longer needed with logic above that
        calls set_npts.  that method creates and initialized the u dmatrix*/
        //this->u=dmatrix(3,nsamp);
        // Load data by a simple copy operation
        /* This is a simple loop version
        for(j=0;j<nsamp;++nsamp)
        {
        	this->u(0,j)=ts[0].s[j];
        	this->u(1,j)=ts[1].s[j];
        	this->u(2,j)=ts[2].s[j];
        }
        */
        // This is a vector version that I'll use because it will
        // be faster albeit infinitely more obscure and
        // intrinsically more dangerous
        dcopy(nsamp,&(ts[0].s[0]),1,u.get_address(0,0),3);
        dcopy(nsamp,&(ts[1].s[0]),1,u.get_address(1,0),3);
        dcopy(nsamp,&(ts[2].s[0]),1,u.get_address(2,0),3);
    }
    else
    {
        /*Land here if the start time or number of samples
        is irregular.  We cut the output to latest start time to earliest end time*/
        /* WARNING - debugging may be needed for this block. SEISPP versio of this
        used gaps.  Here we cut the output to match an irregularities. */
        double tsmax,temin;
        tsmax=max(t0_component[0],t0_component[1]);
        tsmax=max(tsmax,t0_component[2]);
        temin=min(ts[0].endtime(),ts[1].endtime());
        temin=min(temin,ts[2].endtime());
        nstest=round((temin-tsmax)/mdt);
        if(nsamp<=0)
          throw MsPASSError(base_error
                +"Irregular time windows of components have no overlap",
                        ErrorSeverity::Invalid);
        else
          this->set_npts(nstest);
        // Now load the data.  Use the time and sample number methods
        // to simplify process
        double t;
        t=tsmax;
        this->set_t0(t);
        double delta=this->dt();
        for(int ic=0; ic<3; ++ic)
        {
            for(j=0; j<ts[ic].s.size(); ++j)
            {
                i=ts[ic].sample_number(t);
                // silently do nothing if outside bounds.  This
                // perhaps should be an error as it shouldn't really
                // happen with the above algorithm, but safety is good
                if( (i>=0) && (i<nsamp) ) this->u(ic,j)=ts[ic].s[i];
                t += delta;
            }
        }
    }
    /* Finally we need to set the transformation matrix.
     This is a direct application of conversion of routines
    in spherical coordinate procedures.  They are procedural
    routines, not objects so the code is procedural.
    */
    SphericalCoordinate scor;
    double *nu;
    // convert all the hang values to spherical coordinate phi
    // (angle from postive east) from input assumed in degrees
    // azimuth from north.  At the same time convert vang to radians.
    for(i=0; i<3; ++i)
    {
        hang[i]=mspass::rad(90.0-hang[i]);
        vang[i]=mspass::rad(vang[i]);
    }
    for(i=0; i<3; ++i)
    {
        scor.phi=hang[i];
        scor.theta=vang[i];
        nu=SphericalToUnitVector(scor);
        for(j=0; j<3; ++j)tmatrix[i][j]=nu[j];
        delete [] nu;
    }
    components_are_cardinal = this->tmatrix_is_cardinal();
    if(components_are_cardinal)
        components_are_orthogonal=true;
    else
        components_are_orthogonal=false;
}
// Note on usage in this group of functions.  The rotation algorithms used here
// all key on the BLAS for speed.  That is, a transformation matrix could be done
// by using the * operator between matrix objects.

void CoreSeismogram::rotate_to_standard()
{
    if( (u.size()[1]<=0) || this->dead()) return; // do nothing in these situations
    double *work[3];
    int i,j;
    if(components_are_cardinal) return;
    /* We assume nsamp is the number of samples = number of columns in u - we don't
    check here for efficiency */
    for(j=0; j<3; ++j) work[j]=new double[nsamp];
    if(components_are_orthogonal)
    {
        //
        //Use a daxpy algorithm.  tmatrix stores the
        //forward transformation used to get current
        //Use the transpose to get back
        //
        for(i=0; i<3; ++i)
        {
            // x has a stride of 3 because we store in fortran order in x
            dcopy(nsamp,u.get_address(0,0),3,work[i],1);
            dscal(nsamp,tmatrix[0][i],work[i],1);
            daxpy(nsamp,tmatrix[1][i],u.get_address(1,0),3,work[i],1);
            daxpy(nsamp,tmatrix[2][i],u.get_address(2,0),3,work[i],1);
        }
        for(i=0; i<3; ++i) dcopy(nsamp,work[i],1,u.get_address(i,0),3);
    }
    else
    {
        //
        //Enter here only when the transformation matrix is
        //not orthogonal.  We have to construct a fortran
        //order matrix a to use LINPACK routine in sunperf/perf
        //This could be done with the matrix template library
        //but the overhead ain't worth it
        //
        double a[9];
        int ipivot[3];
        int info;
        a[0] = tmatrix[0][0];
        a[1] = tmatrix[1][0];
        a[2] = tmatrix[2][0];
        a[3] = tmatrix[0][1];
        a[4] = tmatrix[1][1];
        a[5] = tmatrix[2][1];
        a[6] = tmatrix[0][2];
        a[7] = tmatrix[1][2];
        a[8] = tmatrix[2][2];
        //LAPACK routine with FORTRAN interface using pass by reference and pointers
        int three(3);
        dgetrf(three,three,a,three,ipivot,info);
        if(info!=0)
        {
            for(i=0; i<3; ++i) delete [] work[i];
            throw(MsPASSError(
                      string("rotate_to_standard:  LU factorization of transformation matrix failed")),
                  ErrorSeverity::Invalid);
        }
        // inversion routine after factorization from lapack FORT$RAN interface
        double awork[10];  //Larger than required but safety value small cost
        int ldwork(10);
        dgetri(three,a,three,ipivot,awork,ldwork,info);
        // This is the openblas version
        //info=LAPACKE_dgetri(LAPACK_COL_MAJOR,3,a,3,ipivot);
        if(info!=0)
        {
            for(i=0; i<3; ++i) delete [] work[i];
            throw(MsPASSError(
                      string("rotate_to_standard:  LU factorization inversion of transformation matrix failed")),
                  ErrorSeverity::Invalid);
        }

        tmatrix[0][0] = a[0];
        tmatrix[1][0] = a[1];
        tmatrix[2][0] = a[2];
        tmatrix[0][1] = a[3];
        tmatrix[1][1] = a[4];
        tmatrix[2][1] = a[5];
        tmatrix[0][2] = a[6];
        tmatrix[1][2] = a[7];
        tmatrix[2][2] = a[8];
        /* The inverse is now in tmatrix so we reverse the
           rows and columms from above loop */

        for(i=0; i<3; ++i)
        {
            dcopy(nsamp,u.get_address(0,0),3,work[i],1);
            dscal(nsamp,tmatrix[i][0],work[i],1);
            daxpy(nsamp,tmatrix[i][1],u.get_address(1,0),3,work[i],1);
            daxpy(nsamp,tmatrix[i][2],u.get_address(2,0),3,work[i],1);
        }
        for(i=0; i<3; ++i) dcopy(nsamp,work[i],1,u.get_address(i,0),3);
        components_are_orthogonal = true;
    }
    //
    //Have to set the transformation matrix to an identity now
    //
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j)
            if(i==j)
                tmatrix[i][i]=1.0;
            else
                tmatrix[i][j]=0.0;

    components_are_cardinal=true;
    for(i=0; i<3; ++i) delete [] work[i];
}


/* This routine takes a spherical coordinate vector that defines
a given direction in space and returns a transformation matrix that
should be viewed as a transformation to ray coordinates under an
assumption that this vector points in the direction of P wave
particle motion.  If the theta angle is greater than PI/2 it
switches the azimuth by 180 degrees so that the direction of the
transformed x1 axis will be pointing upward in space.  This removes
ambiguities in the transformation that make it easier to sort out
handedness of the transformation.

The transformation produced for a P wave will be true ray coordinates
with X1 = transverse, X2 = radial, and X3 = longitudinal.
The best way to understand the transformation is as a pair of
rotations:  (1) rotate North to radial about z, (2) rotate z to
transverse around X1 (transverse).  Note this leaves X1 (transverse)
always as a purely horizontal direction.  It should also work for a
principal component direction determined for an S phase, but the
appropriate the only component that will make any sense after the
transformation, in that case, is the X3 direction = direction of
inferred peak particle motion.

One special case has to be dealt with.  If the direction passed into
the program is purely vertical (up or down), the function can only
return an identity matrix because there is no way to determine a
horizontal rotation direction.

Arguments:
	xsc - spherical coordinate structure defining unit vector used
		to define the transform (radius is ignored).  Angles
		are assumed in radians.

Author:  Gary L. Pavlis
Written:  Sept. 1999
Modified:  Feb 2003
Original was plain C.  Adapted to C++ for seismic processing
*/
void CoreSeismogram::rotate(SphericalCoordinate& xsc)
{
    if( (u.size()[1]<=0) || dead()) return; // do nothing in these situations

    //Earlier version had a reset of the nsamp variable here - we need to trust
    //that is correct here for efficiency.  We the new API it would be hard
    //to have that happen. without a serious blunder
    int i;
    double theta, phi;  /* corrected angles after dealing with signs */
    double a,b,c,d;

    //
    //Undo any previous transformations
    //
    this->rotate_to_standard();
    if(xsc.theta == M_PI)
    {
        //This will be left handed
        tmatrix[2][2] = -1.0;
        dscal(nsamp,-1.0,u.get_address(2,0),3);
        return;
    }

    if(xsc.theta < 0.0)
    {
        theta = -(xsc.theta);
        phi = xsc.phi + M_PI;
        if(phi > M_PI) phi -= (2.0*M_PI);
    }
    else if(xsc.theta > M_PI)
    {
        theta = xsc.theta - M_PI;
        phi = xsc.phi + M_PI;
        if(phi > M_PI) phi -= (2.0*M_PI);
    }
    else
    {
        theta = xsc.theta;
        phi = xsc.phi;
    }
    /* Am using a formula here for azimuth with is pi/2 - phi*/
    double azimuth=M_PI_2-phi;
    a = cos(azimuth);
    b = sin(azimuth);
    c = cos(theta);
    d = sin(theta);

    tmatrix[0][0] = a;
    tmatrix[1][0] = b*c;
    tmatrix[2][0] = b*d;
    tmatrix[0][1] = -b;
    tmatrix[1][1] = a*c;
    tmatrix[2][1] = a*d;
    tmatrix[0][2] = 0.0;
    tmatrix[1][2] = -d;
    tmatrix[2][2] = c;

    /* Now multiply the data by this transformation matrix.  */
    double *work[3];
    for(i=0; i<3; ++i)work[i] = new double[nsamp];
    for(i=0; i<3; ++i)
    {
        dcopy(nsamp,u.get_address(0,0),3,work[i],1);
        dscal(nsamp,tmatrix[i][0],work[i],1);
        daxpy(nsamp,tmatrix[i][1],u.get_address(1,0),3,work[i],1);
        daxpy(nsamp,tmatrix[i][2],u.get_address(2,0),3,work[i],1);
    }
    for(i=0; i<3; ++i) dcopy(nsamp,work[i],1,u.get_address(i,0),3);
    components_are_cardinal=false;
    for(i=0; i<3; ++i) delete [] work[i];
}
void CoreSeismogram::rotate(const double nu[3])
{
    if( (u.size()[1]<=0) || this->dead()) return; // do nothing in these situations
    SphericalCoordinate xsc=UnitVectorToSpherical(nu);
    this->rotate(xsc);
}
/* simplified procedure to rotate only zonal angle by phi radians.
 Similar to above but using only azimuth angle AND doing a simple
 rotation in the horizontal plane.  Efficient algorithm only
 alters 0 and 1 components. */
void CoreSeismogram::rotate(double phi)
{
    if( (u.size()[1]<=0) || dead()) return; // do nothing in these situations
    int i,j,k;
    double a,b;
    a=cos(phi);
    b=sin(phi);
    double tmnew[3][3];
    tmnew[0][0] = a;
    tmnew[1][0] = b;
    tmnew[2][0] = 0.0;
    tmnew[0][1] = -b;
    tmnew[1][1] = a;
    tmnew[2][1] = 0.0;
    tmnew[0][2] = 0.0;
    tmnew[1][2] = 0.0;
    tmnew[2][2] = 1.0;

    /* Now multiply the data by this transformation matrix.
     Not trick in this i only goes to 2 because 3 component
     is an identity.*/
    double *work[2];
    for(i=0; i<2; ++i)work[i] = new double[nsamp];
    for(i=0; i<2; ++i)
    {
        dcopy(nsamp,u.get_address(0,0),3,work[i],1);
        dscal(nsamp,tmnew[i][0],work[i],1);
        daxpy(nsamp,tmnew[i][1],u.get_address(1,0),3,work[i],1);
    }
    for(i=0; i<2; ++i) dcopy(nsamp,work[i],1,u.get_address(i,0),3);
    double tm_tmp[3][3];
    double prod;
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j)
        {
            for(prod=0.0,k=0; k<3; ++k)
                prod+=tmnew[i][k]*tmatrix[k][j];
            tm_tmp[i][j]=prod;
        }
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j)tmatrix[i][j]=tm_tmp[i][j];
    components_are_cardinal=false;
    for(i=0; i<2; ++i) delete [] work[i];
}
void CoreSeismogram::transform(const double a[3][3])
{
    if( (u.size()[1]<=0) || dead()) return; // do nothing in these situations
    /* Older version had this - we need to trust ns is already u.columns().  */
    //size_t ns = u.size()[1];
    size_t i,j,k;
    double *work[3];
    for(i=0; i<3; ++i) work[i] = new double[nsamp];
    for(i=0; i<3; ++i)
    {
        dcopy(nsamp,u.get_address(0,0),3,work[i],1);
        dscal(nsamp,a[i][0],work[i],1);
        daxpy(nsamp,a[i][1],u.get_address(1,0),3,work[i],1);
        daxpy(nsamp,a[i][2],u.get_address(2,0),3,work[i],1);
    }
    for(i=0; i<3; ++i) dcopy(nsamp,work[i],1,u.get_address(i,0),3);
    for(i=0; i<3; ++i) delete [] work[i];
    /* Hand code this rather than use dmatrix or other library.
       Probably dumb, but this is just a 3x3 system.  This
       is simply a multiply of a*tmatrix with result replacing
       the internal tmatrix */
    double tmnew[3][3];
    double prod;
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j)
        {
            for(prod=0.0,k=0; k<3; ++k)
                prod+=a[i][k]*tmatrix[k][j];
            tmnew[i][j]=prod;
        }
    for(i=0; i<3; ++i)
        for(j=0; j<3; ++j)tmatrix[i][j]=tmnew[i][j];
    components_are_cardinal = this->tmatrix_is_cardinal();
    /* Assume this method does not yield cartesian coordinate directions.*/
    if(!components_are_cardinal) components_are_orthogonal=false;
}
/* This function computes and applies the free surface tranformaton
matrix described by Kennett 1991.  The result is a ray coordinate
transformation with x1=transverse, x2=radial, and x3=longitudinal.
Note this transformation is into a nonorthogonal system.

Algorithm first applies a rotation of horizontal coordinates to
horizonal radial and transverse, then applies free surface
transformation to the radial-vertical plane.

The free surface transformation code segment is a direct
translation of m file from Michael Bostock.

Author:  Gary Pavlis
*/
void CoreSeismogram::free_surface_transformation(SlownessVector uvec,
        double a0, double b0)
{
    if( (u.size()[1]<=0) || dead()) return; // do nothing in these situations
    double a02,b02,pslow,p2;
    double qa,qb,vpz,vpr,vsr,vsz;
    pslow=uvec.mag();
    // silently do nothing if magnitude of the slowness vector is 0
    // (vertical incidence)
    if(pslow<DBL_EPSILON) return;
    // Can't handle evanescent waves with this operator
    double vapparent=1.0/pslow;
    if(vapparent<a0 || vapparent<b0)
    {
        stringstream ss;
        ss<<"CoreSeismogram::free_surface_transformation method:  illegal input"<<endl
          << "Apparent velocity defined by input slowness vector="<<vapparent<<endl
          << "Smaller than specified surface P velocity="<<a0<<" or S velocity="<<b0<<endl
          << "That implies evanescent waves that violate the assumption of this operator"<<endl;
        throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }

    // First the horizonal rotation
    SphericalCoordinate scor;
    //rotation angle is - azimuth to put x2 (north in standard coord)
    //in radial direction
    scor.phi=atan2(uvec.uy,uvec.ux);
    scor.theta=0.0;
    scor.radius=1.0;
    // after this transformation x1=transverse horizontal
    // x2=radial horizonal, and x3 is still vertical
    this->rotate(scor);

    a02=a0*a0;
    b02=b0*b0;
    p2=pslow*pslow;
    qa=sqrt((1.0/a02)-p2);
    qb=sqrt((1.0/b02)-p2);
    vpz=-(1.0-2.0*b02*p2)/(2.0*a0*qa);
    vpr=pslow*b02/a0;
    vsr=(1.0-2.0*b02*p2)/(2.0*b0*qb);
    vsz=pslow*b0;
    /* Now construct the transformation matrix
     This is different from Bostock's original code
     in sign and order.  Also note this transformation
         is not scaled to have a unit matrix norm so amplitudes
         after the transformation are distorted.  rotate_to_standard,
         however, should still restore original data within roundoff
         error if called on the result. */
    double fstran[3][3];
    fstran[0][0]=0.5;
    fstran[0][1]=0.0;
    fstran[0][2]=0.0;
    fstran[1][0]=0.0;
    fstran[1][1]=vsr;
    fstran[1][2]=vpr;
    fstran[2][0]=0.0;
    fstran[2][1]=-vsz;
    fstran[2][2]=-vpz;
    this->transform(fstran);

    components_are_cardinal=false;
    components_are_orthogonal=false;
}
bool CoreSeismogram::set_transformation_matrix(const dmatrix& A)
{
    for(int i=0;i<3;++i)
        for(int j=0;j<3;++j) tmatrix[i][j]=A(i,j);
    bool cardinal;
    cardinal=this->tmatrix_is_cardinal();
    if(cardinal)
    {
        components_are_cardinal=true;
        components_are_orthogonal=true;
    }
    else
    {
        components_are_cardinal=false;
        /* Not necessarily true, but small overhead cost*/
        components_are_orthogonal=false;
    }
    return components_are_cardinal;
}
bool CoreSeismogram::set_transformation_matrix(const double a[3][3])
{
    for(int i=0;i<3;++i)
        for(int j=0;j<3;++j) tmatrix[i][j]=a[i][j];
    bool cardinal;
    cardinal=this->tmatrix_is_cardinal();
    if(cardinal)
    {
        components_are_cardinal=true;
        components_are_orthogonal=true;
    }
    else
    {
        components_are_cardinal=false;
        /* Not necessarily true, but small overhead cost*/
        components_are_orthogonal=false;
    }
    return components_are_cardinal;
}
CoreSeismogram& CoreSeismogram::operator=(const CoreSeismogram& seisin)
{
    if(this!=&seisin)
    {
        this->BasicTimeSeries::operator=(seisin);
        this->Metadata::operator=(seisin);
        components_are_orthogonal=seisin.components_are_orthogonal;
        components_are_cardinal=seisin.components_are_cardinal;
        for(int i=0; i<3; ++i)
        {
            for(int j=0; j<3; ++j)
            {
                tmatrix[i][j]=seisin.tmatrix[i][j];
            }
        }
        u=seisin.u;
    }
    return(*this);
}
void CoreSeismogram::set_dt(const double sample_interval)
{
  this->BasicTimeSeries::set_dt(sample_interval);
  /* This is the unique name - we always set it. */
  this->put("delta",sample_interval);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("dt");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,sample_interval);
    }
  }
}
void CoreSeismogram::set_t0(const double t0in)
{
  this->BasicTimeSeries::set_t0(t0in);
  /* This is the unique name - we always set it. */
  this->put("starttime",t0in);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("t0");
  aliases.insert("time");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,t0in);
    }
  }
}
void CoreSeismogram::set_npts(const size_t npts)
{
  this->BasicTimeSeries::set_npts(npts);
  /* This is the unique name - we always set it. */
  this->put("npts",npts);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("nsamp");
  aliases.insert("wfdisc.nsamp");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,npts);
    }
  }
  /* this method has the further complication that npts sets the size of the
  data matrix.   Here we resize the matrix and initialize it to 0s.*/
  this->u=dmatrix(3,npts);
  this->u.zero();
}

vector<double> CoreSeismogram::operator[] (const int i)const
{
    try {
        vector<double> result;
        result.reserve(3);
        for(int k=0;k<3;++k)
          result.push_back(this->u(k,i));
        return result;
    }catch(...){throw;};
}
vector<double> CoreSeismogram::operator[] (const double t) const
{
    try {
        vector<double> result;
        size_t i=this->sample_number(t);
        for(int k=0;k<3;++k)
          result.push_back(this->u(k,i));
        return result;
    }catch(...){throw;};
}
} // end namespace SEISPP
