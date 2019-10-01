#include <float.h>
#include <math.h>
#include <sstream>
#include "cblas.h"
/* This is a temporary hack fix for mac laptop.   Had fink install
   openblas.  This is the fink default location for include files. */
//#include "/sw/include/lapacke.h"
/* These should com from lapacke.h but there are some collisions with 
 * boost that create compile errors. Hack fix until we resolve the 
 * issue of how we will standardize lapack version.*/
#define LAPACK_ROW_MAJOR               101
#define LAPACK_COL_MAJOR               102
int LAPACKE_dgetrf(int,int,int,double*,int,int*);
int LAPACKE_dgetri(int,int,double*,int,int *);
#include "seismic/Seismogram.h"
#include "mspass/MsPASSError.h"
#include "mspass/SphericalCoordinate.h"
namespace mspass
{
using namespace mspass;
/*
 *  Start with all the constructors.
 *
*/
//
// Default constructor for Seismogram could be
// done inline in seispp.h, but it is complication enough I put
// it here
//
Seismogram::Seismogram() : Metadata(),u(0,0)
{
    live = false;
    dt=0.0;
    t0=0.0;
    ns=0;
    tref=TimeReferenceType::UTC;
    components_are_orthogonal=true;
    components_are_cardinal=true;
    for(int i=0; i<3; ++i)
        for(int j=0; j<3; ++j)
            if(i==j)
                tmatrix[i][i]=1.0;
            else
                tmatrix[i][j]=0.0;
}
Seismogram::Seismogram(int nsamples)
    : BasicTimeSeries(),Metadata(),u(3,nsamples)
{
    components_are_orthogonal=true;
    components_are_cardinal=true;
    for(int i=0; i<3; ++i)
        for(int j=0; j<3; ++j)
            if(i==j)
                tmatrix[i][i]=1.0;
            else
                tmatrix[i][j]=0.0;
}

Seismogram::Seismogram(const Seismogram& t3c) :
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
bool Seismogram::tmatrix_is_cardinal()
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

Seismogram::Seismogram(const vector<TimeSeries>& ts,
                       const int component_to_clone)
    : BasicTimeSeries(dynamic_cast<const BasicTimeSeries&>(ts[component_to_clone])),
     Metadata(dynamic_cast<const Metadata&>(ts[component_to_clone])),
      u()
{
    const string base_error("Seismogram constructor from 3 Time Series:  ");
    int i,j;
    /* beware irregular sample rates, but don' be too machevelian.
           Abort only if the mismatch is large defined as accumulated time
           over data range of this constructor is less than half a sample */
    if( (ts[0].dt!=ts[1].dt) || (ts[1].dt!=ts[2].dt) )
    {
        double ddtmag1=fabs(ts[0].dt-ts[1].dt);
        double ddtmag2=fabs(ts[1].dt-ts[2].dt);
        double ddtmag;
        if(ddtmag1>ddtmag1)
            ddtmag=ddtmag1;
        else
            ddtmag=ddtmag2;
        ddtmag1=fabs(ts[0].dt-ts[2].dt);
        if(ddtmag1>ddtmag)  ddtmag=ddtmag1;
        double ddtcum=ddtmag*((double)ts[0].ns);
        if(ddtcum>(ts[0].dt)/2.0)
        {
            stringstream ss;
            ss << base_error
               << "Sample intervals of components are not consistent"<<endl;
            for(int ie=0; ie<3; ++ie) ss << "Component "<<ie<<" dt="<<ts[ie].dt<<" ";
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
    t0_component[0]=ts[0].t0;
    t0_component[1]=ts[1].t0;
    t0_component[2]=ts[2].t0;

    // Treat the normal case specially and avoid a bunch of work unless
    // it is required
    if( (ts[0].ns==ts[1].ns) && (ts[1].ns==ts[2].ns)
            && (fabs( (t0_component[0]-t0_component[1])/dt )<1.0)
            && (fabs( (t0_component[1]-t0_component[2])/dt )<1.0))
    {
        this->u=dmatrix(3,ns);
        // Load data by a simple copy operation
        /* This is a simple loop version
        for(j=0;j<ns;++ns)
        {
        	this->u(0,j)=ts[0].s[j];
        	this->u(1,j)=ts[1].s[j];
        	this->u(2,j)=ts[2].s[j];
        }
        */
        // This is a vector version that I'll use because it will
        // be faster albeit infinitely more obscure and
        // intrinsically more dangerous
        cblas_dcopy(ns,&(ts[0].s[0]),1,u.get_address(0,0),3);
        cblas_dcopy(ns,&(ts[1].s[0]),1,u.get_address(1,0),3);
        cblas_dcopy(ns,&(ts[2].s[0]),1,u.get_address(2,0),3);
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
        ns=round((temin-tsmax)/dt);
        if(ns<=0) throw MsPASSError(base_error
                                        +"Irregular time windows of components have no overlap",
                                        ErrorSeverity::Invalid);
        this->u=dmatrix(3,ns);
        // Now load the data.  Use the time and sample number methods
        // to simplify process
        double t;
        t=tsmax;
        this->t0 = t;
        for(int ic=0; ic<3; ++ic)
        {
            for(j=0; j<ts[ic].ns; ++j)
            {
                i=ts[ic].sample_number(t);
                // silently do nothing if outside bounds.  This
                // perhaps should be an error as it shouldn't really
                // happen with the above algorithm, but safety is good
                if( (i>=0) && (i<ns) ) this->u(ic,j)=ts[ic].s[i];
                t += (this->dt);
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

void Seismogram::rotate_to_standard()
{
    if( (ns<=0) || !live) return; // do nothing in these situations
    double *work[3];
    int i,j;
    if(components_are_cardinal) return;
    for(j=0; j<3; ++j) work[j]=new double[ns];
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
            cblas_dcopy(ns,u.get_address(0,0),3,work[i],1);
            cblas_dscal(ns,tmatrix[0][i],work[i],1);
            cblas_daxpy(ns,tmatrix[1][i],u.get_address(1,0),3,work[i],1);
            cblas_daxpy(ns,tmatrix[2][i],u.get_address(2,0),3,work[i],1);
        }
        for(i=0; i<3; ++i) cblas_dcopy(ns,work[i],1,u.get_address(i,0),3);
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
        //Perf lib matrix inversion routine using LU factorizatoin
        // Note this is changed from parent code.  Untested.
        //This was the call using perf library in antelope contrib
        //dgetrf_(&asize,&asize,a,&asize,ipivot,&info);
        // This is the openblas version 
        info=LAPACKE_dgetrf(LAPACK_COL_MAJOR,3,3,a,3,ipivot);
        if(info!=0)
        {
            for(i=0; i<3; ++i) delete [] work[i];
            throw(MsPASSError(
                      string("rotate_to_standard:  LU factorization of transformation matrix failed")),
                  ErrorSeverity::Invalid);
        }
        // Again this is the perf lib version
        //dgetri_(&asize,a,&asize,ipivot,awork,&ldwork,&info);
        // This is the openblas version
        info=LAPACKE_dgetri(LAPACK_COL_MAJOR,3,a,3,ipivot);
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
            cblas_dcopy(ns,u.get_address(0,0),3,work[i],1);
            cblas_dscal(ns,tmatrix[i][0],work[i],1);
            cblas_daxpy(ns,tmatrix[i][1],u.get_address(1,0),3,work[i],1);
            cblas_daxpy(ns,tmatrix[i][2],u.get_address(2,0),3,work[i],1);
        }
        for(i=0; i<3; ++i) cblas_dcopy(ns,work[i],1,u.get_address(i,0),3);
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
void Seismogram::rotate(SphericalCoordinate xsc)
{
    if( (ns<=0) || !live) return; // do nothing in these situations
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
        cblas_dscal(ns,-1.0,u.get_address(2,0),3);
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
    for(i=0; i<3; ++i)work[i] = new double[ns];
    for(i=0; i<3; ++i)
    {
        cblas_dcopy(ns,u.get_address(0,0),3,work[i],1);
        cblas_dscal(ns,tmatrix[i][0],work[i],1);
        cblas_daxpy(ns,tmatrix[i][1],u.get_address(1,0),3,work[i],1);
        cblas_daxpy(ns,tmatrix[i][2],u.get_address(2,0),3,work[i],1);
    }
    for(i=0; i<3; ++i) cblas_dcopy(ns,work[i],1,u.get_address(i,0),3);
    components_are_cardinal=false;
    for(i=0; i<3; ++i) delete [] work[i];
}
void Seismogram::rotate(const double nu[3])
{
    if( (ns<=0) || !live) return; // do nothing in these situations
    SphericalCoordinate xsc=UnitVectorToSpherical(nu);
    this->rotate(xsc);
}
/* simplified procedure to rotate only zonal angle by phi radians.
 Similar to above but using only azimuth angle AND doing a simple
 rotation in the horizontal plane.  Efficient algorithm only
 alters 0 and 1 components. */
void Seismogram::rotate(double phi)
{
    if( (ns<=0) || !live) return; // do nothing in these situations
    int i;
    double a,b;
    /* Restore to cardinate coordinates */
    this->rotate_to_standard();
    a=cos(phi);
    b=sin(phi);
    tmatrix[0][0] = a;
    tmatrix[1][0] = b;
    tmatrix[2][0] = 0.0;
    tmatrix[0][1] = -b;
    tmatrix[1][1] = a;
    tmatrix[2][1] = 0.0;
    tmatrix[0][2] = 0.0;
    tmatrix[1][2] = 0.0;
    tmatrix[2][2] = 1.0;

    /* Now multiply the data by this transformation matrix.
     Not trick in this i only goes to 2 because 3 component
     is an identity.*/
    double *work[2];
    for(i=0; i<2; ++i)work[i] = new double[ns];
    for(i=0; i<2; ++i)
    {
        cblas_dcopy(ns,u.get_address(0,0),3,work[i],1);
        cblas_dscal(ns,tmatrix[i][0],work[i],1);
        cblas_daxpy(ns,tmatrix[i][1],u.get_address(1,0),3,work[i],1);
    }
    for(i=0; i<2; ++i) cblas_dcopy(ns,work[i],1,u.get_address(i,0),3);
    components_are_cardinal=false;
    for(i=0; i<2; ++i) delete [] work[i];
}
void Seismogram::transform(const double a[3][3])
{
    if( (ns<=0) || !live) return; // do nothing in these situations
    int i,j,k;
    double *work[3];
    for(i=0; i<3; ++i) work[i] = new double[ns];
    for(i=0; i<3; ++i)
    {
        cblas_dcopy(ns,u.get_address(0,0),3,work[i],1);
        cblas_dscal(ns,a[i][0],work[i],1);
        cblas_daxpy(ns,a[i][1],u.get_address(1,0),3,work[i],1);
        cblas_daxpy(ns,a[i][2],u.get_address(2,0),3,work[i],1);
    }
    for(i=0; i<3; ++i) cblas_dcopy(ns,work[i],1,u.get_address(i,0),3);
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
void Seismogram::free_surface_transformation(SlownessVector uvec,
        double a0, double b0)
{
    if( (ns<=0) || !live) return; // do nothing in these situations
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
        ss<<"Seismogram::free_surface_transformation method:  illegal input"<<endl
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

Seismogram& Seismogram::operator=(const Seismogram& seisin)
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
vector<double> Seismogram::operator[] (const int i)const
{
    try {
        vector<double> result;
        result.reserve(3);
        for(int k=0;k<3;++k) 
          result.push_back(this->u(k,i));
        return result;
    }catch(...){throw;};
}
vector<double> Seismogram::operator[] (const double t) const
{
    try {
        vector<double> result;
        int i=this->sample_number(t);
        for(int k=0;k<3;++k) 
          result.push_back(this->u(k,i));
        return result;
    }catch(...){throw;};
}
} // end namespace SEISPP
