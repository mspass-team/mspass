#include <float.h>
#include <math.h>
#include "mspass/utility/SphericalCoordinate.h"
namespace mspass
{
using namespace mspass;
/* This routine takes a 3-d unit vector, nu, and converts it
to a SphericalCoordinate structure which is returned.  The 
input coordinates are assume to be standard, right handed
cartesian coordinates in 1,2,3 order */
SphericalCoordinate UnitVectorToSpherical(const double nu[3])
{
	SphericalCoordinate xsc;

	xsc.radius = 1.0;
	xsc.theta = acos(nu[2]);
	if(hypot(nu[0],nu[1])<DBL_EPSILON)
		xsc.phi=0.0;
	else
		xsc.phi = atan2(nu[1],nu[0]);
	return(xsc);
}
/* Reciprocal of above.  A bit harder as it has to handle singular
case.  Note the double vector of 3 is allocated here and externally
needs to be free using C++ delete NOT free.*/
double *SphericalToUnitVector(const SphericalCoordinate& scor)
{
	double *nu=new double[3];
	// vertical vector case
	if(fabs(scor.theta)<= DBL_EPSILON) 
	{
		nu[0]=0.0;
		nu[1]=0.0;
		nu[2]=1.0;
	}
	else
	{
		nu[0]=sin(scor.theta)*cos(scor.phi);
		nu[1]=sin(scor.theta)*sin(scor.phi);
		nu[2]=cos(scor.theta);
		// force pure zeros for convenience
		for(int i=0;i<3;++i)
			if(fabs(nu[i])<DBL_EPSILON)
				nu[i]=0.0;
	}
	return(nu);
}
/* Other procedures conveniently placed here. */
double rad(const double theta_deg)
{
    const double deg_to_rad(57.295779513082321);
    return theta_deg*deg_to_rad;
}
double deg(const double theta_rad)
{
    const double rad_to_deg(0.017453292519943);
    return theta_rad*rad_to_deg;
}
} // end namespace declaration
