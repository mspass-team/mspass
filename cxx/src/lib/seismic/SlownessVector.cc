#include <float.h>
#include <math.h>
#include "mspass/seismic/SlownessVector.h"
namespace mspass
{
using namespace std;
using namespace mspass;
// these are trivial constructors and could be done inline, but 
// decided to put them here to keep things together.  Learned this
// lesson the hard way
//
SlownessVector::SlownessVector()
{
	ux=0.0;
	uy=0.0;
	azimuth0=0.0;
}
SlownessVector::SlownessVector(const SlownessVector& old)
{
	ux=old.ux;
	uy=old.uy;
	azimuth0=old.azimuth0;
}
SlownessVector::SlownessVector(const double ux0, const double uy0, const double az0)
{
	ux=ux0;
	uy=uy0;
	azimuth0=az0;
}
SlownessVector& SlownessVector::operator=(const SlownessVector& parent)
{
	if(this!=&parent)
	{
		ux=parent.ux;
		uy=parent.uy;
		azimuth0=parent.azimuth0;
	}
	return(*this);
}
SlownessVector& SlownessVector::operator+=(const SlownessVector& other)
{
    ux+=other.ux;
    uy+=other.uy;
    return(*this);
}
SlownessVector& SlownessVector::operator-=(const SlownessVector& other)
{
    ux-=other.ux;
    uy-=other.uy;
    return(*this);
}
const SlownessVector SlownessVector::operator+(const SlownessVector& other) const {
    SlownessVector result(*this);
    result += other;
    return result;
}
const SlownessVector SlownessVector::operator-(const SlownessVector& other) const {
    SlownessVector result(*this);
    result -= other;
    return result;
}

// These could (and once were) inline, but decided that was poor
// memory management
double SlownessVector::mag() const noexcept
{
	return(hypot(ux,uy));
}
double SlownessVector::azimuth() const noexcept
{
	if(this->mag() <= FLT_EPSILON) return(azimuth0);
	double phi;
	phi=M_PI_2-atan2(uy,ux);
	if(phi>M_PI)
                return(phi-2.0*M_PI);
	else if(phi<-M_PI)
		return(phi+2.0*M_PI);
        else
                return(phi);
}
double SlownessVector::baz() const noexcept
{
	double phi;
	if(this->mag() <= FLT_EPSILON) 
		phi=M_PI-azimuth0;
	else
		phi=3.0*M_PI_2-atan2(uy,ux);
	if(phi>M_PI)
                return(phi-2.0*M_PI);
	else if(phi<-M_PI)
		return(phi+2.0*M_PI);
        else
                return(phi);
}


}  // End namespace SEISPP
