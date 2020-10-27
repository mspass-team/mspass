#ifndef _SLOWNESS_H_
#define _SLOWNESS_H_
#include <string>
namespace mspass::seismic
{
/*! \brief Slowness vector object.  

 Slowness vectors are a seismology concept used to describe wave propagation.
 A slowness vector points in the direction of propagation of a wave with a
 magnitude equal to the slowness (1/velocity) of propagation.  
\author Gary L. Pavlis
**/

class SlownessVector
{
public:
/*!
 East-west component of slowness vector.
**/
	double ux;
/*!
 North-south component of slowness vector.
**/
	double uy;
/*!
 Default constructor.
**/
	SlownessVector();
/*! \brief Fully parameterized constructor.

A slowness vector is defined by it's components.  There is one ambiguity, however,
with a zero slowness vector.  That is, normally direction of propagation is
inferred from the vector azimuth.  A zero slowness vector has physical significance
(normal incidence) but presents and ambiguity in this regard.  We use a defaulted
az0 parameter to specify the azimuth that should be used if the magnitude of
slowness vector is 0.

\param ux0 - set x (EW) component to this value.
\param uy0 - set y (NS) component to this value.
\param az0 - use this as azimuth (radians) if this is a zero slowness vector
	(default 0.0)
*/
	SlownessVector(const double ux0, const double uy0, const double az0=0.0);
/*!
 Copy constructor.
**/
	SlownessVector(const SlownessVector&);
/*!
 Computes the magntitude of the slowness vector.
 Value returned is in units of seconds/kilometer.  
**/
	double mag() const noexcept;
/*!
 Returns the propagation direction defined by a slowness vector.
 Azimuth is a direction clockwise from north in the standard geographic
 convention.  Value returned is in radians.
**/
	double azimuth() const noexcept;
/*!
 Returns the back azimuth direction defined by a slowness vector.
 A back azimuth is 180 degrees away from the direction of propagation and 
 points along the great circle path directed back to the source point 
 from a given position.  The value returned is in radians.
**/
	double baz() const noexcept; 
/*! \brief Standard assignment operator. */
	SlownessVector& operator=(const SlownessVector& parent);
/* \brief Standard accumulation operator. */
	SlownessVector& operator+=(const SlownessVector& other);
/* \brief Standard subtract from  operator. */
	SlownessVector& operator-=(const SlownessVector& other);
/* \brief Standard addition  operator. */
        const SlownessVector operator+(const SlownessVector& other) const;
/* \brief Standard subraction  operator. */
        const SlownessVector operator-(const SlownessVector& other) const;
private:
	double azimuth0;
};

} // End mspass::seismic namespace declaration
#endif
