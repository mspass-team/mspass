#include <math.h>
#include <vector>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"

namespace mspass{

class BasicTaper
{
public:
  BasicTaper()
  {
    head =false;
    tail = false;
    all = false;
  };
  virtual ~BasicTaper(){};
  virtual int apply(mspass::TimeSeries& d)=0;
  virtual int apply(mspass::Seismogram& d)=0;
protected:
  /* A taper can be head, tail, or all.  For efficiency it is required
  implementations set these three booleans.   head or tail may be true.
  all means a single function is needed to defne the taper.  */
  bool head,tail,all;
};
/*! \brief Used to construct an operator to apply a linear taper to either end.

Linear tapers are defined here as a time spanning a ramp running from 0 to 1.
Data will be zeroed on each end of a 0 mark and a linear weight applied between
0 points and 1 points.  Postive ramp slope on left and negative slope ramp on
right. */
class LinearTaper : public BasicTaper
{
public:
  LinearTaper();
  /*! \brief primary constructor.

  Defines linear taper for front and/or back end of a time range.
  Zero times before t0head and after t0tail.   Linear ramp between
  t0head and t1head and in opposite sense from t1tail to t0tail.
  Setting 0 value = 1 value should be used as a signal to disable.
  */
  LinearTaper(const double t0head,const double t1head,
            const double t1tail,const double t0tail);
  /* these need to post to history using new feature*/
  int apply(mspass::TimeSeries& d);
  int apply(mspass::Seismogram& d);
private:
  double t0head,t1head,t1tail,t0tail;
};
/*! \brief Taper front and/or end of a time seris with a half cosine function.

A sine taper is a common, simple approach to taper data.  When applied at the
front it defnes a half cycle of a cosine curve +1.0 in range -pi to 0.  On
the right it defines the same function for the range 0 to pi.  The period
of the left and right operator can be different.  Turn off left or right by
giving illegal start and end points and the operator will silently be
only one sided. */
class CosineTaper : public BasicTaper
{
public:
  CosineTaper();
  /*! \brief primary constructor.

  Defines half-cycle sine taper for front and/or back end of a time range.
  Zero times before t0head and after t0tail.   Taper between
  t0head and t1head and in opposite sense from t1tail to t0tail.
  */
  CosineTaper(const double t0head,const double t1head,
            const double t1tail,const double t0tail);
  /* these need to post to history using new feature*/
  int apply(mspass::TimeSeries& d);
  int apply(mspass::Seismogram& d);
private:
  double t0head,t1head,t1tail,t0tail;
};
/*! General taper.

This method provides a simple way to build a taper from a set of uniformly
spaced points.   The apply methods will dogmatically only accept input
data of the same length as the taper defined in the operator. */
class VectorTaper : public BasicTaper
{
public:
  VectorTaper();
  VectorTaper(const vector<double> taperdata);
  int apply(mspass::TimeSeries& d);
  int apply(mspass::Seismogram& d);
  void disable(){all=false;};
  void enable(){
    if(taper.size()>0) all=true;
  };
private:
  vector<double> taper;
};
} // End namespace
