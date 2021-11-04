#ifndef _TAPER_H_
#define _TAPER_H_
//#include <math.h>
#include <vector>
#include <memory>


#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>

#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"

namespace mspass::algorithms{

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
  virtual int apply(mspass::seismic::TimeSeries& d)=0;
  virtual int apply(mspass::seismic::Seismogram& d)=0;
  void enable_head(){head=true;};
  void disable_head(){head=false;all=false;};
  void enable_tail(){tail=true;};
  void disable_tail(){tail=false;all=false;};
  bool head_is_enabled()
  {
    if(head || all)
    {
      return true;
    }
    return false;
  };
  bool tail_is_enable()
  {
    if(tail || all)
    {
      return true;
    }
    return false;
  }
protected:
  /* A taper can be head, tail, or all.  For efficiency it is required
  implementations set these three booleans.   head or tail may be true.
  all means a single function is needed to defne the taper.  */
  bool head,tail,all;
private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
      ar & head;
      ar & tail;
      ar & all;
  };
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
  int apply(mspass::seismic::TimeSeries& d);
  int apply(mspass::seismic::Seismogram& d);
  double get_t0head(){return t0head;};
  double get_t1head(){return t1head;};
  double get_t0tail(){return t0tail;};
  double get_t1tail(){return t1tail;};
private:
  double t0head,t1head,t1tail,t0tail;
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & boost::serialization::base_object<BasicTaper>(*this);
    ar & t0head;
    ar & t1head;
    ar & t1tail;
    ar & t0tail;
  };
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
  int apply(mspass::seismic::TimeSeries& d);
  int apply(mspass::seismic::Seismogram& d);
  double get_t0head(){return t0head;};
  double get_t1head(){return t1head;};
  double get_t0tail(){return t0tail;};
  double get_t1tail(){return t1tail;};
private:
  double t0head,t1head,t1tail,t0tail;
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & boost::serialization::base_object<BasicTaper>(*this);
    ar & t0head;
    ar & t1head;
    ar & t1tail;
    ar & t0tail;
  };
};
/*! General taper.

This method provides a simple way to build a taper from a set of uniformly
spaced points.   The apply methods will dogmatically only accept input
data of the same length as the taper defined in the operator. */
class VectorTaper : public BasicTaper
{
public:
  VectorTaper();
  VectorTaper(const std::vector<double> taperdata);
  int apply(mspass::seismic::TimeSeries& d);
  int apply(mspass::seismic::Seismogram& d);
  void disable(){all=false;};
  void enable(){
    if(taper.size()>0) all=true;
  };
  std::vector<double> get_taper(){return taper;};
private:
  std::vector<double> taper;
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & boost::serialization::base_object<BasicTaper>(*this);
    ar & taper;
  };
};
/*! \brief Mute operator for "top" of signals defined first smaple forward.

A top mute is very commonly used in a many forms of seismic processing.
It is, for example, a very low level operation in traditional seismic
reflection processing.  A top mute zeros the front (forward in time from first
sample) of the signal and ramps up to a multiplier of 1 (does nothing)
at some later time.  It can also be thought of as a taper with only the
low side altered.  The implementation, in fact, uses the family of
mspass taper operators internally the the high time range (tail) turned off.

The main constructor uses a string keyword to select the type of tapering
applied to define the mute.   Because of the relationship to mspass tapers
there is also a constructor using the base class for Taper objects.  It
allows custom implementations of taper beyond those associated with
keywords in the definition passed to the main constructor.
*/
class TopMute
{
public:
  /*! Default constructor.  Exists but the result is invalid */
  TopMute();
  /*! \brief Primary constructor driven by a named keyword.

  This is the normal constructor most users will want to us.  It is
  defined by a time range for the mute to ramp from 0 to 1 and a string
  matching one of the supported types.  Note the mspass VectorTaper cannot
  be used for this constructor because it requires more than 2 arguments
  to be defined.

  \param t0 is the end of the zeroed time range.  Date from the first sample
     to this value will be zeroed.
  \param t1 end ramp. Data with t>t1 will be unaltered.
  \param type defines the type of taper desired. Current options are
    'linear' and 'cosine'.  They enable the LinearTaper and CosineTaper
    opeators respectively.

  \exception This function will throw a MsPASSError if t1<=t0.
  */
  TopMute(const double t0, const double t1, const std::string type);
  /*! Standard copy constructor. */
  TopMute(const TopMute& parent);
  /*! Destructor.  The destructor of this class is not null. */
  ~TopMute();
  /*! Standard assignment operator. */
  TopMute& operator=(const TopMute& parent);
  /*! Apply the operator to a TimeSeries object. */
  int apply(mspass::seismic::TimeSeries& d);
  /*! Apply the operator to a Seismogram object. */
  int apply(mspass::seismic::Seismogram& d);
private:
  /* We use a shared_ptr to the base class.  That allows inheritance to
  handle the actual form - a classic oop use of a base class. the shared_ptr
  allows us to get around an abstract base problem.   May be other solutions
  but this should be ok.  There may be a problem in parallel environment,
  however, as not sure how this would be handled by spark or dask.   this is
  in that pickable realm.*/
  std::shared_ptr<BasicTaper> taper;
};
} // End namespace
#endif // End guard
