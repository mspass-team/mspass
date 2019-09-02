#ifndef _BASICMETADATA_H_
#define _BASICMETADATA_H_
#include <string>
using std::string;
namespace MsPASS
{
/*! \brief Abstract base class for Metadata concept.

A core idea in MsPASS is the idea of a generic header that allows storage and
retrieval of arbitrary attributes.   This base class forces support for
the standard basic data types.
*/
class BasicMetadata
{
public:
  virtual int get_int(const string key)=0;
  virtual double get_double(const string key)=0;
  virtual bool get_bool(const string key)=0;
  virtual string get_string(const string key)=0;
  virtual void put(const string key, const double val)=0;
  virtual void put(const string key, const int val)=0;
  virtual void put(const string key, const bool val)=0;
  virtual void put(const string key, const string val)=0;
};

};   // End MsPASS namespace encapsulation
#endif
