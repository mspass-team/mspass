#ifndef _BASICMETADATA_H_
#define _BASICMETADATA_H_
#include <string>

namespace mspass
{
namespace utility{
/*! \brief Abstract base class for Metadata concept.

A core idea in MsPASS is the idea of a generic header that allows storage and
retrieval of arbitrary attributes.   This base class forces support for
the standard basic data types.
*/
class BasicMetadata
{
public:
  virtual ~BasicMetadata(){};
  virtual int get_int(const std::string key) const =0;
  virtual double get_double(const std::string key)const =0;
  virtual bool get_bool(const std::string key) const =0;
  virtual std::string get_string(const std::string key)const =0;
  virtual void put(const std::string key, const double val)=0;
  virtual void put(const std::string key, const int val)=0;
  virtual void put(const std::string key, const bool val)=0;
  virtual void put(const std::string key, const std::string val)=0;
};
} // end utility namespace
};   // End mspass namespace encapsulation
#endif
