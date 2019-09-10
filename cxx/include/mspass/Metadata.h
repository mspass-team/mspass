#ifndef _METADATA_H_
#define _METADATA_H_
#include <typeinfo>
#include <map>
#include <set>
#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/any.hpp>
#include "MsPASSError.h"
#include "BasicMetadata.h"

namespace mspass
{
/*! \brief Error thrown when get operators fail.   
 *
 * This is a convenience class used to construct a more informative 
 * set of errors when get operations fail.  */
class MetadataGetError : public MsPASSError
{
public:
  stringstream ss;
  /*! Constructor called when a key is not found in the Metadata.
   * \param Texpected is the type name (return of typeid name method) trying to extract. */
  MetadataGetError(string key,string Texpected)
  {
    
    string pretty_name(boost::core::demangle(Texpected.c_str()));
    ss<<"Error trying to extract Metadata with key="<<key<<endl
      << "No value associated with this key is set in Metadata object"<<endl
      << "Expected an entry of type="<<pretty_name<<endl;
    message=ss.str();
  };
  /*! \brief Constructor called when type requested does not match contents. 
 
    This implementation uses a pickle style map container where the 
    contents of the map can by any type.  We use boost::any to provide
    sanity checks on types.   This is creates the error message thrown
    when the type of the return does not match the type requested. */
  MetadataGetError(const char *boostmessage,string key,
          string Texpected,string Tactual)
  {
    ss << "Error in Metadata get method.   Type mismatch in request"<<endl
      << "boost::any bad_any_cast wrote this message:  "<< boostmessage<<endl;
    string name_e(boost::core::demangle(Texpected.c_str()));
    ss << "Trying to convert to data of type="<<name_e<<endl;
    string name_a(boost::core::demangle(Tactual.c_str()));
    ss << "Actual entry has type="<<name_a<<endl;
    message=ss.str();
  };
};

class Metadata : public BasicMetadata
{
public:
  /*! Default constructor.  Does nothing.
  **/
  Metadata(){};
  /*! Construct from a file.

  This simple file based constructor assumes the file contains only a set
  lines with this format:   key value type
  where type must be one of:   real, integer, bool, or string.   Note int is actually
  always promoted to a long.  The optional format
  variable is there to allow alternative formats in the future.

  \param ifs - ifstream from which to read data
  \param format - optional format specification.   Currently only default of "text"
     is accepted.
  \exception MsPASSError can be thrown for a variety of conditions.
  */
  Metadata(ifstream& ifs, const string form=string("pf"));
  /*!
  Standard copy constructor.

  \param mdold - parent object to be copied
  **/
  Metadata(const Metadata& mdold);
  /*! Standard assignment operator.
    \param mdold - parent object to copy
  */
  Metadata& operator=(const Metadata& mdold);
  /*! Append additional metadata with replacement.

A plus operator implies addition, but this overloading does something very
different.  A simple way to describe the effect is that on completion the
left hand side Metadata object will contain a duplicate of the right hand
side plus any attributes in the rhs that were not present on the lhs.
Another way to clarify this is to describe the algorithm.   We take each
attribute on the right and search for it in the lhs.  If it is not in the lhs
it will be added.  If it is there already, the rhs value will replace the old
value on the lhs.   This is most useful when an algorithm creates a new set of
attributes that we want to use in downstream processing but retain all the
other attributes.

\param rhs is the new metadata to be insert/replace on the lhs.
*/
  Metadata& operator+=(const Metadata& rhs) noexcept;
  const Metadata operator+(const Metadata& other) const;
  /* All the getters - all but the template are wrappers with the type
  fixed */
  /*!
  Get a real number from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  double get_double(const string key){
    try{
      double val;
      val=get<double>(key);
      return val;
    }catch(...){throw;};
  };
  /*!
  Get an integer from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  int get_int(const string key)
  {
    try{
      int val;
      val=get<int>(key);
      return val;
    }catch(...){throw;};
  };
  /*!
  Get a long integer from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  long get_long(const string key){
    try{
      long val;
      val=get<long>(key);
      return val;
    }catch(...){throw;};
  };
  /*!
  Get a string from the Metadata object.

  Note the string in this case can be quite large.  If the string
  was parsed from an Antelope Pf nested Tbl and Arrs can be extracted
  this way and parsed with pf routines.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  string get_string(const string key){
    try{
      string val;
      val=get<string>(key);
      return val;
    }catch(...){throw;};
  };
  /*!
  Get a  boolean parameter from the Metadata object.

  This method never throws an exception assuming that if the
  requested parameter is not found it is false.

  \param key keyword associated with requested metadata member.
  **/
  bool get_bool(const string key){
    try{
      bool val;
      val=get<bool>(key);
      return val;
    }catch(...){throw;};
  };
  /*! Generic get interface.

  This is a generic interface most useful for template procedures
  that need to get a Metadata component.   Since this object only
  can contain simple types the type requested must be simple.
  Currently supports only int, long, short, double, float, and string.
  C char* is intentionally not supported.
  Calls to anything but the supported types will
  throw an exception.

  \param key is the name tag of desired component.

  \exception - will throw a MetadataGetError (child of MsPASSError) for
       type mismatch or in an overflow or underflow condition.
  */
  template <typename T> T get(const string key);
  /*! \brief Generic get interface for C char array.

          This is a generic interface most useful for template procedures
          that need to get a Metadata component.   Since this object only
          can contain simple types the type requested must be simple.
          Currently supports only int, long, short, double, float, and string.
          C char* is intentionally not supported. This is largely a wrapper
          on the string key version of this same generic function.

          \param key is the name tag of desired component.

          \exception - will throw a MetadataGetError (child of MsPASSError) for
             type mismatch or in an overflow or underflow condition.
             */
  template <typename T> T get(const char *key)
  {
    try{
      T val;
      val=get<T>(string(key));
      return val;
    }catch(...){throw;};
  }
  template <typename T> void put(const string key, T val) noexcept
  {
    boost::any aval=val;
    md[key]=aval;
    changed_or_set.insert(key);
  }
  template <typename T> void put (const char *key, T val) noexcept
  {
    /* could do this as put(string(key),val) but this is so trivial duplicating
    the code for the string method is more efficient than an added function call.*/
    boost::any aval=val;
    md[string(key)]=aval;
    changed_or_set.insert(string(key));
  }
  void put(const string key, const double val)
  {
      this->put<double>(key,val);
  };
  void put(const string key, const int val)
  {
      this->put<int>(key,val);
  };
  void put(const string key, const bool val)
  {
      this->put<bool>(key,val);
  };
  void put(const string key, const string val)
  {
      this->put<string>(key,val);
  };

  friend ostream& operator<<(ostream&, Metadata&);
protected:
  map<string,boost::any> md;
  /* The keys of any entry changed will be contained here.   */
  set<string> changed_or_set;
};

template <typename T> T Metadata::get(const string key)
{
  T result;
  map<string,boost::any>::const_iterator iptr;
  iptr=md.find(key);
  if(iptr==md.end())
  {
    throw MetadataGetError(key,typeid(T).name());
  }
  boost::any aval=iptr->second;
  try{
    result=boost::any_cast<T>(aval);
  }catch(boost::bad_any_cast& err)
  {
    const std::type_info &ti = aval.type();
    throw MetadataGetError(err.what(),key,typeid(T).name(),ti.name());
  };
  return result;
}
}  //End of namespace MsPASS
#endif
