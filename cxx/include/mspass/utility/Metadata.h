#ifndef _METADATA_H_
#define _METADATA_H_
#include <typeinfo>
#include <map>
#include <set>
#include <list>
#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/any.hpp>
#include <pybind11/pybind11.h>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/BasicMetadata.h"

namespace mspass
{
namespace utility{
/*! \brief Error thrown when get operators fail.
 *
 * This is a convenience class used to construct a more informative
 * set of errors when get operations fail.  */
class MetadataGetError : public MsPASSError
{
public:
  std::stringstream ss;
  MetadataGetError():MsPASSError(){}; // seems necessary to not default this with gcc
  /*! Constructor called when a key is not found in the Metadata.
   * \param Texpected is the type name (return of typeid name method) trying to extract. */
  MetadataGetError(const std::string key,const char *Texpected)
  {

    std::string pretty_name(boost::core::demangle(Texpected));
    ss<<"Error trying to extract Metadata with key="<<key<<std::endl
      << "No value associated with this key is set in Metadata object"<<std::endl
      << "Expected an entry of type="<<pretty_name<<std::endl;
    message=ss.str();
    badness=ErrorSeverity::Suspect;
  };
  /*! \brief Constructor called when type requested does not match contents.

    This implementation uses a pickle style map container where the
    contents of the map can by any type.  We use boost::any to provide
    sanity checks on types.   This is creates the error message thrown
    when the type of the return does not match the type requested. */
  MetadataGetError(const char *boostmessage,const std::string key,
      const char *Texpected, const char *Tactual)
  {
    ss << "Error in Metadata get method.   Type mismatch in attem to get "
	    << "data with key="<<key<<std::endl
      << "boost::any bad_any_cast wrote this message:  "<<std::endl
      << boostmessage<<std::endl;
    std::string name_e(boost::core::demangle(Texpected));
    ss << "Trying to convert to data of type="<<name_e<<std::endl;
    std::string name_a(boost::core::demangle(Tactual));
    ss << "Actual entry has type="<<name_a<<std::endl;
    message=ss.str();
    badness=ErrorSeverity::Suspect;
  };
  MetadataGetError(const MetadataGetError& parent)
  {
      message=parent.message;
      badness=parent.badness;
  };
  MetadataGetError operator=(const MetadataGetError& parent)
  {
      if(this!=&parent)
      {
          message=parent.message;
          badness=parent.badness;
      }
      return *this;
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
  Metadata(std::ifstream& ifs, const std::string form=std::string("pf"));
  /*!
  Standard copy constructor.

  \param mdold - parent object to be copied
  **/
  Metadata(const Metadata& mdold);
  /*! Destructor - has to be explicitly implemented and declared virtual
    for reasons found in textbooks and various web forums.  A very subtle
    feature of C++  inheritance. */
  virtual ~Metadata(){};
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
  /*! Add two Metadata objects.   Uses operator+=*/
  const Metadata operator+(const Metadata& other) const;
  /* All the getters - all but the template are wrappers with the type
  fixed */
  /*!
  Get a real number from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  double get_double(const std::string key) const override{
    try{
      double val;
      val=get<double>(key);
      return val;
    }catch(MetadataGetError& merr)
    {
    /* Try a float if that failed */
      try{
        float fval;
        fval=get<float>(key);
        return fval;
      }catch(MetadataGetError& merr)
      {
	throw merr;
      }
    }
  };
  /*!
  Get an integer from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  int get_int(const std::string key) const override
  {
      try{
        int val;
        val=get<int>(key);
        return val;
      }
      catch(MetadataGetError& merr)
      {
	try{
          long lval;
  	  lval=get<long>(key);
  	  return static_cast<int>(lval);
	}catch(MetadataGetError& merr)
	{
	  throw merr;
	}
      }
  };
  /*!
  Get a long integer from the Metadata object.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  long get_long(const std::string key) const
  {
      try{
        long val;
        val=get<long>(key);
        return val;
      }
      catch(MetadataGetError& merr)
      {
	try{
          int ival;
  	  ival=get<int>(key);
  	  return static_cast<long>(ival);
	}catch(MetadataGetError& merr)
	{
	  throw merr;
	}
      }
  };
  /*!
  Get a string from the Metadata object.

  Note the string in this case can be quite large.  If the string
  was parsed from an Antelope Pf nested Tbl and Arrs can be extracted
  this way and parsed with pf routines.

  \exception MetadataGetError if requested parameter is not found or there is a type mismatch.
  \param key keyword associated with requested metadata member.
  **/
  std::string get_string(const std::string key) const override{
    try{
      std::string val;
      val=get<std::string>(key);
      return val;
    }catch(...){throw;};
  };
  /*!
  Get a  boolean parameter from the Metadata object.

  This method never throws an exception assuming that if the
  requested parameter is not found it is false.

  \param key keyword associated with requested metadata member.
  **/
  bool get_bool(const std::string key) const override{
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
  template <typename T> T get(const std::string key) const;
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
  template <typename T> T get(const char *key) const
  {
    try{
      T val;
      val=get<T>(std::string(key));
      return val;
    }catch(...){throw;};
  }
  /*!
  Get the boost::any container from the Metadata object.

  This method is mostly for Python bindings so that a generic get method
  can work in Python.

  \param key is the name tag of desired component.

  \exception - MetadataGetError if requested parameter is not found.
  */
  boost::any get_any(const std::string key) const {
    std::map<std::string,boost::any>::const_iterator iptr;
    iptr=md.find(key);
    if(iptr==md.end())
    {
      throw MetadataGetError(key,typeid(boost::any).name());
    }
    return iptr->second;
  };
  std::string type(const std::string key) const;
  template <typename T> void put(const std::string key, T val) noexcept
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
    md[std::string(key)]=aval;
    changed_or_set.insert(std::string(key));
  }
  void put(const std::string key, const double val) override
  {
      this->put<double>(key,val);
  };
  void put(const std::string key, const int val) override
  {
      this->put<int>(key,val);
  };
  void put(const std::string key, const bool val) override
  {
      this->put<bool>(key,val);
  };
  void put(const std::string key, const std::string val) override
  {
      this->put<std::string>(key,val);
  };
  void put_object(const std::string key, const pybind11::object val)
  {
      this->put<pybind11::object>(key,val);
  }
  void put_int(const std::string key,const int val)
  {
    this->put<int>(key,val);
  };
  void put_string(const std::string key,const std::string val)
  {
    this->put<std::string>(key,val);
  };
  void put_bool(const std::string key,const bool val)
  {
    this->put<bool>(key,val);
  };
  void put_double(const std::string key,const double val)
  {
    this->put<double>(key,val);
  };
  void put_long(const std::string key,const long val)
  {
    this->put<long>(key,val);
  };
  /*! Create or append to a chained string.
   *
   A chain conceptually is identical to a list of string data.
   We implement it in Metadata because sometimes (e.g. MongoDB
   interaction and some constructs like the unix shell PATH variable)
   handling a full scale container like list<std::string> would be
   awkward.   If more extensive capability like that is needed it would
   be better to add a class that inherits Metadata and does so.
   AntelopePf more or less does this, for example, handling Tbl sections.
   In any case, this usage is more for one word strings separated by
   a common separator:  e.g. path=/usr/local/bin:/bin uses : as
   the separator.   /usr/local/bin and /bin are the chain.

   If the key related to the chain does not yet exist it is silently
   created.  If it already exists and we append to it.

   \param key is the key that defines the string.
   \param val is the the new string to append to the chain
   \param separator is the string used for a separator (default ":")

   \exception MsPASSError will be thrown if data is found in key
     and it is not of type string.
   */
  void append_chain(const std::string key, const std::string val,
		  const std::string separator=std::string(":"));

  /*! Return the keys of all altered Metadata values. */
  std::set<std::string> modified() const
  {
      return changed_or_set;
  };
  /*! \brief Mark all data as unmodified.
   *
   * There are situations where it is necessary to clear the
   * data structure used to mark changed metadata.  The best
   * example know is when data objects interact with a database
   * and try to do updates.   Effort can be wasted in unnecessary
   * updates if metadata are improperly marked as modified.
   * This method clears the entire container that defines
   * changed data.
   * */
  void clear_modified()
  {
	  changed_or_set.clear();
  };
  /*! Return all keys without any type information. */
  std::set<std::string> keys() const noexcept;
  /*! Test if a key has an associated value.  Returns true if
   * a value is defined. */
  bool is_defined(const std::string key) const noexcept;
  /*! Overload for C string*/
  /*
  bool is_defined(const char* key) const noexcept
  {
    return this->is_defined(string(key));
  };
  */
  /*! Clear data associated with a particular key. */
  void erase(const std::string key);
  /*! Overload for C string*/
  /*
  void erase(const char* key)
  {
    return this->erase(string(key));
  };
  */
  /*! Return the size of the internal map container. */
  std::size_t size() const noexcept;
  /*! Return iterator to beginning of internal map container. */
  std::map<std::string,boost::any>::const_iterator  begin() const noexcept;
  /*! Return iterator to end of internal map container. */
  std::map<std::string,boost::any>::const_iterator  end() const noexcept;
  /*! \brief Change the keyword to access an attribute.

  Sometimes it is useful to change the key used to access a particular piece
  of data.   Doing so, for example, is one way to implement an alias
  (alternative name) for something.  The entry for the old key is
  copied to a entry accessible by the new key.   The entry for old is
  then deleted.  This avoids downstream inconsistencies a the cost of
  possible failures from the translation.  This method always returns
  and will silently do nothing if old is not defined. If the new key is
  already defined, its content will be replaced by the old's.

  \param oldkey is the key to search for to be changed
  \param newkey is the new key to use for the replacement.
  */
  void change_key(const std::string oldkey, const std::string newkey);
  friend std::ostringstream& operator<<(std::ostringstream&, mspass::utility::Metadata&);
  /*! Serialize Metadata to a python bytes object.

  This function is needed to support pickle in the python interface.
  It cast the C++ object to a Python dict and calls pickle against that
  dict directly to generate a Python bytes object. This may not be the 
  most elegant approach, but it should be bombproof.

  \param md is the Metadata object to be serialized
  \return pickle serialized data object.
  */
  friend pybind11::object serialize_metadata_py(const Metadata &md);
  /*! Unpack serialized Metadata.
  *
  This function is the inverse of the serialize function.   It recreates a
  Metadata object serialized previously with the serialize function.  

  \param sd is the serialized data to be unpacked
  \return Metadata derived from sd
  */
  friend Metadata restore_serialized_metadata_py(const pybind11::object &sd);
protected:
  std::map<std::string,boost::any> md;
  /* The keys of any entry changed will be contained here.   */
  std::set<std::string> changed_or_set;
};
template <typename T> T Metadata::get(const std::string key) const
{
  T result;
  std::map<std::string,boost::any>::const_iterator iptr;
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
/*! Return a pretty name from a boost any object.
 *
 * We use a boost::any object as a container to hold any generic object.
 * The type name is complicated by name mangling.  This small function
 * returns a human readable type name.
 *
 * \param val is the boost::any container to be checked for type.
 * \return demangled name of type of the entity stored in the container.
 * */
std::string demangled_name(const boost::any val);
/*   Start of helper procedures for Metadata. */
/*! \brief Define standard types for Metadata.

Attributes in Metadata here can be any type that boost::any supports.
However, 99% of attributes one normally wants to work with can be
cast into the stock language types defined by this enum.   This is
derived form seispp in antelope contrib but adapted to the new form with
boost::any.   */
enum class MDtype{
    Real,
    Real32,
    Double,
    Real64,
    Integer,
    Int32,
    Long,
    Int64,
    String,
    Boolean,
    Double_Array,
    Invalid
};
/*! \brief Used in Metadata to defined type of Metadata associated with
a given tag.
**/
typedef struct Metadata_typedef {
    std::string tag; /*!< Name attached to this item.*/
    MDtype mdt; /*!< Type of this item. */
} Metadata_typedef;

/*! Container to drive selected copies.

Often it is necessary to define a list of Metadata elements
that are to be copied or accessed sequentially.  This is common
enough we use this typedef to reduce the ugly syntax.  */
typedef std::list<Metadata_typedef> MetadataList;

/*! \brief Procedure to copy a subset of a container of Metadata.

It is often useful to do a selective copy of the contents of a Metadata
container.  e.g. the function ExtractComponent creates a scalar time
series object from a three component seismogram extracting a single component.
It would make no sense to copy attributes related to the orientation of all
three components in the copy.   Programs using this feature should build
the MetadataList at startup to define the subset.   See related procedures
that create one of them.   (Not presently a class because the MetadataList is
just a simple std::list container.)

\param mdin is the container to retrieve attributes from (commonly a dynamic_cast
from a data object).
\param mdout is the output Metadata (also commonly a dynamic_cast from a data object.)
\param mdlist is the list that defines the subset to copy from mdin to mdout.

\return number of items copied

\exception will throw an MsPASSError if the input is missing one of the attributes
  defined in mdlist or if there is a type mismatch.  This means the copy
  will be incomplete and not trusted.   Handlers need to decide what to
  do in this condition.
*/
int copy_selected_metadata(const Metadata& mdin, Metadata& mdout,
        const MetadataList& mdlist);

/*! Serialize Metadata to a string.
This function is needed to support pickle in the python interface.
It is called in the pickle definitions in the wrapper for objects using
Metadata to provide a way to serialize the contents of the Metadata
object to a string.   The data that string contains is expected to
restored with the inverse of this function called restore.
Serialized output is readable with each entry on one line with
this format:
key type value
where type is restricted to double, long, bool, and the long C++ name for
an std::string.  Currently this:
std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >
Note any entry not of the four supported types will generate an error message
posted to stderr.   That is an ugly approach, but an intentional design
decision as this function should normally be called only pickling
methods for data objects.   Could see no solution to save errors in
that environment without throwing an exception and aborting the processing.
\param md is the Metadata object to be serialized
\return std::string of serialized data.
*/
std::string serialize_metadata(const Metadata &md);
/*! Unpack serialized Metadata.
 *
This function is the inverse of the serialize function.   It recreates a
Metadata object serialized previous with the serialize function.  Note it
only supports basic types currently supported by mspass:  long ints, double,
boolean, and string.  Since the output is assumed to be form serialize we
do not test for validity of the type assuming serialize didn't handle
anything else.
\param sd is the serialized data to be unpacked
\return Metadata derived from sd
*/
Metadata restore_serialized_metadata(const std::string);

Metadata restore_serialized_metadata_py(const pybind11::object &sd);
} // end utility namespace
}  //End of namespace MsPASS
#endif
