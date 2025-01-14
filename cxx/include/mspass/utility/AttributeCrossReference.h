#ifndef _ATTRIBUTE_CROSS_REFERENCE_
#define _ATTRIBUTE_CROSS_REFERENCE_
#include "mspass/utility/AttributeMap.h"
#include "mspass/utility/Metadata.h"
#include <map>
#include <set>
#include <string>
namespace mspass {
namespace utility {
/*! \brief Cross reference between external and internal names.

  Data formats commonly have a frozen namespace with which people
  are very familiar.  An example is SAC where scripts commonly manipulate
  header attribute by a fixed set of names.  For good reasons one may
  want to use a different naming convention internally in a piece of
  software that loads data using an external format but wishes to use a
  different set of names internally.  This object simplifies the task
  of managing the differences in internal and external names */
class AttributeCrossReference {
public:
  /*! Default constructor.

    The default constructor is a pure placeholder that
    does nothing.  Result is a null namespace mapper.*/
  AttributeCrossReference() {};
  /*! Construct from a string.

    This constructor assumes the string variable passed is a
    complete image of a set of (newline separated) lines
    defining the mapping.  The format of each line is
    assumed to be:  internal_name external_name type.
    As the names imply "internal_name" is the name to use
    internally and "external_name" is the foramt specific external
    name.  The "type" variable is generic and should be one of the
    simple keywords real, int, string, or boolean.

    \param lines_to_parse is the (multiline) string in the
    format described above.

    \exception MsPASSError will be thrown for parsing errors. */
  AttributeCrossReference(const std::string lines_to_parse);
  /*! Construct from a list container.

  This constructor is nearly identical to the single string with
  newline constructor.  The list elements are expected to be
  the contents of each line (newline break) for the string version.

  \lines list container with input lines in same format as that
     described above for single string constructor.
  \exception MsPASSError will be thrown if there are parsing errors.
  */
  AttributeCrossReference(const std::list<std::string> &lines);

  /*! Build for a set of STL containers.

    This is lower level constructor that effectively builds this
    object from a set of components that are used to actually
    implement the concept.
    \param internal2external is an associative array keyed by
      the internal name that defines external names linked to
      each internal name.
    \param mdlist is a MsPASS::MetadataList object defining the
      complete internal namespace. */
  AttributeCrossReference(
      const std::map<std::string, std::string> internal2external,
      const mspass::utility::MetadataList &mdlist);
  /*! Standard copy constructor. */
  AttributeCrossReference(const AttributeCrossReference &parent);
  /*! Get internal name for attribute with external name key.*/
  std::string internal(const std::string key) const;
  /*! Get external name for attribute with internal name key.*/
  std::string external(const std::string key) const;
  /*! Get type information for attribute with internal name key.*/
  MDtype type(const std::string key) const;
  /*! Standard assignment operator. */
  AttributeCrossReference &operator=(const AttributeCrossReference &parent);
  /*! Return number of entries in the cross reference map. */
  int size() const;
  /*! Add a new entry to the map.

    This method is used to extend the namespace.

    \param intern is the internal name
    \param ext is the external name to be added. */
  void put(const std::string intern, const std::string ext);
  /*! Return the set of internal names defined by this object.

    Returns an std::set container of strings that are the internal
    names defined by this object. */
  std::set<std::string> internal_names() const;
  /*! Return the set of external names defined by this object.

    Returns an std::set container of strings that are the external
    names defined by this object. */
  std::set<std::string> external_names() const;

private:
  std::map<std::string, std::string> itoe;
  std::map<std::string, std::string> etoi;
  /* keyed by internal names.  Get type of this attribute*/
  std::map<std::string, MDtype> imdtypemap;
};
} // namespace utility
} // namespace mspass
#endif
