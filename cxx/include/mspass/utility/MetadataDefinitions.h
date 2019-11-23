#ifndef _METADATADEFINITIONS_H_
#define _METADATADEFINITIONS_H_
#include <map>
namespace mspass{
/*! \brief Define properties of Metadata known to mspass.

The metadata system in mspass was designed to be infinitely flexible.
However, to be maintainable and provide some stability we need a method to
define the full properties of the keys that define attributes known to
the system.  This object does that through wwhat I hope is a simple interface.
The expectation is the main use of this class is in python code that
will read and write to mongo.   Mongo is type sensitive but python tries hard to
by loosy goosy about types.   The main use is then expected that all gets and
puts to Metadata will be preceded by calls to the type method here.  Based on
the return the right get or put method can be called.

The overhead of creating this thing is not small.   It will likely be recommended
as an initialization step for most mspass processing scripts.   Ultimately it
perhaps should have a database constructor, but initially we will build it only from
data file.
*/
class MetadataDefinitions
{
public:
  /*! Default constructor - defaults to mspass namespace. */
  MetadataDefinitions()[];
  /*! \brief Construct using a specified attribute namespace.

  This constructor uses an alternative definition to the default namespace.
  It is an implementation detail (to be worked out) on where the data for
  any attribute namespace lives.

  \param mdname is a name tag used to define the attribute namespace.
  */
  MetadataDefinitions(const std::string mdname);
  /*! Standard copy constructor. */
  MetadataDefinitions(const MetadataDefinitions& parent);
  /*! Return a description of the concept this attribute defines.

  \param key is the name that defines the attribute of interest

  \return a string with a terse description of the concept this attribute defines.
  */
  std::string concept(const std::string key) const;
  /*! Get the type of an attribute.

  \param key is the name that defines the attribute of interest

  \return MDtype enum that can be used to establish the proper type.
  */
  mspass::MDtype type(const std::string key) const;
  /*! Basic putter.

  Use to add a new entry to the definitions.   Note that because this is
  implemented with C++ map containers if data for the defined key is
  already present it will be silently replaced.

  \param key is the key for indexing this attribute
  \param concept_ is brief description saved as the concept for this key
  \param type defines the type to be defined for this key.
  */
  void add(const std::string key, const std::string concept_, const MDtype mdt);
  /*! \brief Methods to handle aliases.

  Sometimes it is helpful to have alias keys to define a common concept.
  For instance, if an attribute is loaded from a ralational db one might
  want to use alias names of the form table.attribute as an alias to attribute.
  has_alias should be called first to establish if a name has an alias.
  To get a list of aliases call the aliases method.
  */
  bool has_alias(const std::string key) const;
  list<std::string> aliases(const std::string key) const;
  /*! Get definitive name for an alias.

  This method is used to ask the opposite question as aliases.  The aliases
  method returns all acceptable alternatives to a definitive name defined as
  the key to get said list.   This method asks what definitive key should be
  used to fetch an attribute and what it's type is.  It does this by returning
  an std::pair with first being the key and second the type.

  \param aliasname is the name of the alias for which we want the definitive key

  \return std::pair with the definitive key as the first of the pair and the type
  in the second field. */
  std::pair<std::string,mspass::MDtype> unique_name(const string aliasname) const;
  /*! Add an alias for key.

  \param key is the main key for which an alias is to be defined
  \param aliasname is the the alternative name to define.
  */
  int add_alias(const std::string key, const std::string aliasname);
  /*! Standard assignment operator. */
  MetadataDefinitions& operator=(const MetadataDefinitions& parent);
  /*! Accumulate additional definitions.   Appends other to current.
  Note that because we use the map container any duplicate keys in other
  will replace those in this.
  */
  MetadataDefinitions& operator+=(const MetadataDefinitions& other);
private:
  map<std::string,MDtype> tmap;
  map<std::string,string> cmap;
  multimap<std::string,std::string> aliasmap;
  map<string,string> alias_xref;
};
}  // end mspass namespace
#endif
