#ifndef _METADATADEFINITIONS_H_
#define _METADATADEFINITIONS_H_
#include <map>
#include <tuple>
namespace mspass{
namespace utility{
enum class MDDefFormat
{
    PF,
    YAML
};

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
  /*! Default constructor.   Loads default schema name of mspass. */
  MetadataDefinitions();
  /*! \brief Construct from a namespace title.

  How this is to be implemented is to be determined, but for many uses a simple
  one line description of the name space for attributes would be a helpful api.
  At this time it is not implemented and attempts to use this will throw
  an exception.
  */
  MetadataDefinitions(const std::string mdname);
  /*! \brief Construct from a file with variable formats.

  This constructor reads from a file to build the object.  The API
  allows multiple formats through the enum class.

  \param mdname is the file to read
  \param form defines the format (limited by MDDefFormat definitions)
  */
  MetadataDefinitions(const std::string mdname,const mspass::utility::MDDefFormat form);
  /*! Standard copy constructor. */
  MetadataDefinitions(const MetadataDefinitions& parent);
  /*! Test if a key is defined either as a unique key or an alias */
  bool is_defined(const std::string key) const noexcept;
  /*! Return a description of the concept this attribute defines.

  \param key is the name that defines the attribute of interest

  \return a string with a terse description of the concept this attribute defines.
  */
  std::string concept(const std::string key) const;
  /*! Get the type of an attribute.

  \param key is the name that defines the attribute of interest

  \return MDtype enum that can be used to establish the proper type.
  */
  mspass::utility::MDtype type(const std::string key) const;
  std::list<std::string> keys() const;
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
  /*! \brief Ask if a key is a registered alias.
   *
   This asks the inverse question to has_alias.  That is, it yields
   true of the key is registered as a valid alias.  It returns false
   if the key is not defined at all.   Note it will yield false if
   the key is a registered unique name and not an alias.
   */
  bool is_alias(const std::string key) const;
  std::list<std::string> aliases(const std::string key) const;
  /*! Get definitive name for an alias.

  This method is used to ask the opposite question as aliases.  The aliases
  method returns all acceptable alternatives to a definitive name defined as
  the key to get said list.   This method asks what definitive key should be
  used to fetch an attribute and what it's type is.  It does this by returning
  an std::pair with first being the key and second the type.

  \param aliasname is the name of the alias for which we want the definitive key

  \return std::pair with the definitive key as the first of the pair and the type
  in the second field. */
  std::pair<std::string,mspass::utility::MDtype> unique_name(const std::string aliasname) const;
  /*! Add an alias for key.

  \param key is the main key for which an alias is to be defined
  \param aliasname is the the alternative name to define.
  */
  void add_alias(const std::string key, const std::string aliasname);
  /*! Check if a key:value pair is mutable(writeable). Inverted logic from
  similar readonly method.

  \param key is key used to access the parameter to be tested.
  \return true if the data linked to this not not marked readonly.
  (if the key is undefined a false is silently returned)
  */
  bool writeable(const std::string key) const;
  /*! Check if a key:value pair is marked readonly. Inverted logic
  of similar writeable method.

  \param key is key used to access the parameter to be tested.
  \return true of the data linked to this keys IS marked readonly.
  (if the key is undefined this method silently returns true)
  */
  bool readonly(const std::string key) const;
  /*! \brief Lock a parameter to assure it will not be saved.

  Parameters can be defined readonly.  That is a standard feature of this
  class, but is normally expected to be set on construction of the object.
  There are sometimes reason to lock out a parameter to keep it from being
  saved in output.  This method allows this.   On the other hand, use this
  feature only if you fully understand the downstream implications or you
  may experience unintended consequences.

  \param key is the key for the attribute with properties to be redefined.
  */
  void set_readonly(const std::string key);
  /*! \brief Force a key:value pair to be writeable.

  Normally some parameters are marked readonly on construction to avoid
  corrupting the database with inconsistent data defined with a common key.
  (e.g. sta)  This method overrides such definitions for any key so marked.
  It does nothing except a pointles search if the key hasn't been marked
  readonly previously.   This method should be used with caution as it could
  have unintended side effects.

  \param key is key for the attribute to be redefined.
  */
  void set_writeable(const std::string key);
  /*! \brief Test if a key:value pair is set as normalized.

  In MongoDB a normalized attribute is one that has a master copy in one and
  only one place.   This method returns true if an attribute is marked
  normalized and false otherwise (It will also return false for any key that
  is undefined.).
  */
  bool is_normalized(const std::string key) const;
  /*! \brief Returns a unique identifier for a normalized attribute.

  In MongoDB a normalized attribute is one that has a master copy in one and
  only one place.  This method returns a unique identifier, which defines
  a key that can be used to access the attribute used as an index to
  identify a unique location for an otherwise potentially ambiguous identifier
  (e.g. sta can be used in may contexts).  The type of attribute to which the
  returned key is linked is expected to normally be acquired by am immediate
  call to the type method of this class using the return key.  It is the callers
  responsibility to handle errors if the request for the type information fails.
  Note for MongoDB the most common (and recommended) type for the unique id is
  an Object_ID.   The design of the API, however, should not preclude some other
  index or an index oriented toward a relational database.  (e.g. chanid
  is an integer key with a one-to-one relation for channel data in the CSS3.0
  schema.)

  Some unique id specifications require a table/collection qualifier.   See
  related collection method that is designed to handle that.

  This method should normally be used only on read operations
  to select the correct entry for what could otherwise be a potentially ambiguous key.
  \param key is the flat namespace key for which normalizing data is needed

  \return  name for unique id for requested key.  Returns an empty string if
    the key is not defined as normalized.   In multiple calls it is more efficient
    to test for a null return and handle such entries inline instead of a double
    search required if preceded by is_normalized.
  */
  std::string unique_id_key(const std::string key) const;
  /*! \Brief return the master collection (table) for a key used as a unique id.

  Support for normalized Metadata requires static tables (collection in MongoDB)
  that contain the data using normalization.   In seismic data type examples are
  receiver location tables, receiver response tables, and source location data.
  This method should nearly always be paired with a call to unique_id_key.
  The idea is to first ask for the unique_id_key and then ask what collection (table)
  contains the key returned by unique_id_key.   This provides a fast and
  convenient lookup for normalized data.

  \param key is the normally the return from unique_id_key
  \return string defining the collection(table) the key can be used for locating the
     unique tuple/document required to access related Metadata.  String will be
     empty if the search fails.
  */
  std::string collection(const std::string key) const;
  /*! \brief Special method for efficiency.

  For mspass using mongodb normalization for all currently supported Metadata
  can be reduced to a collection(table)-attribute name pair.   The unique_id_key
  and collection methods can be called to obtained this information, but doing so
  requires a purely duplicate (internal map container) search.
  This convenience method is best used with MongoDB for efficiency.

  \param key is the flat namespace key for which normalizing data is needed
  \return an std::pair with of strings with first=collection and second=attribute name.
  */

  std::pair<std::string,std::string> normalize_data(const std::string key) const;
  /*! \brief Apply a set of aliases to data.

  This method should be called in processing workflows to apply a series
  of defined aliases to data.  The method uses the is_alias method to
  verify if an alias name is valid.  It will not change the key if the name is
  not defined as a valid alias.   The keys that fail that test will be
  posted to the std::list that is returned.   Callers should test the
  size of the return and if it is not empty take appropriate action.

    \param d is the data to alter (usually actually a TimeSeries of Seismogram)
    \param aliaslist is a list of aliases names to apply.

    \return std::list of srings of failed changes.  Callers should
     test the size of this return and take action if needed.
    */
std::list<std::string> apply_aliases(mspass::utility::Metadata& d,
          const std::list<std::string> aliaslist);
  /*! \brief Restore any aliases to unique names.

  Aliases are needed to support legacy packages, but can cause downstream
  problem if left intact.  This method clears any aliases and sets them
  to the unique_name defined by this object.

  \param d is data to be altered.  Normally a Seismogram of TimeSeries but
    can be a raw Metadata object.

  \return ErrorLogger containing any problems encountered.  This function
   always returns and treats any problems as nonfatal errors and posts
   a log entry into this returned object. Caller should test the size of
   the return and handle or ignore errors as appropriate.
   */
  void clear_aliases(mspass::utility::Metadata& d);
  /*! Standard assignment operator. */
  MetadataDefinitions& operator=(const MetadataDefinitions& other);
  /*!\brief Accumulate additional definitions.

  Appends data in other to current.   The behavior or this operator
  is not really a pure + operation as one would think of it as with
  arithmetic.   Because the model is that we are defining the properties of
  unique keys handling the case where other has a duplicate key to
  the instance of the left hand side is ambiguous.  We choose the
  more intuitive case where the right hand side overrides the left.
  i.e. if other has duplicate data for a key the right hand side
  version replaces the left.  There are two exceptions.   Aliases are
  multivalued so they accumulate.  i.e. this mechanism can be used to
  do little more than add an alias if the others data are the same.
  The second case is more trivial and rarely of importance.  That is,
  other can have empty concept data for a key and it will be silently
  set empty.   The reason is concept is purely for human readers
  and is not expected to ever be used by processors.

  */
  MetadataDefinitions& operator+=(const MetadataDefinitions& other);
private:
  std::map<std::string,MDtype> tmap;
  std::map<std::string,std::string> cmap;
  std::multimap<std::string,std::string> aliasmap;
  std::map<std::string,std::string> alias_xref;
  std::set<std::string> roset;
  /* This map is used to handle normalized data in any database.   For the
  initial design the data could be a pair, but I make it a tuple because I
  can conveive extensions that would require additional information to provides
  a unique index definition.   e.g.  the antelope indexing of sta,chan,time:endtime.
  */
  std::map<std::string,std::tuple<std::string,std::string>> unique_id_data;
  void pfreader(const std::string pfname);
  void yaml_reader(const std::string fname);

};
} // end utility namespace
}  // end mspass namespace
#endif
