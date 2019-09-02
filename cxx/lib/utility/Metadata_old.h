enum MDtype {MDreal, /*!< Attribute is a real (floating point) number */
    MDint, /*!< Attribute is an integer. */
    MDstring, /*!< Attribute is a character string. */
    MDboolean, /*!< Attribute is a boolean (true/false). */
    MDinvalid  /*!< Attribute type is not recognized (an error code). */
};
/*!
\brief Used in Metadata to defined type of Metadata associated with
a given tag.
**/
typedef struct Metadata_typedef {
	string tag; /*!< Name attached to this item.*/
	enum MDtype mdt; /*!< Type of this item. */
} Metadata_typedef;

/*!
// Some components of the Metadata object are driven by
// this STL list.
**/
typedef list<Metadata_typedef> MetadataList;


/*! \brief Object to hold auxiliary parameters referenced by a keyword.
*
*  Ancillary data (Metadata) are a common need in data processing.
*  Most data objects have a set of parameters required to define to all such
*  objects (e.g. sample rate in a time series).  With real data of
*  any kind there are always ancillary parameters that are required
*  for one algorithm but are not needed by another.  A common solution
*  to this is traditional data processing is a header like that used
*  in seismic data processing.  A Metadata object can be conveniently
*  thought of as a generalized header.  Data can be put into the
*  Metadata object with constructors and additional data added or replaced by
*  various put methods.  Data is extract with get methods.
*
*  This object supports only the standard data types:  integers,
*  real numbers, booleans, and strings.  The expectation is if one
*  requires a more complicated objects to be associated with another
*  through this mechanism one can readily extend this object by
*  standard inheritance mechanisms.  It is important to note that
*  data are stored internally in STL map objects indexed by
*  a keyword.  Reals, ints, and booleans are stored in the machines
*  native form in these maps.  For each of them if a parameter is
*  requested by one of the typed get methods it looks first in the
*  typed container.  If it is not there, it then tries the string
*  container and throws an exception if that parameter is not their
*  either.
*
**/

class Metadata
{
public:
/*!
// Default constructor.  Does nothing but build an empty object.
**/
        Metadata(){};
/*!
//  Restricted build from a string driven by a typed list.
//
// Similar to the string constructor, but only Metadata
// defined by a list (mdl) are copied.
//
//\param s string to be compiled into Metadata
//\param mdl MetadataList object containing list of items to be copied
//  from string to Metadata contents.  Any contents of the input string
//  s that do not match keys found in this list will be dropped.
**/
	Metadata(const string s,const MetadataList& mdl);
/*!
// Standard copy constructor.
//
//\param mdold - parent object to be copied
**/
	Metadata(const Metadata& mdold);

/*! Standard assignment operator.
  \param mdold - parent object to copy
  */
	Metadata& operator=(const Metadata& mdold);
	// In this implementation destructor can be defaulted.
	// There is thus no need to declare it.
        // ~Metadata();

/*!
// Get a real number from the Metadata object.
//
//\exception MetadataGetError if requested parameter is not found.
//\param key keyword associated with requested metadata member.
**/
        double get_double(const string key) throw(MetadataGetError);
/*!
// Get an integer from the Metadata object.
//
//\exception MetadataGetError if requested parameter is not found.
//\param key keyword associated with requested metadata member.
**/
        int get_int(const string key)throw(MetadataGetError);
/*!
// Get a long integer from the Metadata object.
//
//\exception MetadataGetError if requested parameter is not found.
//\param key keyword associated with requested metadata member.
**/
        long get_long(const string key)throw(MetadataGetError);
/*!
// Get a string from the Metadata object.
//
// Note the string in this case can be quite large.  If the string
// was parsed from an Antelope Pf nested Tbl and Arrs can be extracted
// this way and parsed with pf routines.
//
//\exception MetadataGetError if requested parameter is not found.
//\param key keyword associated with requested metadata member.
**/
        string get_string(const string key)throw(MetadataGetError);
/*!
// Get a  boolean parameter from the Metadata object.
//
// This method never throws an exception assuming that if the
// requested parameter is not found it is false.
//
//\param key keyword associated with requested metadata member.
**/
        bool get_bool(const string key) throw(MetadataGetError);
/*! Generic get interface.

  This is a generic interface most useful for template procedures
  that need to get a Metadata component.   Since this object only
  can contain simple types the type requested must be simple.
  Currently supports only int, long, short, double, float, and string.
  C char* is intentionally not supported.
  Calls to anything but the supported types will
  throw an exception.

  \param key is the name tag of desired component.

  \exception - will throw a MetadataGetError (child of SeisppError) for
     type mismatch or in an overflow or underflow condition.
     */
      template <typename T> T get(const string key) throw(MetadataGetError);
      /*! \brief Generic get interface for C char array.

        This is a generic interface most useful for template procedures
        that need to get a Metadata component.   Since this object only
        can contain simple types the type requested must be simple.
        Currently supports only int, long, short, double, float, and string.
        C char* is intentionally not supported. This is largely a wrapper
        on the string key version of this same generic function.

        \param key is the name tag of desired component.

        \exception - will throw a MetadataGetError (child of SeisppError) for
           type mismatch or in an overflow or underflow condition.
           */
      template <typename T> T get(const char *key) throw(MetadataGetError)
      {
        try{
          T val;
          val=get<T>(string(key));
          return val;
        }catch(...){throw;};
      }
/*!
// Place a real number into the Metadata object.
//
// Inserts a new, real number parameter into the Metadata object.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const double val);
        void put(const char *key,const double val);
/*!
// Place a long integer into the Metadata object.
//
// Inserts a new, integer parameter into the Metadata object.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const long val);
        void put(const char *key, const long val);
/*!
// Place an integer into the Metadata object.
//
// Inserts a new, integer parameter into the Metadata object.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const int val);
        void put(const char *key,const int val);
/*!
// Place a boolean parameter into the Metadata object.
//
// Inserts a boolean parameter into the Metadata object.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const bool val);
        void put(const char *key,const bool val);
/*!
// Place a string parameter the Metadata object.
//
// Inserts a new string parameter into the Metadata object.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const string val);
        void put(const char *key,string val);
/*!
// Place a string parameter into the Metadata object.
//
// Inserts a new string parameter into the Metadata object.
// Differs from similar method with C++ string in that this passes
// a plan C char *.  The char * is converted to C++ string internally.
// If the parameter was present before it will be replaced.  If not
// it will be inserted.
//
//\param key keyword to be used to reference this parameter.
//\param val value to load.
**/
        void put(const string key,const char * val);
        void put(const char *key,const char * val);
/*! \brief Query to find out if an attribute is set.
//
// It is frequently necessary to ask if an attribute has been set.
// The get routines throw an exception if one tries to fetch an attribute
// that is not defined, which is always trouble.  Rather than depend
// on exception handlers, which is bad form, a program that cannot
// be certain an attribute is defined should call this method instead
// of using an error handler.  It is both faster and better form.
//
// Note the algorithm used is independent of type simply searching
// the containers that hold each type stored by this object.
//
// \param key attribute to be test.
*/
	bool is_attribute_set(const string key);
/*! \brief Query to find out if an attribute is set.
//
// It is frequently necessary to ask if an attribute has been set.
// The get routines throw an exception if one tries to fetch an attribute
// that is not defined, which is always trouble.  Rather than depend
// on exception handlers, which is bad form, a program that cannot
// be certain an attribute is defined should call this method instead
// of using an error handler.  It is both faster and better form.
// This overloaded form is a convenience for testing using char constants
// (the standard result of a string between double quotes.).
//
// \param key attribute to be test.
*/
	bool is_attribute_set(const char *val);
/*!
// Delete a parameter from the Metadata object.
//
//\param key keyword tagging parameter to be removed.
**/
	void remove(const string key);
/*!
// Appends a string to an existing string value with a separator.
//
// It is frequently useful to append a new string to an existing
// string variable stored in a Metadata object.  This can be used,
// for example to build up file names.  A real example in this
// library at the moment is that this is used to accumulate filter
// parameters when multiple filters are cascaded on data in the
// TimeInvariantFilter object.
//
// This could be done by a get and put, but this automates the process.
// Note that if the key passed was not present in the Metadata object
// before this method is called the separator is ignored and only the
// third argument becomes the value associated with key.
//
//\param key keyword to access string.
//\param separator string used to separate new string from previous
//   contents.  Note that if the key passed was not present in the Metadata object
//   before this method is called the separator is ignored and only the
//   third argument becomes the value associated with key.
//\param appendage this string is appended to the current contents subject to
//   special case noted above for separator parameter.
**/
	void append_string(const string key, const string separator, const string appendage);
/*!
// Output function to a standard stream.
//
// Output format is an Antelope parameter file.
**/
	friend ostream& operator<<(ostream&,Metadata&);
/*!
// Return a list of keys and associated types.
**/
	MetadataList keys();
protected:
	// Typed methods use appropriate map first.  If the
	// key is not found in the typed version they try to
	// fetch from mstring and convert
	map<string,double> mreal;
	map<string,long> mint;
	map<string,bool> mbool;
	map<string,string> mstring;
private:
        friend class boost::serialization::access;
        template<class Archive>
            void serialize(Archive & ar, const unsigned int version)
        {
            ar & mreal;
            ar & mint;
            ar & mbool;
            ar & mstring;
        };
};
/* Anything but specializations of this template (found in Metadata.cc)  will lead
   to an exception - unsupported type*/
template <typename T> T Metadata::get(string key) throw(MetadataGetError)
{
  const string base_error("Metadata generic get template: ");
  throw MetadataGetError(typeid(T).name(),key,base_error+"Unsupported type");
}

//
// Helpers
//
/*!
// Copy only selected Metadata from one Metadata object to another.
//
//\param mdin input Metadata (copy from here).
//\param mdout target to which desired Metadata is to be copied.
//\param mdlist object containing a typed list of Metadata components
//  to copy from mdin to mdout.
**/
void copy_selected_metadata(Metadata& mdin, Metadata& mdout,
	MetadataList& mdlist) throw(MetadataError);
/*!
// Build a MetadataList from a parameter file.
//
// This is essentially a constructor for the MetadataList structure.
//
//\param pf pointer to Pf to be parsed (normally produced by pfread
//   of a parameter file.
//\param tag key of Tbl in Pf holding the list.
**/
MetadataList pfget_mdlist(Pf *pf,const string tag);
/*!
// Convert a Metadata to an Antelope Pf.  This is essentially
// an inverse to the Pf constructor.
//
//\param md Metadata to be converted.
//\return Antelope parameter file Pf pointer.
**/
Pf *Metadata_to_pf(Metadata& md);
/*!  \brief Extract an antelope Pf Tbl into a string.

  Antelope pf files have the concept of a Tbl grouping of stuff
  that is commonly parsed by programs for data that is not a simple
  single value type.   This procedure finds a Tbl with a specified
  tag and extracts the Tbl contents into a string which is returned.

\param pf  is the Antelope Pf pointer
\param tag is the tag for the Tbl to be extracted

\return string of Tbl contents.
*/
string pftbl2string(Pf *pf, const char *tag);
/*!  \brief Extract an antelope Pf Tbl into a list of strings.

  Antelope pf files have the concept of a Tbl grouping of stuff
  that is commonly parsed by programs for data that is not a simple
  single value type.   This procedure finds a Tbl with a specified
  tag and extracts the Tbl contents into list container.   Each
  string in this list is defined by newlines in the original Tbl of
  the pf file.  Said another way the basic algorithm is a gettbl for
  each line in the Tbl followed by a push_back to the STL list.

\param pf  is the Antelope Pf pointer
\param tag is the tag for the Tbl to be extracted

\return STL list container of std::string objects derived from tbl lines.
*/
list<string> pftbl2list(Pf *pf, const char *tag);
/*! \brief Saves a specified list of Metadata components to an antelope db.

  Any object that is a child of Metadata can find this procedure useful.
  It saves a list of metadata to a specified database table.
  NOTE VERY IMPORTANT ASSUMPTION:  the Datascope db pointer must have
  the record field set to the correct insertion point or this procedure
  will throw an exception and fail.

  \param md - Metadata from which the data are to be extracted
  \param db - Antelope Dbptr of table to which these are to be saved
  \param table - name of the table where the data are to be written
            (Used for consistency check - db must point to this table)
  \param mdl - list of metadata to write to db
  \param am - internal to external name mapping object

  \exception - SeisppError object will be throw for a variety of
     problems.  Most common is if the name is not defined on one
     side or the other (i.e. ask to save something not stored in
     metadata or name requested to save is not defined in table. )
     */
void save_metadata_for_object(Metadata& md,Dbptr db, string table,
        MetadataList& mdl, AttributeMap& am);
