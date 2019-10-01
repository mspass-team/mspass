#ifndef _ANTELOPE_PF_H_
#define _ANTELOPE_PF_H_

#include <list>
#include <string>
#include "Metadata.h"
namespace mspass{
/*! key for accessing Tbl and Arr entries.
 *
 These are of use only when this object is converted to a Metadata
 object using the ConvertToMetadata method.   This object holds
 a representation of Tbl and Arr sections of the input pffile as 
 in map containers that are converted by that method and stored 
 with the boost::any capability.   This allows them to be retrieved if
 desired with these keys and the correct type specification.   
 */
const string pftbl_key("AntelopePfTbl");
const string pfarr_key("AntelopePfArr");
/*! \brief C++ object version of a parameter file.

   This object encapsulates the Antelope concept of a parameter
file in a single wrapper.   The main constructor is actually
act much like the Antelope pfread procedure.
   Internally this object does not use an antelope Pf at all
directly, but it is a child of Metadata.  Simple attributes
(i.e. key-value pairs) are posted directly to the Metadata 
associative array (map) container.  Note that the
parser attempts to guess the type of each value given in the
obvious ways (periods imply real numbers, e or E imply real
numbers, etc.) but the algorithm used may not be foolproof. The get
methods are from the Metadata object.  Be warned like the Metadata 
object the type of an entry for a key can change and will be the last
one set.   
    An Antelope Tbl in a pf file is converted to an stl list
container of stl::string's that contain the input lines.  This is
a strong divergence from the tbl interface of Antelope, but one
I judged a reasonable modernization.   Similarly, Arr's are
converted to what I am here calling a "branch".   Branches
are map indexed containers with the key pointing to nested
versions of this sampe object type. This is in keeping with the way
Arr's are used in antelope, but with an object flavor instead of
the pointer style of pfget_arr.   Thus, get_branch returns a
AntelopePf object instead of a pointer that has to be memory
managed.
    A final note about this beast is that the entire thing was
created with a tacit assumption the object itself is not huge.
i.e. this implementation may not scale well if applied to very
large (millions) line pf files.  This is a job for something like 
MongoDB.   View this as a convenient format for building a Metadata
object.  Note, a code fragment to use this to create lower level
metadata would go like this:
  AntelopePf pfdata("example");   //parameter file constructor
  Metadata md(dynamic_cast<Metadata&>(pfdata);

\author Gary L. Pavlis, Indiana University

This version for MsPASS is derived from a similar thing called a 
PfStyleMetadata object in antelope contrib.   
*/
class AntelopePf : public Metadata
{
public:
    /*! Default constructor - does nothing.*/
    AntelopePf():Metadata(){};
    /*! \brief Construct from a base pf name.

    This constructor acts like the antelope pfread function for
    parameter files constructed in plain C.   That is, the argument is
    a base name sans the .pf extension.   Like antelope's pfread it
    will follow the chain of directories defined by PFPATH.   As with
    the antelope pfread procedure the last file read takes precendence.
    Note if PFPATH is not defined the path defaults to ".".
    Further if pfbase begins with a slash (i.e. "/") it is assumed to be
    the actual file name to read and the PFPATH feature is disabled. 

      \param pfbase is a the base pf name (always adds .pf to each name in PFPATH)

      \exception - throws a MsPASSError object with an informative message if
        constuctor fails for any reason.
        */
    AntelopePf(string pfbase);
    /*! \brief Construct from a set of text lines.

      This is a cruder constructor that could be used for small pf file.
      Read the contents of a pf file into a vector of lines with each 
      line being one line from the pf.  The constructor then sequentially parses
      this series of lines as it would if reading from a file.

      \param lines is the modified version of the text pf file.*/
    AntelopePf(list<string> lines);
    /*!  Standard copy constructor. */
    AntelopePf(const AntelopePf& parent);
    /*! \brief get a Tbl component by key.

      Antelope has the idea of a tbl, which is a list of
      lines that are parsed independently.  This is the
      get method to extract one of these by its key.

      \param key is the key for the Tbl desired.
      \exception AntelopePfError will be thrown if the key
         is not present. */
    list<string> get_tbl(const string key) const;
    /*! \brief used for subtrees (nested Arrs in antelope)

       This method is used for nested Arr constructs.
       This returns a copy of the branch defined by key
       attached to this object.   The original from the parent
       is retained.

       \param key is the key used to locate this branch.
       \returns a copy of the contents of the branch linked to key.

       \exception AntelopePfError will be thrown if the key is not
            found. */
    AntelopePf get_branch(const string key) const;
    /*! Return a list of keys for branches (Arrs) in the pf file. */
    list<string> arr_keys() const;
    /*! Return a list of keys for Tbls in the pf.*/
    list<string> tbl_keys() const;
    /*! \brief Return an object with only simple name:value pairs.
     *
     The Metadata parent of this object only handles name:value pairs.
     The values can, however, be any object boost::any can handle.  
     The documentation says that means it is copy constructable.  
     For now this method returns an object containin the boost::any 
     values.   Any Arr and Tbl entries are pushed directly to the 
     output Metadata using boost::any and the two keys defined as 
     const strings at the top of this file (pftbl_key and pfarr_key).   

     \return Metadata sans any Arr and Tbl entries.
     */
    Metadata ConvertToMetadata();
    /*! Standard assignment operator, */
    AntelopePf& operator=(const AntelopePf& parent);
    /*! \brief save result in a pf format.

       This is functionally equivalent to the Antelope pfwrite
       procedure, but is a member of this object.   A feature of
       the current implementation is that all simply type parameters
       will usually be listed twice in the output file.   The reason
       is that the constructor attempts to guess type, but to allow
       for mistakes all simple parameters are also treated as string
       variables so get methods are more robust.

       \param ofs is a std::ostream (e.g. cout) where the result
          will be written in pf style.   Usually should end in ".pf".
          */
    void pfwrite(ostream& ofs);
private:
    map<string,list<string> > pftbls;
    /* This is used for nested Arrs */
    map<string, AntelopePf> pfbranches;
    /* This method is used to implemetn PFPATH - it is called for
    each secondary file in the PFPATH chain.   Returns the number of
    items changed */
    int merge_pfmf(AntelopePf& m);
};
/*! \brief Error class for AntelopePf object.

  This error object is similar to that for Metadata but tags
  all errors cleanly as originating from this child of Metadata.
  Note MsPASSError is a child of std::exception, so catch that
  to most easily fetch messages coming from this beast.
  */
class AntelopePfError : public MsPASSError
{
    public:
        AntelopePfError(){
            message=string("AntelopePfError->undefined error");
        };
        AntelopePfError(string mess)
        {
            message="AntelopePfError object message="+mess;
        };
        AntelopePfError(const char *mess)
        {
            message=string("AntelopePfError object message=")+mess;
        };
        void log_error(){
            cerr << message<<endl;
        };
};
/* Procedural functions using AntelopePf object */

/*! \brief Build a MetadataList using AntelopePf object.

A convenient format to define a MetadataList is an Antelope Pf file.  
This procedure creates a MetadataList from an AntelopePf object 
(generated from a pf file) keying on tag that defines a pf Tbl& 
section. 

\param m is the PfStyleMetadata object where you expect to find the list.
\param tag is the unique tag on the Tbl in the original Pf containing the
  data defining the MetadataList.

  */
MetadataList get_mdlist(const mspass::AntelopePf& m, const std::string tag);

} // End mspass namespace declaration
#endif
