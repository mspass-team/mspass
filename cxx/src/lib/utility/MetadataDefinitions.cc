#include <fstream>
#include <sstream>
#include <string.h>
#include <string>
#include "mspass/utility/utility.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/AntelopePf.h"
#include "yaml-cpp/yaml.h"
#include "mspass/utility/MetadataDefinitions.h"
const string DefaultSchemaName("mspass");
namespace mspass{
using namespace mspass;
using namespace std;
MetadataDefinitions::MetadataDefinitions(const std::string mdname)
{
    try{
      /* silent try to recover if the user adds .yaml to mdname*/
      std::size_t ipos;  //size_t seems essential here for this to work -weird
      string name_to_use;
      ipos=mdname.find(".yaml");
      if(ipos==std::string::npos)
      {
	name_to_use=mdname;
      }
      /* We throw an exception if the name is a path with / characters*/
      else if(mdname.find("/")!=std::string::npos)
      {
	throw MsPASSError("MetadataDefinitions:  name passed seems to be a full path name that is not allowed\nReceived this:  "
		+mdname,ErrorSeverity::Invalid);
      }
      else
      {
        name_to_use.assign(mdname,0,ipos);
      }
      string datadir=mspass::data_directory();
      string path;
      path=datadir+"/yaml/"+name_to_use+".yaml";
      MetadataDefinitions tmp(path,MDDefFormat::YAML);
      *this=tmp;
    }catch(...){throw;};

}
/* The unparameterized constructor is almost like the single string constructor
 * loading a frozen file name. The only difference is not needing to worry
 * about user errors.*/
MetadataDefinitions::MetadataDefinitions()
{
    try{
      string datadir=mspass::data_directory();
      string path;
      path=datadir+"/yaml/"+DefaultSchemaName+".yaml";
      MetadataDefinitions tmp(path,MDDefFormat::YAML);
      *this=tmp;
    }catch(...){throw;};
}

MetadataDefinitions::MetadataDefinitions(const string fname,const MDDefFormat mdf)
{
  try{
    switch(mdf)
    {
      case MDDefFormat::YAML:
        this->yaml_reader(fname);
        break;
      case MDDefFormat::PF:
        this->pfreader(fname);
        break;
      default:
        throw MsPASSError("MetadataDefinitions file constructor:   illegal format specification");
    };
  }catch(...){throw;};
}
MetadataDefinitions::MetadataDefinitions(const MetadataDefinitions& parent)
  : tmap(parent.tmap),cmap(parent.cmap),
  aliasmap(parent.aliasmap),
  alias_xref(parent.alias_xref),roset(parent.roset),
  unique_id_data(parent.unique_id_data)
{}
bool MetadataDefinitions::is_defined(const std::string key) const noexcept
{
  /* test type map because concept can be empty for a key */
  map<string,MDtype>::const_iterator tptr;
  tptr=tmap.find(key);
  if(tptr!=tmap.end())
  {
    return true;
  }
  else
  {
    pair<string,MDtype> unr;
    /* A bit weird to catch an exception as a way to test for a false, but
    the way the api currently is defined requires this. */
    try{
      unr=this->unique_name(key);
      return true;
    }catch(MsPASSError& mderr)
    {
      return false;
    }
  }
}
std::string MetadataDefinitions::concept(const std::string key) const
{
  const string base_error("MetadataDefinitions::concept:  ");
  map<string,string>::const_iterator cptr;
  cptr=cmap.find(key);
  if(cptr==cmap.end())
  {
    stringstream ss;
    ss<<base_error<<"no match for key="<<key<<" found"<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
  return cptr->second;
}

mspass::MDtype MetadataDefinitions::type(const std::string key) const
{
  const string base_error("MetadataDefinitions::type:  ");
  map<std::string,mspass::MDtype>::const_iterator tptr;
  tptr=tmap.find(key);
  if(tptr==tmap.end())
  {
    stringstream ss;
    ss<<base_error<<"no match for key="<<key<<" found"<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
  return tptr->second;
}

void MetadataDefinitions::add(const std::string key, const std::string concept_, const MDtype mdt)
{
  cmap[key]=concept_;
  tmap[key]=mdt;
}
bool MetadataDefinitions::has_alias(const std::string key) const
{
  multimap<string,string>::const_iterator aptr;
  aptr=aliasmap.find(key);
  if(aptr==aliasmap.end())
    return false;
  else
    return true;
}
list<std::string> MetadataDefinitions::aliases(const std::string key) const
{
  list<std::string> result;
  multimap<string,string>::const_iterator aptr;
  aptr=aliasmap.find(key);
  if(aptr==aliasmap.end())  return result;  //return an empty list when not found
  std::pair<multimap<string,string>::const_iterator,
              multimap<string,string>::const_iterator> rng;
  rng=aliasmap.equal_range(key);
  /* Obscure iterator loop over return of equal_range.   See multimap documentation*/
  for(aptr=rng.first;aptr!=rng.second;++aptr)
  {
    result.push_back(aptr->second);
  }
  return result;
}
void MetadataDefinitions::add_alias(const std::string key, const std::string aliasname)
{
  /* We could use operator[] but this is more bombproof if not as clear */
  aliasmap.insert(std::pair<string,string>(key,aliasname));
  alias_xref.insert(std::pair<string,string>(aliasname,key));
}
std::pair<std::string,mspass::MDtype> MetadataDefinitions::unique_name
                                  (const string aliasname) const
{
    const string base_error("MetadataDefinitions::unique_name:  ");
    map<std::string,std::string>::const_iterator aptr;
    aptr=alias_xref.find(aliasname);
    if(aptr==alias_xref.end())
    {
      throw MsPASSError(base_error+"alias name="
          + aliasname + " is not defined",ErrorSeverity::Invalid);
    }
    else
    {
      /* We do not assume the key returned from alias_xref resolves. Small
      cost for a stability gain*/
      string kname=aptr->second;
      map<std::string,MDtype>::const_iterator tptr;
      tptr=tmap.find(kname);
      if(tptr==tmap.end()) throw MsPASSError(base_error+"alias name="+aliasname
        + " has no matching entry in alias tables\n"
        + "SETUP ERROR - FIX CONFIGURATION FILES",
        ErrorSeverity::Fatal);
      return(pair<std::string,mspass::MDtype>(kname,tptr->second));
    }
}
std::list<std::string> MetadataDefinitions::keys() const
{
  /* assume tmap and cmap have the same keys*/
  std::list<std::string> result;
  map<string,mspass::MDtype>::const_iterator tptr;
  for(tptr=tmap.cbegin();tptr!=tmap.cend();++tptr)
  {
    result.push_back(tptr->first);
  }
  return result;
}
MetadataDefinitions& MetadataDefinitions::operator=(const MetadataDefinitions& parent)
{
  if(this!=&parent)
  {
    tmap=parent.tmap;
    cmap=parent.cmap;
    aliasmap=parent.aliasmap;
    alias_xref=parent.alias_xref;
    roset=parent.roset;
    unique_id_data=parent.unique_id_data;
  }
  return *this;
}
MetadataDefinitions& MetadataDefinitions::operator+=(const MetadataDefinitions& other)
{
  if(this==&other) return *this;
  list<string> kvals=other.keys();
  list<string>::iterator kptr;
  for(kptr=kvals.begin();kptr!=kvals.end();++kptr)
  {
    MDtype mdt=other.type(*kptr);
    /* Note this will silently overwrite previous if the key was already present*/
    this->tmap[*kptr]=mdt;
    try{
      string cother=other.concept(*kptr);
      this->cmap[*kptr]=cother;
    }catch(MsPASSError& merr)
    {
      //Assume the only error here comes from concept methode failing
      cerr << "MetadataDefinitions operator+= (Warning): concept description is missing for key="
         <<*kptr<<endl<<"Error will be ignored"<<endl;
    }
    /* The alias_xref is a multimap so we just append other data - no such thing as duplicates*/
    map<string,string>::const_iterator aptr;
    for(aptr=other.alias_xref.begin();aptr!=other.alias_xref.end();++aptr)
    {
      this->alias_xref.insert(*aptr);
    }
    /* These behave like the type map above - we silently replace any entry
    that was present before.  i.e. other overrides */
    set<string>::const_iterator sptr;
    for(sptr=other.roset.begin();sptr!=roset.end();++sptr)
    {
      this->roset.insert(*sptr);
    }
    map<string,tuple<string,string>>::const_iterator uptr;
    for(uptr=other.unique_id_data.begin();uptr!=other.unique_id_data.end();++uptr)
    {
      this->unique_id_data.insert(*uptr);
    }
  }
  return *this;
}
/* We assume the readonly list is smaller so the set of defined
keys is those for readonly.  That inverts the logic of this function.
i.e. roset contains keys of attributes marked readonly and this function
is the not of that logic. */
bool MetadataDefinitions::writeable(const string key) const
{
  set<string>::const_iterator roptr;
  roptr=roset.find(key);
  if(roptr==roset.end())
    return true;
  else
  {
    /* roset only contains unique key entries.  This checks any possible
    aliases. */
    pair<string,MDtype> kp;
    /* unique_name method returns an exception if the key is not defined.
    We avoid that and silently return false if that happens, although
    that situation will likely create downstream problems. */
    try{
      kp=this->unique_name(key);
    }catch(MsPASSError &mderr)
    {
      cerr << "MetadataDefinitions::writeable method (WARNING):  Requested key is undefined"<<endl
         <<"This may cause downstream problems"<<endl;
      return false;
    }
    roptr=roset.find(kp.first);
    if(roptr==roset.end())
      return true;
    else
      return false;
  }
}
/* This function is a thin wrapper for writeable for efficiency */
bool MetadataDefinitions::readonly(const string key) const
{
  return( ! (this->writeable(key)));
}
void MetadataDefinitions::set_readonly(const string key)
{
  /* Silently return if key is already set as readonly */
  if(roset.find(key)==roset.end())
  {
    roset.insert(key);
  }
}
void MetadataDefinitions::set_writeable(const string key)
{
  set<string>::const_iterator roptr;
  roptr=roset.find(key);
  /* Here we silently do nothing if the key is not in roset */
  if(roptr!=roset.end())
  {
    roset.erase(roptr);
  }
}
bool MetadataDefinitions::is_normalized(const string key) const
{
  if(unique_id_data.find(key)!=unique_id_data.end())
    return true;
  else
    return false;
}
/* The normalization is stored with the 0 element of the tuple being the
table(collection) name and the 1 element being the key for the id needed */
string MetadataDefinitions::unique_id_key(const string key) const
{
  map<string,tuple<string,string>>::const_iterator uidptr;
  uidptr=unique_id_data.find(key);

  if(uidptr!=unique_id_data.end())
  {
    return(get<1>(uidptr->second));
  }
  else
  {
    return (string(""));
  }
}
string MetadataDefinitions::collection(const string key) const
{
  map<string,tuple<string,string>>::const_iterator uidptr;
  uidptr=unique_id_data.find(key);
  if(uidptr==unique_id_data.end())
  {
    return (string(""));
  }
  else
  {
    return(get<0>(uidptr->second));
  }
}
std::pair<std::string,std::string> MetadataDefinitions::normalize_data(const string key) const
{
  map<string,tuple<string,string>>::const_iterator uidptr;
  uidptr=unique_id_data.find(key);
  if(uidptr==unique_id_data.end())
  {
    throw MsPASSError("MetadataDefinitions::normalize_data:  key="
      + key + " has no normalization data");
  }
  else
  {
    pair<string,string> result;
    result.first=get<0>(uidptr->second);
    result.second=get<1>(uidptr->second);
    return result;
  }
}
/* Helper for below */
/* This function takes a list of lines form a tbl in  pf, adds a newline at
end of each element, and appends to to a master returning one string with
newlines marking the list item boundaries. */
std::string list_to_1str(list<std::string>& l)
{
  list<std::string>::iterator lptr;
  string result;
  for(lptr=l.begin();lptr!=l.end();++lptr)
  {
    result += (*lptr);
    result += "\n";
  }
  return result;
}
/* Small helper used by parsers.   Note tstr can't be const because it is
altered, but because it is called by value I don't think the caller would
be modified copy could be modified. */
MDtype str2mdt(string tstr)
{
  transform(tstr.begin(), tstr.end(), tstr.begin(), ::tolower);
  MDtype mdt_this;
  if( (tstr=="real") || (tstr=="float") || (tstr=="real32") )
    mdt_this=MDtype::Real32;
  else if(tstr=="int32")
    mdt_this=MDtype::Int32;
  else if((tstr=="real64") || (tstr=="double"))
    mdt_this=MDtype::Double;
  /* in the 64 bit world we default int and integer to int64 */
  else if((tstr=="long") || (tstr=="int64") || (tstr=="int") || (tstr=="integer") )
    mdt_this=MDtype::Int64;
  else if(tstr=="string")
    mdt_this=MDtype::String;
  else
    throw MsPASSError("MetadataDefinitions::pfreader:  type value="
      + tstr+" not recognized");
  return mdt_this;
}
/* Private methods */
void MetadataDefinitions::pfreader(const string pfname)
{
  try{
    /* Most of these may throw a MsPASSError.   Any failure for this class
    is bad so we just do one catch at the end.  A program is expected to
    normally create this thing near the start of execution so aborting on failure
    is the expected norm */
    AntelopePf pf(pfname);
    list<string> akeys=pf.arr_keys();
    list<string>::iterator kptr,aptr;
    for(kptr=akeys.begin();kptr!=akeys.end();++kptr)
    {
      AntelopePf pfb(pf.get_branch(*kptr));
      list<string> con_list;
      con_list=pfb.get_tbl("concept");
      string con_str=list_to_1str(con_list);
      cmap[*kptr]=con_str;
      string tstr=pfb.get_string("type");
      MDtype mdt_this=str2mdt(tstr);
      tmap[*kptr]=mdt_this;
      /* parse aliases as a tbl linked to this key */
      list<string> alist=pfb.get_tbl("aliases");
      for(aptr=alist.begin();aptr!=alist.end();++aptr)
      {
        this->add_alias(*kptr,*aptr);
      }
    }
  }catch(...){throw;};
}
void MetadataDefinitions::yaml_reader(const string fname)
{
  try{
    YAML::Node outer=YAML::LoadFile(fname.c_str());
    /* The structure of yaml file is a map with a group key
     * for each piece.  We ignore the group key here and use it
     * only to make the file more human readable.  */
    //const YAML::Node& attributes=outer["Attributes"];
    for(YAML::const_iterator it=outer.begin();it!=outer.end();++it)
    {
	    string group_key=it->first.as<string>();
	    const YAML::Node& attributes=outer[group_key];
    	unsigned int natt=attributes.size();
    	unsigned int i;
    	for(i=0;i<natt;++i)
    	{
      	string key;
      	key=attributes[i]["name"].as<string>();
      	string concept=attributes[i]["concept"].as<string>();
      	cmap[key]=concept;
      	string styp=attributes[i]["type"].as<string>();
      	MDtype mdt_this=str2mdt(styp);
      	tmap[key]=mdt_this;
	      /* Aliases is optional - this skips parsing aliases if key is missing*/
	      if(attributes[i]["aliases"])
        {
          string str=attributes[i]["aliases"].as<string>();
          if(str.size()>0)
          {
	        /* using strtok which will alter the string contents so we have
	        to copy it first*/
            char *s=strdup(str.c_str());
	          string delim(" ,");  // allow either spaces or commas as delimiters
	          char *p=strtok(s,delim.c_str());
	          while(p!=NULL){
	            this->add_alias(key,string(p));;
	            p=strtok(NULL,delim.c_str());  //strtok oddity of NULL meaning use last position
            }
	          free(s);
          }
        }
        /* We parse this set of parameters only when readonly is set */
        if(attributes[i]["readonly"])
        {
          string str=attributes[i]["readonly"].as<string>();
          /* Break out of this if tagged false - user error handled silently. */
          if(str=="false" || str=="False") continue;
          roset.insert(key);
          string uid, tbl;
          uid=attributes[i]["unique_id"].as<string>();
          tbl=attributes[i]["collection"].as<string>();
          std::tuple<string,string> entry(std::make_tuple(tbl,uid));
          unique_id_data[key]=entry;
        }
      }
    }
  }catch(YAML::Exception& eyaml)
  {
    /* Rethrow these as a MsPASSError */
    throw MsPASSError(eyaml.what(),ErrorSeverity::Invalid);
  }
  catch(...){throw;};
}
}
