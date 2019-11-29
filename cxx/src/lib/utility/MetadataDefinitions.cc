#include <fstream>
#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/MetadataDefinitions.h"
namespace mspass{
using namespace mspass;
using std::string;
/* This is a temporary implementation to test a theory for why this created a
seg fault*/
MetadataDefinitions::MetadataDefinitions()
{
  /*
  MetadataDefinitions temp(string("obspy_namespace.pf"),MDDefFormat::PF);
  *this=temp;
  */
}
MetadataDefinitions::MetadataDefinitions(const std::string mdname)
{
throw MsPASSError("MetadataDefinitions constructor form namespace title not yet implemented");

}
MetadataDefinitions::MetadataDefinitions(const string fname,const MDDefFormat mdf)
{
  try{
    switch(mdf)
    {
      case MDDefFormat::PF:
        this->pfreader(fname);
      case MDDefFormat::SimpleText:
        this->text_reader(fname);
    };
  }catch(...){throw;};
}
MetadataDefinitions::MetadataDefinitions(const MetadataDefinitions& parent)
  : tmap(parent.tmap),cmap(parent.cmap), aliasmap(parent.aliasmap), alias_xref(parent.alias_xref)
{}
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
  }
  return *this;
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
    list<string>::iterator kptr;
    for(kptr=akeys.begin();kptr!=akeys.end();++kptr)
    {
      AntelopePf pfb(pf.get_branch(*kptr));
      list<string> con_list;
      con_list=pfb.get_tbl("concept");
      string con_str=list_to_1str(con_list);
      cmap[*kptr]=con_str;
      string tstr=pfb.get_string("type");
      MDtype mdt_this;
      if( (tstr=="real") || (tstr=="float") || (tstr=="real32") )
        mdt_this=MDtype::Real32;
      else if((tstr=="int") || (tstr=="integer") || (tstr=="int32"))
        mdt_this=MDtype::Int32;
      else if((tstr=="Real64") || (tstr=="double"))
        mdt_this=MDtype::Double;
      else if((tstr=="long") || (tstr=="int64"))
        mdt_this=MDtype::Int64;
      else
        throw MsPASSError("MetadataDefinitions::pfreader:  type value="
          + tstr+" not recognized");
      tmap[*kptr]=mdt_this;
    }
    /* now parse any aliases */
    list<string> alist=pf.get_tbl("aliases");
    for(kptr=alist.begin();kptr!=alist.end();++kptr)
    {
      stringstream ss(*kptr);
      string skey,salias;
      ss>>skey;
      ss>>salias;
      this->add_alias(skey,salias);
    }
  }catch(...){throw;};
}
void MetadataDefinitions::text_reader(const string pfname)
{
    throw MsPASSError("text_reader not yet implemented");
}

}
