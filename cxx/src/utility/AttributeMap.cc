#include <sstream>
#include "mspass/Metadata.h"
#include "AttributeMap.h"
namespace mspass{
using namespace std;
using namespace mspass;
// AttributeProperties encapsulate concepts about what a piece
// of metadata is.  It was designed originally with attributes
// extracted from a relational database as the model.  The
// AttributeProperties object, however, should be more general
// than this as it can support things like lists ala antelope tbls.
AttributeProperties::AttributeProperties()
{
	db_attribute_name="none";
	db_table_name="";
	internal_name="NULL";
	mdt=MDstring;
}
AttributeProperties::AttributeProperties(const string st)
{
	const string white(" \t\n");
	int current=0,next;
	int end_current;
	string mdtype_word;
	string bool_word;
	string stmp;  // use temporary to allow string edits
	const string emess("AttributeProperties(string) constructor failure:  failure parsing the following string:\n");

	if(st.empty())
		throw MsPASSError(string("AttributeProperties constructor failure;  Constructor was passed an empty string\n"));

	// this should find first nonwhite position
	//INCOMPLETE needs error checking for missing values

	// create a copy of st and remove any leading white
	// space at the head of the string
	stmp = st;
	if((current=stmp.find_first_not_of(white,0)) != 0)
	{
		stmp.erase(0,current);
		current = 0;
	}
	end_current=stmp.find_first_of(white,current);
	if(end_current<0) throw MsPASSError(string(emess+st));
	internal_name.assign(stmp,current,end_current-current);
	current = stmp.find_first_not_of(white,end_current);
	if(current<0) throw MsPASSError(string(emess+st));
	end_current=stmp.find_first_of(white,current);
	if(end_current<0) throw MsPASSError(string(emess+st));
	db_attribute_name.assign(stmp,current,end_current-current);
	current = stmp.find_first_not_of(white,end_current);
	if(current<0) throw MsPASSError(string(emess+st));
	end_current=stmp.find_first_of(white,current);
	if(end_current<0) throw MsPASSError(string(emess+st));
	db_table_name.assign(stmp,current,end_current-current);
	current = stmp.find_first_not_of(white,end_current);
	if(current<0) throw MsPASSError(string(emess+st));
	end_current=stmp.find_first_of(white,current);
	if(end_current<0) end_current=stmp.length();
	mdtype_word.assign(stmp,current,end_current-current);
	if(mdtype_word=="REAL" || mdtype_word=="real")
		mdt=MDreal;
	else if(mdtype_word=="INT" || mdtype_word=="int"
		|| mdtype_word=="integer")
		mdt = MDint;
	else if(mdtype_word=="STRING" || mdtype_word=="string")
		mdt=MDstring;
	else if(mdtype_word=="BOOL" || mdtype_word=="bool"
		|| mdtype_word=="BOOLEAN" || mdtype_word=="boolean")
		mdt=MDboolean;
	else
	{
		mdt = MDinvalid;
		throw MsPASSError(string("AttributeProperties(string) constructor:  Unrecognized metadata type = ")+mdtype_word);
	}
	// optional is_key field.  Defaulted false
	is_key = false;

	current = stmp.find_first_not_of(white,end_current);
	if(current>=0)
	{
		end_current=stmp.find_first_of(white,current);
		if(end_current<0) end_current=stmp.length();
		bool_word.assign(stmp,current,end_current-current);
		if(bool_word=="yes" || bool_word=="true"
			|| bool_word=="TRUE")  is_key=true;
	}
}
AttributeProperties::AttributeProperties(const AttributeProperties& apin)
{
	db_attribute_name=apin.db_attribute_name;
	db_table_name=apin.db_table_name;
	internal_name=apin.internal_name;
	mdt = apin.mdt;
	is_key = apin.is_key;
}
AttributeProperties& AttributeProperties::operator=(const AttributeProperties& apin)
{
	if(&apin==this)return(*this);
	db_attribute_name=apin.db_attribute_name;
	db_table_name=apin.db_table_name;
	internal_name=apin.internal_name;
	mdt = apin.mdt;
	is_key = apin.is_key;
	return(*this);
}
string AttributeProperties::fully_qualified_name() const
{
	string result;
	result=db_table_name + "." + db_attribute_name;
	return(result);
}
AttributeProperties& operator<<(ostream& ofs, const AttributeProperties& d)
{
    ofs<<d.db_attribute_name<<" "<<d.db_table_name<<" "
        <<d.fully_qualified_name()<<" "
        <<d.internal_name<<" ";
    switch(d.mdt)
    {
        case MDint:
            ofs<< "integer"<<endl;
            break;
        case MDreal:
            ofs<< "real"<<endl;
            break;
        case MDboolean:
            ofs<< "boolean"<<endl;
            break;
        case MDstring:
            ofs<< "string"<<endl;
            break;
        case MDinvalid:
            ofs<< "DEFINED_INVALID"<<endl;
            break;
        default:
            ofs<< "ERROR - illegal data"<<endl;
            throw MsPASSError(string("AttributeProperties::operator<<:  ")
                    + "MDtype of object is improperly defined.\n"
                    +"Likely memory problem as this should not happen");
    };
}


// Antelope version of this constructor had this signature.
//AttributeMap::AttributeMap(Pf *pf,string name)
/* This is the primary constructor for this object that currently uses
   an antelope pf format.   The map desired is nested inside a Pf 
   Arr with tag name.   This allows multiple maps to be stored in a 
   single file.   In mspass we might one day want to add a constructor
   that would use mongodb to store maps like this. */
AttributeMap::AttributeMap(const AntelopePf &pf,const string name)
{
	const string base_error("AttributeMap AntelopePf constructor:  ");
	typedef map<string,AttributeProperties> APMAP;
	try{
	  AntelopePf pfnested=pf.get_branch(name);
		list<string> attbl=pf.get_tbl("Attributes");
		if(attbl.size()<=0) throw MsPASSError(base_error
			 + "Attributes table is empty. Is parameter file may be missing Attributes Tbl?");
		list<string>::iterator tptr;
		for(tptr=attbl.begin();tptr!=attbl.end();++tptr)
		{
			AttributeProperties ap(*tptr);
			attributes[ap.internal_name]=ap;
		}
		list<string> alias_tbl=pf.get_tbl("aliases");
		/* Assume the aliasmap container is propertly initialized in the (possible)
		condition that the aliases section is empty. */
		for(tptr=alias_tbl.begin();tptr!=alias_tbl.end();++tptr)
		{
			string key,token;
			istringstream in(*tptr);
			in>>key;
			if(key.length()<=0)
			{
				throw MsPASSError(base_error + "illegal line in aliases table ->"
			             + (*tptr) +"<-\nFirst token must be internal name key");
			}
			/* We allow multiple aliases for the same internal name key.   Hence
			we have to parse this whole line */
			in>>token;
			if(token.length()<=0) throw MsPASSError(base_error
			   + "Empty list of aliases in this input line\n" +(*tptr));
			list<string> aliaslist;
			while(!in.eof())
			{
				aliaslist.push_back(token);
				in>>token;
			}
		}
	}catch(...){throw;};
}
AttributeMap::AttributeMap(const string schema)
{
	const string base_error("AttributeMap constructor:");
	const string pfname("attribute_maps.pf");
	string datadir,pfdir,pffile;
	try{
                mspass::datadir=mspass::data_directory();
		pfdir=datadir+"/pf";
		pffile=pfdir+pfname;
		AntelopePf pfall_maps(pffile);
		*this=AttributeMap(pfall_maps,schema);
	}catch(...){throw;};
}
/* The default constructor can be implemented as a special case of core
constructor that specifies a specific schema.  What is defined as default is
hard coded here though */
AttributeMap::AttributeMap()
{
	const string DEFAULT_SCHEMA_NAME("css3.0");
	try{
		(*this)=AttributeMap(DEFAULT_SCHEMA_NAME);
	}catch(...){throw;};
}
AttributeMap& AttributeMap::operator=(const AttributeMap& am)
{
	if(this!=&am)
	{
		attributes = am.attributes;
		aliasmap=am.aliasmap;
	}
	return (*this);
}
AttributeMap::AttributeMap(const AttributeMap& am)
{
	attributes = am.attributes;
	aliasmap=am.aliasmap;
}
bool AttributeMap::is_alias(const string key) const
{
	if(aliasmap.size()==0) return false;
	if(aliasmap.find(key)==aliasmap.end()) return false;
	return true;
}
map<string,AttributeProperties> AttributeMap::aliases(const string key) const
{
	map<string,AttributeProperties> result;
	/* reverse logic a bit odd, but cleanest solution */
	if(!this->is_alias(key))
		throw MsPASSError("AttributeMap::aliases method: "
		 + string("Attribute ") + key
		 + string(" is not defined as an alias") );
	else
	{
                map<string,list<string>>::const_iterator aptr;
                /* We don't need to test if this find works because is_alias
                   will not return true unless it is found */
                aptr=aliasmap.find(key);
		list<string> aml(aptr->second);
		map<string,AttributeProperties>::const_iterator amiter;
		list<string>::const_iterator listiter;
		for(listiter=aml.begin();listiter!=aml.end();++listiter)
		{
			amiter=attributes.find(*listiter);
			if(amiter==attributes.end())
			  throw MsPASSError("AttributeMap::aliases method: "
				+ string("Attribute named ")
				+ (*listiter)
				+ string(" is not defined for this AttributeMap"));
                        /* We need to copy this AttributeProperty and
                         * change the internal_name to the alias name */
                        AttributeProperties alias_property(amiter->second);
                        alias_property.internal_name=key;
			result[amiter->second.db_table_name]=alias_property;
		}
	}
	/* note this silently returns an empty list if key is not alias*/
	return(result);

}
/* This code has very strong parallels to aliases because they do similar
things even though they return very different results. */
list<string> AttributeMap::aliastables(const string key) const
{
	list<string> result;
	if(!this->is_alias(key))
		throw MsPASSError("AttributeMap::aliastables method: "
		 + string("Attribute ") + key
		 + string(" is not defined as an alias") );
	else
	{
                map<string,list<string>>::const_iterator aliasptr;
                aliasptr=aliasmap.find(key);
                /* We don't have to test that aliasptr is valid because
                   the is_alias method test assures the find will yield 
                   a valid iterator*/
                list<string> aml(aliasptr->second);
		map<string,AttributeProperties>::const_iterator amiter;
		list<string>::iterator listiter;
		for(listiter=aml.begin();listiter!=aml.end();++listiter)
		{
			amiter=attributes.find(*listiter);
			if(amiter==attributes.end())
			  throw MsPASSError("AttributeMap::aliastables method: "
				+ string("Attribute named ")
				+ (*listiter)
				+ string(" is not defined for this AttributeMap"));
			result.push_back(amiter->second.db_table_name);
		}
	}
	return(result);
}
AttributeProperties AttributeMap::operator[](const std::string key) const
{
    map<std::string,AttributeProperties>::const_iterator amptr;
    amptr=attributes.find(key);
    if(amptr==attributes.end())
    {
        throw MsPASSError(string("AttributeMap::opertor[]: key=")
                + key + " has not entry in this AttributeMap");
    }
    else
    {
        return amptr->second;
    }
}


} // End mspass Namespace declaration
