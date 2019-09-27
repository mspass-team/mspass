#include <string>
#include "mspass/MsPASSError.h"
namespace mspass{
/* Standardizes top level directory for mspass */
std::string data_directory()
{
	const std::string mspass_home_envname("MSPASS_HOME");
	char *base;
	/* Note man page for getenv says explicitly the return of getenv should not
			be touched - i.e. don't free it*/
	base=getenv(mspass_home_envname.c_str());
	if(base==NULL)throw MsPASSError(string("maspass_data_directory procedure:  ")
	    + "required environmental variable="+mspass_home_envname+" is not set");
        std::string datadir;
	datadir=std::string(base)+"/data";
	return datadir;
}
}

