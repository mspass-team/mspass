#ifndef _METADATA_MSPASS_H_
#define _METADATA_MSPASS_H_
#include "mspass/utility/Metadata.h"
/* Not sure this class is necessary.   I think we could have
int update(Somemongohandle);
int save(Someomongohandle);
*/
class MsPASSMetadata
{
public:
  MsPASSMetadata(DatabaseHandle& dbh, MetadataList& mdl);
  update();
  save();
private:
  /* Some version of this is essential to have the object point at the
  document to which it is associated.  Updates would be very difficult otherwise*/
  DatabaseHandle dbh;
}
#endif
