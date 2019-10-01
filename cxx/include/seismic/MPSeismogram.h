#include "Seismogram.h"
#include "MsPASSAddOn.h"
class MPSeismogram : public Seismogram : public MsPASSAddOn
{
  public:
      MPSeismogram();
      MPSeismogram(const MPSeismogram& parent);
      MPSeismogram& operator=(const MPSeismogram& parent);
      MPSeismogram(MongoDBHandle& h);  //unclear if we can do this just yet
};

