#include "mspass/utility/ProcessManager.h"
#include "mspass/utility/MsPASSError.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <fstream>
#include <map>
#include <sstream>
using namespace std;
namespace mspass::utility {
ProcessManager::ProcessManager() : jobnm() { job_uuid = gen(); };
ProcessManager::ProcessManager(string fname) {
  const string base_error("ProcessManager file constructor:  ");
  try {
    job_uuid = gen();
    ifstream ifs;
    ifs.open(fname.c_str(), std::ifstream::in);
    if (!ifs.is_open())
      throw MsPASSError(base_error + "fopen failed for file=" + fname,
                        ErrorSeverity::Fatal);
    /* job names needs to be the first line */
    ifs >> jobnm;
    /* Now we loop over lines with algorithm name and and idstring */
    char inp[128];
    while (ifs.getline(inp, 128)) {
      map<string, vector<AlgorithmDefinition>>::iterator aptr;
      stringstream ss(inp);
      string algnm, idin, inptyp, outtyp;
      ss >> algnm;
      ss >> idin;
      ss >> inptyp;
      ss >> outtyp;
      AlgorithmDefinition thisalg(algnm, inptyp, outtyp, idin);
      aptr = algs.find(algnm);
      if (aptr == algs.end()) {
        vector<AlgorithmDefinition> temp;
        temp.push_back(thisalg);
        algs.insert(pair<string, vector<AlgorithmDefinition>>(algnm, temp));
      } else {
        aptr->second.push_back(thisalg);
      }
    }
  } catch (...) {
    throw;
  };
};
AlgorithmDefinition ProcessManager::algorithm(const string name,
                                              const size_t instance) const {
  size_t i;
  map<string, vector<AlgorithmDefinition>>::const_iterator aptr;
  aptr = algs.find(name);
  if (aptr == algs.end()) {
    /* If the name is not registered the map we don't want this
    method to throw an error.  Instead we return the name with
    the id field set to UNDEFINED*/
    return AlgorithmDefinition(name, "UNDEFINED", "UNDEFINED", "UNDEFINED");
  } else {
    i = instance;
    if (instance >= (aptr->second.size())) {
      cerr << "ProcessManager::algorithm(Warning):  "
           << "algorithm=" << name
           << " was defined but requested instance=" << instance
           << " exceeds number of defined instances=" << aptr->second.size()
           << endl
           << "Set to id for instance=" << aptr->second.size() - 1 << endl
           << "History data may be invalid" << endl;
      i = aptr->second.size() - 1;
    }
  }
  return aptr->second[i];
}
} // namespace mspass::utility
