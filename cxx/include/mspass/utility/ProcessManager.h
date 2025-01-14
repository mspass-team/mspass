#ifndef _PROCESS_MANAGER_H_
#define _PROCESS_MANAGER_H_
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_serialize.hpp>
#include <map>
#include <vector>
namespace mspass {
namespace utility {
/*! \brief Lightweight data structure to completely describe an algorithm.

Processing data always involves application of one or more algorithms.
Most algorithms have one to many parameters that define the algorithm's
detailed behavior.  Because the size of parametric input can sometimes
be huge MsPASS needed a way to carry a concise summary of algorithms applied
to data.   The issue is complicated by the fact that the same algorithm
may be applied to data at diffrent stages with different parameters (e.g.
bandpass filters applied before and after deconvolution).  To address this
problem we save input parameters for any instance of an algorithm in MongoDB
as a document in the history collection.  This implementation uses a
simple string to define a particular instance.  In the current implementation
of MsPASS that is set as the string representation of the ObjectID defined
for that document.   An alternative would be UUID as used in TimeSeries and
Seismogram objects, but we prefer the ObjectID string here as it is a guaranteed
unique key by MongoDB, always has an index defined for the collection, and
reduces the size of the history collection by not requiring a uuid attribute.

Any algorithm for which no optional parameters are needed will have the id
field empty.

A special case is readers that act as origins from "raw"  which may not
literally be "raw" but is just a signal that a reader initiated a history
chain.   For readers input_type is set to "NotApplicable" and output_type
is to be defined for that reader.  Readers may or may not have control
parameters.
*/
class AlgorithmDefinition {
public:
  /*! Default constructor.

  This consructor is realy the same as what would be automatically generated.
  We define it to be clear and because I think pybind11 may need this
  declaration to allow a python wrapper for a default constructor.  */
  AlgorithmDefinition() : nm(), myid(), input_type(), output_type() {};
  /*! Primary constructor.

  This constructor sets the two primary attributes of this object.
  name is a descriptive (unique) name assigned to the algorithm and
  id is a unique id key.   In MsPASS it is the string representation of the
  ObjectID assigned to the MongoDB document holding the parameter data
  that defines this instance of an algorithm.

  \param name is the algorithm's (unique) name
  \param id is a unique id string defining the parameters that were used for
    this instance of an algorithm.
  */
  AlgorithmDefinition(const std::string name, const std::string typin,
                      const std::string typout, const std::string id) {
    nm = name;
    myid = id;
    input_type = typin;
    output_type = typout;
  };
  AlgorithmDefinition(const AlgorithmDefinition &parent) {
    nm = parent.nm;
    myid = parent.myid;
    input_type = parent.input_type;
    output_type = parent.output_type;
  };
  std::string name() const { return nm; };
  /*! \brief return the id as a string.

  In MsPASS the id is normally a MongoDB ObjectID string representation of
  the documnt saved in the database that holds the paramters defining a
  particular algorithm instance.  If the algorithm has no parameters
  this string will be null. Callers should test that condition by calling
  the length method of std::string to verify the id is not zero length */
  std::string id() const { return myid; };
  /*! Set a new id string.

  The id straing is used to define a unique instance of an algorithm
  for a particular set of parameters.  This is the only putter for this
  class because it is the only attribute that should ever be changed
  after construction.  The reason is the name and type constraints are
  fixed, but id defines a particular instance that may be variable. */
  void set_id(const std::string id) { myid = id; };
  // void set_name(const string name){nm=name;};
  AlgorithmDefinition &operator=(const AlgorithmDefinition &parent) {
    if (this == &parent) {
      nm = parent.nm;
      myid = parent.myid;
      input_type = parent.input_type;
      output_type = parent.output_type;
    }
    return *this;
  };

private:
  std::string nm;
  std::string myid;
  std::string input_type;
  std::string output_type;
  friend boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & nm;
    ar & myid;
  };
};
class ProcessManager {
public:
  ProcessManager();
  ProcessManager(std::string fname);
  AlgorithmDefinition algorithm(const std::string name,
                                const size_t instance = 0) const;
  std::string jobname() const { return jobnm; };
  std::string jobid() const { return boost::uuids::to_string(job_uuid); };
  /*! \brief Get a new UUID to define unique job run.

  MsPASS data objects are tagged with a UUID to properly handle
  processing history.  Procedures can call his method to get a
  uuid based on boost's random_generator.   We keep the generator
  in this object because web conversations suggest construction of
  random_generator is expensive in time and needs to not be done
  at the object level.

  \return new uuid definign this job in string form.
  */
  std::string new_newid() {
    boost::uuids::uuid id;
    id = gen();
    return boost::uuids::to_string(id);
  }

private:
  std::string jobnm;
  boost::uuids::uuid job_uuid;
  boost::uuids::random_generator gen;
  std::map<std::string, std::vector<AlgorithmDefinition>> algs;
  friend boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & jobnm;
    ar & job_uuid;
    ar & algs;
  };
};
} // namespace utility
} // namespace mspass
#endif
