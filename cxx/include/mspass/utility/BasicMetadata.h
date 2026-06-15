#ifndef _BASICMETADATA_H_
#define _BASICMETADATA_H_
#include <string>

namespace mspass {
namespace utility {
/*! \brief Abstract base class for Metadata concept.

A core idea in MsPASS is the idea of a generic header that allows storage and
retrieval of arbitrary attributes.   This base class forces support for
the standard basic data types.
*/
class BasicMetadata {
public:
  /*! Virtual destructor for safe deletion through BasicMetadata pointers. */
  virtual ~BasicMetadata() {};
  /*! Return an integer-valued metadata attribute.
   *
   * \param key metadata key to fetch.
   * \exception MetadataGetError or another MsPASSError-derived exception when
   * the key is undefined or cannot be represented as an int.
   */
  virtual int get_int(const std::string key) const = 0;
  /*! Return a floating-point metadata attribute.
   *
   * \param key metadata key to fetch.
   * \exception MetadataGetError or another MsPASSError-derived exception when
   * the key is undefined or cannot be represented as a double.
   */
  virtual double get_double(const std::string key) const = 0;
  /*! Return a boolean metadata attribute.
   *
   * \param key metadata key to fetch.
   * \exception MetadataGetError or another MsPASSError-derived exception when
   * the key is undefined or cannot be represented as a bool.
   */
  virtual bool get_bool(const std::string key) const = 0;
  /*! Return a string metadata attribute.
   *
   * \param key metadata key to fetch.
   * \exception MetadataGetError or another MsPASSError-derived exception when
   * the key is undefined or cannot be represented as a string.
   */
  virtual std::string get_string(const std::string key) const = 0;
  /*! Store a double-valued metadata attribute.
   *
   * \param key metadata key to create or replace.
   * \param val value to store.
   */
  virtual void put(const std::string key, const double val) = 0;
  /*! Store an integer-valued metadata attribute.
   *
   * \param key metadata key to create or replace.
   * \param val value to store.
   */
  virtual void put(const std::string key, const int val) = 0;
  /*! Store a boolean metadata attribute.
   *
   * \param key metadata key to create or replace.
   * \param val value to store.
   */
  virtual void put(const std::string key, const bool val) = 0;
  /*! Store a string metadata attribute.
   *
   * \param key metadata key to create or replace.
   * \param val value to store.
   */
  virtual void put(const std::string key, const std::string val) = 0;
};
} // namespace utility
}; // namespace mspass
#endif
