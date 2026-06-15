
#ifndef __CORE_DECON_OPERATOR_H__
#define __CORE_DECON_OPERATOR_H__
#include "mspass/utility/Metadata.h"
namespace mspass::algorithms::deconvolution {
/*! \brief Abstract interface for deconvolution operators configurable by Metadata. */
class BasicDeconOperator {
public:
  /*! \brief Update operator parameters from a Metadata container. */
  virtual void changeparameter(const mspass::utility::Metadata &md) = 0;
  /*! Virtual destructor for derived deconvolution operators. */
  virtual ~BasicDeconOperator() {};
};
} // namespace mspass::algorithms::deconvolution
#endif
