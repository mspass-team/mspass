
#ifndef __CORE_DECON_OPERATOR_H__
#define __CORE_DECON_OPERATOR_H__
#include "mspass/utility/Metadata.h"
namespace mspass::algorithms::deconvolution {
class BasicDeconOperator {
public:
  virtual void changeparameter(const mspass::utility::Metadata &md) = 0;
  virtual ~BasicDeconOperator() {};
};
} // namespace mspass::algorithms::deconvolution
#endif
