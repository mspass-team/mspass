
#ifndef __CORE_DECON_OPERATOR_H__
#define __CORE_DECON_OPERATOR_H__
#include "mspass/utility/Metadata.h"
namespace mspass{
class BasicDeconOperator
{
public:
    virtual void changeparameter(const mspass::Metadata &md)=0;
    virtual ~BasicDeconOperator() {};
};
}
#endif
