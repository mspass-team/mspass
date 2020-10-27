#include <mspass/utility/dmatrix.h>
namespace mspass {
namespace mspasspy {
using namespace mspass::utility;
/* The following is needed for the binding to access protected members */
class Publicdmatrix : public dmatrix {
public:
    using dmatrix::ary;
    using dmatrix::length;
    using dmatrix::nrr;
    using dmatrix::ncc;
};
} // namespace mspasspy
} // namespace mspass