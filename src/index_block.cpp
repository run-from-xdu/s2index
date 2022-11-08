#include "index_block.hpp"

namespace ssindex {

template<typename ValueType>
auto IndexBlock<ValueType>::GetValue(const IndexEdge<ValueType> & edge) const -> ValueType {
    return ValueType(-1);
}

template<typename ValueType>
auto IndexBlock<ValueType>::TryBuild(std::vector<IndexEdge<ValueType>> & edges,
              uint64_t seed,
              uint64_t fp_bits) -> Status {
    return Status::SUCCESS;
}

}  // namespace ssindex
