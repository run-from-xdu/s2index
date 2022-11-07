#include "ssindex.hpp"
#include "index_common.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Set(const KeyType &key, const ValueType &value) {

}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Get(const KeyType & key) -> ValueType {
    return KeyNotFound;
}

}  // namespace ssindex
