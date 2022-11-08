#include "ssindex.hpp"
#include "index_common.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Set(const KeyType &key, const ValueType &value) {
    memtable_.insert_or_assign(key, value);
    if (memtable_.size() == MemtableFlushThreshold) {
        /// TODO: flush and reset memtable
    }
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Get(const KeyType & key) -> ValueType {
    if (auto iter = memtable_.find(key); iter != memtable_.end()) {
        return iter.second;
    }

    char * key_buf = nullptr;
    size_t key_buf_len = 0;
    Codec<KeyType>::EncodeValue(key, key_buf, static_cast<size_t>(-1), &key_buf_len);

}

}  // namespace ssindex
