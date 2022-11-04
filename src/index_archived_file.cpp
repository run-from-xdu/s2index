#include "index_archived_file.hpp"
#include "encoding.hpp"

namespace ssindex {

//template<typename KeyType, typename ValueType>
//auto IndexArchivedFile<KeyType, ValueType>::WriteData(
//    size_t partition_id,
//    const KeyType & key,
//    const ValueType & value
//    ) -> Status {
//    size_t offset = buffer_usages_[partition_id];
//    size_t left_space = pageSize() - offset;
//    auto * buffer = buffers_[partition_id].get() + offset;
//    auto status = Codec<KeyType>::EncodeValue(key, buffer, left_space);
//    if (status != Status::SUCCESS) {
//        /// TODO: handle space not enough
//        return status;
//    }
//    left_space -= sizeof(KeyType);
//    buffer_usages_[partition_id] += sizeof(KeyType);
//    status = Codec<ValueType>::EncodeValue(value, buffer, left_space);
//    if (status != Status::SUCCESS) {
//        /// TODO: handle space not enough
//        return status;
//    }
//    buffer_usages_[partition_id] += sizeof(ValueType);
//    return Status::SUCCESS;
//}
//
//template<typename KeyType, typename ValueType>
//auto IndexArchivedFile<KeyType, ValueType>::ReadData(
//    size_t partition_id,
//    std::vector<std::pair<KeyType, ValueType>> & result
//    ) const -> Status {
//    auto * buffer = buffers_[partition_id].get();
//    size_t end = buffer_usages_[partition_id];
//    size_t curr_pos = 0;
//    KeyType key;
//    ValueType value;
//    while (curr_pos < end) {
//        Codec<KeyType>::DecodeValue(buffer + curr_pos, &key);
//        curr_pos += sizeof(KeyType);
//        Codec<ValueType>::DecodeValue(buffer + curr_pos, &value);
//        curr_pos += sizeof(ValueType);
//        result.emplace_back({key, value});
//    }
//    /// TODO: read on-disk data
//    return Status::SUCCESS;
//}

}  // namespace ssindex
