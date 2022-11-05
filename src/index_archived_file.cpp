#include "index_archived_file.hpp"
#include "encoding.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
auto IndexArchivedFile<KeyType, ValueType>::WriteData(size_t partition_id, const KeyType & key, const ValueType & value) -> Status {
    size_t offset = buffer_usages_[partition_id];
    size_t left_space = pageSize() - offset;
    size_t span = 0;
    char * buffer = buffers_[partition_id];
    auto status = Codec<KeyType>::EncodeValue(key, buffer + offset, left_space, &span);
    if (status != Status::SUCCESS) {
        /// TODO: handle space not enough
        return status;
    }
    left_space -= span;
    buffer_usages_[partition_id] += span;
    offset += span;
    status = Codec<ValueType>::EncodeValue(value, buffer + offset, left_space, &span);
    if (status != Status::SUCCESS) {
        /// TODO: handle space not enough
        return status;
    }
    //std::cout << buffer_usages_[partition_id] << " " << *reinterpret_cast<uint64_t *>(buffer + buffer_usages_[partition_id]) << std::endl;
    buffer_usages_[partition_id] += span;
    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto IndexArchivedFile<KeyType, ValueType>::ReadData(size_t partition_id, std::vector<std::pair<KeyType, ValueType>> & result) const -> Status {
    char * buffer = buffers_[partition_id];
    size_t end = buffer_usages_[partition_id];
    size_t curr_pos = 0;
    while (curr_pos < end) {
        KeyType key;
        ValueType value;
        size_t span = 0;
        Codec<KeyType>::DecodeValue(buffer + curr_pos, &key, &span);
        curr_pos += span;
        Codec<ValueType>::DecodeValue(buffer + curr_pos, &value, &span);
        //std::cout << *reinterpret_cast<uint64_t *>(buffer + 32) << std::endl;
        curr_pos += span;
        result.emplace_back(std::make_pair(key, value));
    }
    /// TODO: read on-disk data
    return Status::SUCCESS;
}

template class IndexArchivedFile<std::string, uint64_t>;
template class IndexArchivedFile<std::string, uint32_t>;
template class IndexArchivedFile<std::string, uint16_t>;
template class IndexArchivedFile<std::string, uint8_t>;
template class IndexArchivedFile<std::string, int64_t>;
template class IndexArchivedFile<std::string, int32_t>;
template class IndexArchivedFile<std::string, int16_t>;
template class IndexArchivedFile<std::string, int8_t>;
}  // namespace ssindex
