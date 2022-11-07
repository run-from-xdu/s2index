#include <cassert>
#include "index_archived_file.hpp"
#include "encoding.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
auto IndexArchivedFile<KeyType, ValueType>::WriteData(size_t partition_id, KeyType key, ValueType value) -> Status {
    size_t offset = buffer_usages_[partition_id];
    size_t left_space = pageSize() - offset;
    if (left_space <= 0 || left_space > pageSize() - UsedSizeWidth) {
        flushBuffer(partition_id);
        offset = buffer_usages_[partition_id];
        left_space = pageSize() - offset;
    }

    size_t span = 0;
    char * buffer = buffers_[partition_id];
    auto entry = std::make_pair<KeyType, ValueType>(std::move(key), std::move(value));
    auto status = Codec<std::pair<KeyType, ValueType>>::EncodeValue(entry, buffer + offset, left_space, &span);
    if (status != Status::SUCCESS) {
        if (status != Status::PAGE_FULL) {
            return status;
        }
        flushBuffer(partition_id);
        offset = buffer_usages_[partition_id];
        left_space = pageSize() - offset;
        status = Codec<std::pair<KeyType, ValueType>>::EncodeValue(entry, buffer + offset, left_space, &span);
        assert(status == Status::SUCCESS);
    }
    buffer_usages_[partition_id] += span;
    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto IndexArchivedFile<KeyType, ValueType>::ReadData(size_t partition_id, std::vector<std::pair<KeyType, ValueType>> & result) const -> Status {
    auto pageIterator = [](char * target_page, size_t start_pos, size_t end_pos, std::vector<std::pair<KeyType, ValueType>> & res) {
        //std::cout << "Iterating Page " << start_pos << ", " << end_pos << std::endl;
        size_t curr_pos = start_pos;
        while (curr_pos < end_pos) {
            auto entry = std::pair<KeyType, ValueType>{};
            size_t span = 0;
            Codec<std::pair<KeyType, ValueType>>::DecodeValue(target_page + curr_pos, &entry, &span);
            curr_pos += span;
            res.emplace_back(entry);
        }
    };

    char * buffer = buffers_[partition_id];
    size_t end = buffer_usages_[partition_id];
    size_t curr_pos = UsedSizeWidth;

    pageIterator(buffer, curr_pos, end, result);

    auto page_buffer = std::make_unique<char[]>(pageSize());
    for (uint64_t page_id : page_ids_[partition_id]) {
        file_manager_->ReadPage(page_id, page_buffer.get());
        uint64_t used;
        Codec<uint64_t>::DecodeValue(page_buffer.get(), &used);
        end = static_cast<size_t>(used);
        curr_pos = UsedSizeWidth;
        pageIterator(page_buffer.get(), curr_pos, end, result);
    }
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
