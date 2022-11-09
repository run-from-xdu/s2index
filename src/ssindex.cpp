#include "ssindex.hpp"
#include "index_common.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Set(const KeyType &key, const ValueType &value) {
    memtable_.insert_or_assign(key, value);
    if (memtable_.size() == MemtableFlushThreshold) {
        /// TODO: schedule a flush task and reset the memtable
    }
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Get(const KeyType & key) -> ValueType {
    if (auto iter = memtable_.find(key); iter != memtable_.end()) {
        return iter->second;
    }

    char * key_buf = nullptr;
    size_t key_buf_len = 0;
    Codec<KeyType>::EncodeValue(key, key_buf, static_cast<size_t>(-1), &key_buf_len);
    IndexEdge<uint64_t> ie{key_buf + sizeof(uint64_t), key_buf_len - sizeof(uint64_t), 0, seed_};
    uint64_t partition = GetBlockPartition(key_buf + sizeof(uint64_t), key_buf_len - sizeof(uint64_t));
    for (size_t i = index_blocks_.size() - 1; i >= 0; --i) {
        auto & batch = index_blocks_[i];
        auto & block = batch[partition];
        auto ret = block.GetValue(ie);
        if (ret != key_not_found) {
            delete [] key_buf;
            return ret;
        }
    }

    delete [] key_buf;
    return key_not_found;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::FlushAndBuildIndexBlocks() -> Status {
    if (memtable_.empty()) {
        return Status::SUCCESS;
    }



    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::buildBlock(
        std::vector<std::pair<KeyType, ValueType>> & kvs,
        IndexBlock<ValueType> & block) -> Status {
    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Flush() -> Status {
//    assert(!memtable_.empty());
//    std::unordered_map<KeyType, ValueType> raw_data{};
//    raw_data.swap(memtable_);
//
//    std::string file_name{};
//    auto file = std::make_unique<IndexArchivedFile<KeyType, ValueType>>(file_name, partition_num_);
//
//    for (auto iter = raw_data.begin(); iter != raw_data.end(); ++iter) {
//        auto & key = iter->first;
//        auto & value = iter->second;
//        uint64_t partition = GetBlockPartition();
//        file->WriteData(static_cast<size_t>(partition), key, value);
//    }

    return Status::SUCCESS;
}

template class SsIndex<std::string, uint64_t>;

}  // namespace ssindex
