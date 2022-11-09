#pragma once

#include <string>
#include <utility>

#include "index_archived_file.hpp"
#include "index_block.hpp"
#include "scheduler.hpp"

namespace ssindex {

/// Space-Saving Index
///
/// |SsIndex| supports basic key/value operations (e.g. set, get), if
/// a value associated with a key does exist, it would always be returned.
/// Occasionally, a random result would be returned if the given key
/// does not exist (i.e. false positive), but the FP rate is tunable.
template<typename KeyType, typename ValueType>
class SsIndex {
public:
    static constexpr ValueType key_not_found = IndexUtils<ValueType>::KeyNotFound;

    using Memtable = std::unordered_map<KeyType, ValueType>;

    explicit SsIndex(std::string directory)
        : working_directory_(std::move(directory)),
          seed_(0x12345678),
          partition_num_(DefaultPartitionNum) {}

    auto Set(const KeyType & key, const ValueType & value);

    auto Get(const KeyType & key) -> ValueType;

private:
    inline uint64_t GetBlockPartition(const char * kbuf, const size_t klen) {
        return HASH(kbuf, klen) % partition_num_;
    }

    auto FlushAndBuildIndexBlocks() -> Status;

    auto buildBlock(std::vector<std::pair<KeyType, ValueType>> & kvs, IndexBlock<ValueType> & block) -> Status;

    auto Flush() -> Status;

    /// Directory that stores all the files generated
    /// by the index
    std::string working_directory_;

    /// Archived files ｜ TODO: replace with the real implementation
    std::vector<std::unique_ptr<IndexArchivedFile<KeyType, ValueType>>> files_;

    /// In-memory mutable hashtable
    Memtable memtable_;

    /// In-memory immutable hashtable waiting to be flushed
    std::vector<Memtable> waiting_queue_;
    std::mutex waiting_queue_mutex_;

//    auto FetchFlushCandidate() -> Memtable {
//        std::lock_guard<std::mutex> latch{waiting_queue_mutex_};
//
//    }

    /// Index blocks
    std::vector<std::vector<IndexBlock<ValueType>>> index_blocks_;

    /// Seed
    uint64_t seed_;

    /// Number of partitions
    uint64_t partition_num_;
};

}  // namespace ssindex
