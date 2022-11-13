#pragma once

#include <string>
#include <utility>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <queue>

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

    //using MemtableData = std::unordered_map<KeyType, ValueType>;
    using MemtableData = std::shared_ptr<std::unordered_map<KeyType, ValueType>>;

    struct Memtable {
        uint64_t id_;
        MemtableData data_;
    };

    explicit SsIndex(std::string directory)
        : working_directory_(std::move(directory)),
          seed_(0x12345678),
          fp_bits_(DefaultFpBits),
          scheduler_(new Scheduler(1)),
          memtable_(std::move(Memtable{FetchMemtableId(), std::make_shared<std::unordered_map<KeyType, ValueType>>()})),
          partition_num_(DefaultPartitionNum) {
        std::filesystem::create_directory(working_directory_);
    }

    ~SsIndex() {
        scheduler_->Stop();
    }

    void Set(const KeyType & key, const ValueType & value);

    auto Get(const KeyType & key) -> ValueType;

    inline uint64_t GetBlockPartition(const char * kbuf, const size_t klen) {
        return HASH(kbuf, klen) % partition_num_;
    }

    void WaitTaskComplete() {
        scheduler_->Wait();
    }

    void PrintInfo() {
        std::cout << "[Memory]\nMemtable_" << memtable_.id_ << " | Entry Num: " << memtable_.data_.get()->size() << std::endl;
        std::cout << "------------" << std::endl;
        for (auto iter = index_blocks_.begin(); iter != index_blocks_.end(); iter++) {
            for (auto it = iter->begin(); it != iter->end(); it++) {
                std::cout << it->GetFootprint() << " ";
            }
            std::cout << std::endl;
        }
        std::cout << "[Disk]" << std::endl;
        for (auto iter = files_.begin(); iter != files_.end(); iter++) {
            iter->get()->PrintInfo();
            std::cout << "------------" << std::endl;
        }
    }

private:

    auto FlushAndBuildIndexBlocks() -> Status;

    auto buildBlock(std::vector<std::pair<KeyType, ValueType>> & kvs, IndexBlock<ValueType> & block) -> Status;

    auto Flush() -> Status;

    /// Directory that stores all the files generated
    /// by the index
    std::string working_directory_;

    /// In-memory mutable hashtable
    Memtable memtable_;
    std::shared_mutex memtable_mutex_;

    /// In-memory immutable hashtable waiting to be flushed
    std::vector<Memtable> waiting_queue_;
    std::shared_mutex waiting_queue_mutex_;

//    auto FetchFlushCandidate() -> MemtableData {
//        std::lock_guard<std::mutex> latch{waiting_queue_mutex_};
//        waiting_queue_.push_back(memtable_);
//
//    }

    /// Index blocks
    std::vector<std::vector<IndexBlock<ValueType>>> index_blocks_;
    /// Archived files ｜ TODO: replace with the real implementation
    std::vector<std::unique_ptr<IndexArchivedFile<KeyType, ValueType>>> files_;
    /// lock for both two members above
    std::shared_mutex immutable_part_mutex_;

    /// Seed
    uint64_t seed_;

    /// False positive validation bits
    uint64_t fp_bits_;

    /// Number of partitions
    uint64_t partition_num_;

    /// Task scheduler
    Scheduler * scheduler_;
};

}  // namespace ssindex
