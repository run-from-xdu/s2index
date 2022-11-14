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

/// TODO: there are bunch of places using |IndexBlock| instead
/// of |std::shared_ptr<IndexBlock>|, which causes many needless
/// copies, and we need to change that afterwards.

namespace ssindex {

template<typename KeyType, typename ValueType>
struct BatchItem {
    using FileHandlePtr = std::shared_ptr<IndexArchivedFile<KeyType, ValueType>>;
    using Blocks = std::vector<IndexBlock<ValueType>>;
    using Batch = std::pair<Blocks, FileHandlePtr>;

    Batch data_;

    uint64_t id_;
};

template<typename KeyType, typename ValueType>
struct BatchHolder {
    using FileHandlePtr = typename BatchItem<KeyType, ValueType>::FileHandlePtr;
    using Blocks = typename BatchItem<KeyType, ValueType>::Blocks;
    using Batch = typename BatchItem<KeyType, ValueType>::Batch;
    using Item = BatchItem<KeyType, ValueType>;

    uint64_t next_id_;

    explicit BatchHolder() : next_id_(0) {}

    void AppendBatch(const FileHandlePtr & file, const Blocks & blocks) {
        items_.emplace_back(Item{{blocks, file}, next_id_});
        next_id_++;
    }

    void CommitCompaction(uint64_t start, size_t count, const FileHandlePtr & file, const Blocks & blocks) {
        for (auto iter = items_.begin(); iter != items_.end(); iter++) {
            if (iter->id_ == start) {
                items_.erase(iter + 1, iter + count);
                *iter = Item{{blocks, file}, next_id_};
                next_id_++;
            }
        }
    }

    bool FindCompactionCandidates(uint64_t * start, size_t * count, std::vector<Batch> & candidates) {
        bool need_compaction = false;
        int current_level = -1;
        size_t current_cnt = 0;
        for (auto iter = items_.rbegin(); iter != items_.rend(); iter++) {
            if (current_level == -1) {
                current_level = iter->data_.first.at(0).level_;
                current_cnt = 1;
                continue;
            }

            if (current_level == iter->data_.first.at(0).level_) {
                current_cnt++;
                if (current_cnt == CompactionThreshold) {
                    need_compaction = true;
                    *start = iter->id_;
                    *count = current_cnt;
                    for (size_t i = 0; i < items_.size(); ++i) {
                        if (items_[i].id_ == *start) {
                            for (size_t j = 0; j < current_cnt; j++) {
                                candidates.emplace_back(items_[i + j].data_);
                            }
                            break;
                        }
                    }
                    break;
                }
            } else {
                current_level = iter->data_.first.at(0).level_;
                current_cnt = 1;
            }
        }

        return need_compaction;
    }

    auto begin() -> decltype(auto) {
        return items_.begin();
    }

    auto end() -> decltype(auto) {
        return items_.end();
    }

    auto rbegin() -> decltype(auto) {
        return items_.rbegin();
    }

    auto rend() -> decltype(auto) {
        return items_.rend();
    }

    std::vector<Item> items_;
};

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

    uint64_t GetUsage() {
        uint64_t sum = 0;
        for (auto iter = batch_holder_.rbegin(); iter != batch_holder_.rend(); iter++) {
            auto & blocks = iter->data_.first;
            for (size_t i = 0; i < blocks.size(); i++) {
                sum += blocks.at(i).GetFootprint();
            }
        }
        return sum;
    }

    void PrintInfo() {
        std::cout << "[Memory]\nMemtable_" << memtable_.id_ << " | Entry Num: " << memtable_.data_.get()->size() << std::endl;
        for (auto iter = waiting_queue_.begin(); iter != waiting_queue_.end(); iter++) {
            std::cout << "Imm | " << iter->id_ << std::endl;
        }
        std::cout << "------------" << std::endl;
        for (auto iter = batch_holder_.rbegin(); iter != batch_holder_.rend(); iter++) {
            auto & blocks = iter->data_.first;
            std::cout << "Batch: ";
            for (size_t i = 0; i < blocks.size(); i++) {
                std::cout << blocks.at(i).GetFootprint() << " ";
            }
            std::cout << std::endl;
        }
        std::cout << "[Disk]" << std::endl;
        for (auto iter = batch_holder_.rbegin(); iter != batch_holder_.rend(); iter++) {
            auto & file = iter->data_.second;
            file->PrintInfo();
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

    BatchHolder<KeyType, ValueType> batch_holder_;
    std::shared_mutex batch_holder_mutex_;

//    /// Index blocks
//    std::vector<std::vector<IndexBlock<ValueType>>> index_blocks_;
//    /// Archived files ｜ TODO: replace with the real implementation
//    std::vector<std::shared_ptr<IndexArchivedFile<KeyType, ValueType>>> files_;
//    /// lock for both two members above
//    std::shared_mutex batch_holder_mutex_;

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
