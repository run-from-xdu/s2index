#pragma once

#include "index_common.hpp"
#include "ssindex.hpp"
#include "scheduler.hpp"
#include "index_block.hpp"

#include <unordered_map>
#include <vector>
#include <memory>

namespace ssindex {

template<typename KeyType, typename ValueType>
struct CompactionTask : public Task {
    using FileHandle = std::unique_ptr<IndexArchivedFile<KeyType, ValueType>>;
    using Blocks = std::vector<IndexBlock<ValueType>>;
    using Batch = std::pair<Blocks, FileHandle>;

    explicit CompactionTask(const std::vector<Batch> & candidate,
                               uint64_t block_num,
                               std::function<uint64_t(const KeyType &)> partitioner,
                               uint64_t seed,
                               uint64_t fp_bits
                               )
            : candidate_(candidate),
              block_num_(block_num),
              seed_(seed),
              fp_bits_(fp_bits),
              file_handle_(std::make_unique<IndexArchivedFile<KeyType, ValueType>>(FetchNextFileName(), block_num)),
              partitioner_(partitioner) {
    }

    ~CompactionTask() override = default;

    void SetAcceptor(std::unique_ptr<IndexArchivedFile<KeyType, ValueType>> & acc_1, std::vector<IndexBlock<ValueType>> & acc_2) {
        SetPostExecute([this, &acc_1, &acc_2]{
            acc_1 = std::move(file_handle_);
            acc_2 = std::move(blocks_);
        });
    }

    Status Execute() override {
        // TODO: implement it
    }

    /// input
    std::vector<Batch> candidates_;

    /// outputs
    FileHandle file_handle_;
    Blocks blocks_;

    uint64_t block_num_;

    uint64_t seed_;

    uint64_t fp_bits_;

    std::function<uint64_t(const KeyType &)> partitioner_;
};

}  // namespace ssindex