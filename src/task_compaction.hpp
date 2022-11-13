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
                               uint64_t seed = 0x12345678,
                               uint64_t fp_bits = 0
                               )
            : candidate_(candidate),
              block_num_(block_num),
              seed_(seed),
              fp_bits_(fp_bits),
              file_handle_(std::make_unique<IndexArchivedFile<KeyType, ValueType>>(FetchNextArchivedFileName(), block_num)),
              /*partitioner_(partitioner)*/ {
    }

    ~CompactionTask() override = default;

    void SetAcceptor(std::unique_ptr<IndexArchivedFile<KeyType, ValueType>> & acc_1, std::vector<IndexBlock<ValueType>> & acc_2) {
        SetPostExecute([this, &acc_1, &acc_2]{
            acc_1 = std::move(file_handle_);
            acc_2 = std::move(blocks_);
        });
    }

    Status Execute() override {
        /// build each partition one by one
        for (uint64_t part = 0; part < block_num_; ++part) {
            /// for a single partition, load the data, build blocks & new file
            /// handle, and emplace the results back.
            std::vector<std::pair<KeyType, ValueType>> part_raw_data{};
            for (auto file_iter = candidates_.begin(); file_iter != candidates_.end(); ++file_iter) {
                IndexArchivedFile<KeyType, ValueType> * file_handle_ptr = file_iter->second.get();
                auto data_collector = [](const std::pair<KeyType, ValueType> & entry) {
                    file_handle_->WriteData(part, entry.first, entry.second);
                };
                auto s = file_handle_ptr->ReadData(part, part_raw_data, data_collector);
                if (s != Status::SUCCESS) {
                    return s;
                }
            }
            IndexBlock<ValueType> part_blk{};
            auto s = buildSinglePartition(part_raw_data, part_blk, seed_, fp_bits_);
            if (s != Status::SUCCESS) {
                return s;
            }
            blocks_.emplace_back(blk);
        }

        return Status::SUCCESS;
    }

    /// TODO: merge this method with flush memtable task
    auto buildSinglePartition(
            std::vector<std::pair<KeyType, ValueType>> & data,
            IndexBlock<ValueType> & block,
            uint64_t seed,
            uint64_t fp_bits) -> Status {
        if (data.size() == 0) {
            return Status::SUCCESS;
        }
        std::stable_sort(data.begin(), data.end(),
                         [](const std::pair<KeyType, ValueType> & v1, const std::pair<KeyType, ValueType> & v2) -> bool {
                             return v1.first < v2.first;
                         });
        data.erase(std::unique(data.begin(), data.end(),
                               [](const std::pair<KeyType, ValueType> & v1, const std::pair<KeyType, ValueType> & v2) -> bool {
                                   return v1.first == v2.first;
                               }), data.end());

        const size_t round = 20;
        for (size_t i = 0; i < round; ++i) {
            std::vector<IndexEdge<ValueType>> ies;
            ies.reserve(data.size());
            for (size_t j = 0; j < data.size(); ++j) {
                size_t length = 0;
                auto buf = IndexUtils<KeyType>::RawBuffer(data[j].first, &length);
                ies.emplace_back(IndexEdge<ValueType>(buf.get(), length, data[j].second, seed));
            }
            if (block.TryBuild(ies, seed, fp_bits) == Status::SUCCESS) {
                return Status::SUCCESS;
            }
            seed += 114514;
            //block = IndexBlock<ValueType>{};
        }
        return Status::ERROR;
    }

    /// input
    std::vector<Batch> candidates_;

    /// outputs
    FileHandle file_handle_;
    Blocks blocks_;

    uint64_t block_num_;

    uint64_t seed_;

    uint64_t fp_bits_;

    //std::function<uint64_t(const KeyType &)> partitioner_;
};

}  // namespace ssindex