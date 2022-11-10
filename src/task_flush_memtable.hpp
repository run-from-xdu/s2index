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
struct FlushMemtableTask : public Task {
    explicit FlushMemtableTask(const std::unordered_map<KeyType, ValueType> & candidate, uint64_t block_num)
        : candidate_(candidate),
          block_num_(block_num),
          file_handle_(std::make_unique<IndexArchivedFile<KeyType, ValueType>>(FetchNextFileName(), block_num)) {}

    ~FlushMemtableTask() override {}

    Status Execute() override {
        file_handle_ = std::make_unique<IndexArchivedFile<KeyType, ValueType>>(FetchNextFileName(), block_num_);
        for (auto iter = candidate_.begin(); iter != candidate_.end(); ++iter) {
            size_t partition = 0;
            auto s = file_handle_->WriteData(partition, iter->first, iter->second);
            if (s != Status::SUCCESS) {
                return s;
            }
        }

        for (uint64_t i = 0; i < block_num_; ++i) {
            std::vector<std::pair<KeyType, ValueType>> data{};
            auto s = file_handle_->ReadData(i, data);
            if (s != Status::SUCCESS) {
                return s;
            }
            IndexBlock<ValueType> blk{};
            s = buildSinglePartition(data, blk, seed_, fp_bits_);
            if (s != Status::SUCCESS) {
                return s;
            }
            blocks_.emplace_back(blk);
        }

        return Status::SUCCESS;
    }

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
                ies.emplace_back(IndexEdge<ValueType>(buf.get() + sizeof(uint64_t), length - sizeof(uint64_t), data[j].second, seed));
            }
            if (block.TryBuild(ies, seed, fp_bits) == Status::SUCCESS) {
                return Status::SUCCESS;
            }
            seed += 114514;
        }
        return Status::ERROR;
    }

    std::unordered_map<KeyType, ValueType> candidate_;

    std::unique_ptr<IndexArchivedFile<KeyType, ValueType>> file_handle_;

    std::vector<IndexBlock<ValueType>> blocks_;

    uint64_t block_num_;

    uint64_t seed_;

    uint64_t fp_bits_;
};

}  // namespace ssindex