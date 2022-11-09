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
          file_handle_(std::make_unique<IndexArchivedFile<KeyType, ValueType>>(FetchNextFileName(), block_num_)) {}

    ~FlushMemtableTask() {
        // TODO
    }

    Status Execute() override {
        return Status::SUCCESS;
    }

    void SetPreExecute(std::function<void()> pre_exec) override {

    }

    void SetPostExecute(std::function<void()> post_exec) override {

    }

    std::unordered_map<KeyType, ValueType> candidate_;

    std::unique_ptr<IndexArchivedFile<KeyType, ValueType>> file_handle_;

    std::vector<IndexBlock<ValueType>> blocks_;

    uint64_t block_num_;
};

}  // namespace ssindex