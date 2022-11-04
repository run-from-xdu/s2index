#pragma once

#include "file_manager.hpp"
#include "encoding.hpp"

#include <memory>
#include <iostream>

namespace ssindex {

template<typename KeyType, typename ValueType>
class IndexArchivedFile {
public:
    explicit IndexArchivedFile(std::string file_name, size_t partition_num)
      : partition_num_(partition_num),
        buffer_usages_(std::vector<size_t>(partition_num, 0)),
        page_ids_(std::vector<std::vector<uint64_t>>(partition_num, std::vector<uint64_t>{})) {
        /// initialize buffers for each partition
        for (auto i = 0; i < partition_num_; i++) {
            buffers_.emplace_back(new char[FileManager::PageSize]);
        }

        /// initialize the FileManager
        file_manager_ = std::make_unique<FileManager>(std::move(file_name));
    }

    /// write the given key/value to the certain partition
    auto WriteData(size_t partition_id, const KeyType & key, const ValueType & value) -> Status {
        //std::cout << *reinterpret_cast<uint64_t *>(buffers_[partition_id] + 32) << std::endl;
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

    /// read all the data of the certain partition
    auto ReadData(size_t partition_id, std::vector<std::pair<KeyType, ValueType>> & result) const -> Status {
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

    auto PrintInfo() {
        for (size_t i = 0; i < partition_num_; i++) {
            std::cout << "Part" << i << " : " << buffer_usages_[i] << std::endl;
        }
    }
private:
    auto resetBuffer(size_t partition_id) {
        char * buffer = buffers_[partition_id];
        memset(buffer, 0, FileManager::PageSize);
        buffer_usages_[partition_id] = 0;
    }

    auto flushAllBuffers() {
        for (size_t i = 0; i < partition_num_; i++) {
            char * buf = buffers_[i];
            uint64_t pid;
            file_manager_->WritePage(&pid, buf);
            resetBuffer(i);
            auto & pids = page_ids_[i];
            pids.emplace_back(pid);
        }
    }

    static auto pageSize() -> size_t {
        return FileManager::PageSize;
    }

    /// file manager of the underlying archived file
    std::unique_ptr<FileManager> file_manager_;

    /// number of partitions
    size_t partition_num_;

    /// the id of pages (written to the disk) of each partitions
    std::vector<std::vector<uint64_t>> page_ids_;

    /// memory buffer for each partition
    std::vector<char *> buffers_;

    /// usage of each buffer
    std::vector<size_t> buffer_usages_;
};

}  // namespace ssindex

