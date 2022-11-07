#pragma once

#include "file_manager.hpp"
#include "encoding.hpp"

#include <memory>
#include <iostream>

namespace ssindex {

/// |IndexArchivedFile| has two working modes:
///
/// 1) Active Mode. In this mode, we can dynamically insert incoming
/// key/value pairs to the certain partition of the buffer, and once a
/// partition's buffer page is full, it's spilled to the disk automatically,
/// the buffer page of that partition is refreshed as well. Every page
/// id for every partitions is recorded internally, so that we can scan
/// a certain partition's data without aware of the underlying layout.
///
/// 2) Frozen Mode. In this mode, the |IndexArchivedFile| is literally
/// archived (i.e. read-only). In addition, all the data has been
/// persisted to the disk, so at this point, |IndexArchivedFile| becomes
/// a real "file". Since we don't store any metadata within the file,
/// additional metadata saving process is needed for the crash safety.
template<typename KeyType, typename ValueType>
class IndexArchivedFile {
public:
    static constexpr size_t UsedSizeWidth = sizeof(uint64_t);

    explicit IndexArchivedFile(std::string file_name, size_t partition_num)
      : partition_num_(partition_num),
        buffer_usages_(std::vector<size_t>(partition_num, UsedSizeWidth)),
        page_ids_(std::vector<std::vector<uint64_t>>(partition_num, std::vector<uint64_t>{})) {
        /// initialize buffers for each partition
        for (auto i = 0; i < partition_num_; i++) {
            buffers_.emplace_back(new char[FileManager::PageSize]);
        }

        /// initialize the FileManager
        file_manager_ = std::make_unique<FileManager>(std::move(file_name));
    }

    ~IndexArchivedFile() {
        for (auto & buffer : buffers_) {
            delete [] buffer;
        }
    }

    /// Write the given key/value to the certain partition
    auto WriteData(size_t partition_id, KeyType key, ValueType value) -> Status;

    /// Read all the data of the certain partition
    auto ReadData(size_t partition_id, std::vector<std::pair<KeyType, ValueType>> & result) const -> Status;

    /// Sync all data on the disk
    auto SyncData() {
        file_manager_->Sync();
    }

    auto PrintInfo() {
        for (size_t i = 0; i < partition_num_; i++) {
            std::cout << "Part" << i << " : " << buffer_usages_[i] << " ";
            if (page_ids_[i].empty()) std::cout << "No Page Allocated";
            for (auto j : page_ids_[i]) std::cout << j << " ";
            std::cout << std::endl;
        }
    }
private:
    auto resetBuffer(size_t partition_id) {
        char * buffer = buffers_[partition_id];
        memset(buffer, 0, FileManager::PageSize);
        buffer_usages_[partition_id] = UsedSizeWidth;
    }

    auto flushBuffer(size_t partition_id) {
        char * buf = buffers_[partition_id];
        /// record used size
        Codec<uint64_t>::EncodeValue(buffer_usages_[partition_id], buf, UsedSizeWidth);
        uint64_t pid;
        file_manager_->WritePage(&pid, buf);
        resetBuffer(partition_id);
        auto & pids = page_ids_[partition_id];
        pids.emplace_back(pid);
    }

    auto flushAllBuffers() {
        for (size_t i = 0; i < partition_num_; i++) {
            flushBuffer(i);
        }
    }

    static auto pageSize() -> size_t {
        return FileManager::PageSize;
    }

    /// File manager of the underlying archived file
    std::unique_ptr<FileManager> file_manager_;

    /// Number of partitions
    size_t partition_num_;

    /// The id of pages (written to the disk) of each partitions
    std::vector<std::vector<uint64_t>> page_ids_;

    /// Memory buffer for each partition
    std::vector<char *> buffers_;

    /// Usage of each buffer
    std::vector<size_t> buffer_usages_;
};

/// TODO: implement this when in-memory logic is done
//template<typename KeyType, typename ValueType>
//class ArchivedFiles {
//public:
//    explicit ArchivedFiles(std::string directory) : directory_(std::move(directory)) {}
//
//private:
//    using File = IndexArchivedFile<KeyType, ValueType>;
//    using FilePtr = std::unique_ptr<File>;
//
//    auto TryCompaction(FilePtr compacted) -> Status;
//
//    /// Working directory
//    std::string directory_;
//
//    /// Frozen-mode archived files
//    std::vector<FilePtr> frozen_files_;
//
//    /// Active-mode archived file
//    FilePtr active_file_;
//
//
//};

}  // namespace ssindex

