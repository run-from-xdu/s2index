#pragma once

#include <string>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include "index_common.hpp"

namespace ssindex {

/// |FileManager| provides these two semantics:
/// 1) write 4KB data to the underlying file -> return a handle called page id
/// 2) read 4KB data via a page id
class FileManager {
public:
    /// number of bytes per page
    static const size_t PageSize = 4096;

    explicit FileManager(std::string file_name)
            : file_name_(std::move(file_name)), next_id_(0) {
        /// initialize the underlying data file
        data_file_.open(file_name_, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
    }

    ~FileManager() {
        data_file_.sync();
        data_file_.close();
    }

    /// Allocate a new page from this file
    //auto AllocatePage() -> uint64_t;

    /// Write the contents of the specified page into disk file
    auto WritePage(uint64_t * page_id, WriteBuffer data) -> Status;

    /// Read the contents of the specified page into the given memory area
    auto ReadPage(uint64_t page_id, ReadBuffer result) -> Status;

    auto Sync() {
        data_file_.sync();
    }
private:
    static inline auto GetOffset(uint64_t page_id) -> size_t {
        return static_cast<size_t>(page_id) * PageSize;
    }

    static inline auto GetFileSize(std::string & file_name) -> int {
        struct stat stat_buf{};
        int rc = stat(file_name.c_str(), &stat_buf);
        return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
    }

    /// name of the working file
    std::string file_name_;

    /// file handle
    std::fstream data_file_;

    /// page id allocator
    uint64_t next_id_;
};

}  // namespace ssindex
