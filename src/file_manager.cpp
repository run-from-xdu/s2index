#include "file_manager.hpp"

#include <iostream>

namespace ssindex {

auto FileManager::WritePage(uint64_t * page_id, WriteBuffer data) -> Status {
    *page_id = next_id_++;
    size_t offset = GetOffset(*page_id);
    data_file_.seekp(offset);
    data_file_.write(data, PageSize);
    /// I/O error
    if (data_file_.bad()) {
        return Status::ERROR;
    }
    data_file_.flush();
    return Status::SUCCESS;
}

auto FileManager::ReadPage(uint64_t page_id, ReadBuffer result) -> Status {
    size_t offset = GetOffset(page_id);
    if (offset > GetFileSize(file_name_)) {
        return Status::ERROR;
    } else {
        data_file_.seekp(offset);
        data_file_.read(result, PageSize);
        if (data_file_.bad()) {
            return Status::ERROR;
        }
    }
    return Status::SUCCESS;
}

//auto FileManager::AllocatePage() -> uint64_t {
//    return next_id_++;
//}

}  // namespace ssindex
