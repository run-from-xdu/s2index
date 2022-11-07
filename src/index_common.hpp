#pragma once

#include <string>

namespace ssindex {

static const std::string default_working_directory = "/tmp/temp_data/";
static constexpr auto GetFullPath(const std::string & file_name) -> std::string {
    return default_working_directory + file_name;
}

using WriteBuffer = const char *;
using ReadBuffer = char *;

enum Status : int {
    ERROR = -1,
    SUCCESS = 0,
    PAGE_FULL = 1
};

}  // namespace ssindex

