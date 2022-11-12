#include "../src/ssindex.hpp"

#include <gtest/gtest.h>

TEST(TestE2E, Flushing) {
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        std::filesystem::remove(ssindex::default_working_directory);
    }

    auto index = ssindex::SsIndex<std::string, uint64_t>(ssindex::default_working_directory);
    uint64_t entry_num = 20000;
    for (uint64_t i = 0; i < entry_num; i++) {
        index.Set(std::to_string(i), i);
        //std::cout << i << std::endl;
    }

    index.WaitTaskComplete();

    index.PrintInfo();
}