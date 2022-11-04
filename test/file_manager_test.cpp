#include <gtest/gtest.h>

#include "../src/file_manager.hpp"

TEST(TestFileManager, Basic) {
    char buf[ssindex::FileManager::PageSize] = {0};
    char data[ssindex::FileManager::PageSize] = {0};
    std::string work_directory = "/tmp/";
    std::string file_name = work_directory + "temp.data";

    auto fileExists = [](std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(file_name)) {
        std::filesystem::remove(file_name);
    }

    auto file_manager = ssindex::FileManager(file_name);

    auto getFileSize = [](std::string & file_name) -> int {
        struct stat stat_buf{};
        int rc = stat(file_name.c_str(), &stat_buf);
        return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
    };

    std::strncpy(data, "page1's data", sizeof(data));
    uint64_t page_id_1;
    EXPECT_EQ(file_manager.WritePage(&page_id_1, data), ssindex::Status::SUCCESS);

    std::strncpy(data, "page2's data", sizeof(data));
    uint64_t page_id_2;
    EXPECT_EQ(file_manager.WritePage(&page_id_2, data), ssindex::Status::SUCCESS);

    std::strncpy(data, "page3's data", sizeof(data));
    uint64_t page_id_3;
    EXPECT_EQ(file_manager.WritePage(&page_id_3, data), ssindex::Status::SUCCESS);

    EXPECT_EQ(file_manager.ReadPage(page_id_2, buf), ssindex::Status::SUCCESS);
    EXPECT_TRUE(!strcmp(buf, "page2's data"));

    EXPECT_EQ(file_manager.ReadPage(page_id_3, buf), ssindex::Status::SUCCESS);
    EXPECT_TRUE(!strcmp(buf, "page3's data"));

    EXPECT_EQ(file_manager.ReadPage(page_id_1, buf), ssindex::Status::SUCCESS);
    EXPECT_TRUE(!strcmp(buf, "page1's data"));
}