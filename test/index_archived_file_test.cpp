#include <gtest/gtest.h>
#include <vector>

#include "../src/index_archived_file.hpp"

TEST(TestIndexArchivedFile, Basic) {
    std::string work_directory = "/tmp/";
    std::string file_name = work_directory + "temp.data";

    auto fileExists = [](std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(file_name)) {
        std::filesystem::remove(file_name);
    }

    size_t num_parts = 8;

    auto printResult = [](std::vector<std::pair<std::string, uint64_t>> & result) {
        for (auto & e : result) {
            std::cout << e.first << ", " << e.second << std::endl;
        }
    };

    auto file = ssindex::IndexArchivedFile<std::string, uint64_t>(file_name, num_parts);
    EXPECT_EQ(ssindex::Status::SUCCESS, file.WriteData(0, std::string("key1"), 1));
    EXPECT_EQ(ssindex::Status::SUCCESS, file.WriteData(7, std::string("key2"), 2));
    EXPECT_EQ(ssindex::Status::SUCCESS, file.WriteData(0, std::string("key3"), 3));
    EXPECT_EQ(ssindex::Status::SUCCESS, file.WriteData(0, std::string("key4"), 4));
    std::vector<std::pair<std::string, uint64_t>> res{};
    EXPECT_EQ(ssindex::Status::SUCCESS, file.ReadData(0, res));
    printResult(res);
    res.clear();
    EXPECT_EQ(ssindex::Status::SUCCESS, file.ReadData(7, res));
    printResult(res);

    file.PrintInfo();
}