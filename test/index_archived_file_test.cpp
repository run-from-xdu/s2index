#include <gtest/gtest.h>
#include <vector>

#include "../src/index_archived_file.hpp"

TEST(TestIndexArchivedFile, PureInMemory) {
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

    {
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
}

TEST(TestIndexArchivedFile, Hybrid) {
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
        std::cout << "Total Entry: " << result.size() << std::endl;
        std::cout << result[0].first << ", " << result[0].second << std::endl;
        std::cout << "......" << std::endl;
        std::cout << result[result.size() - 1].first << ", " << result[result.size() - 1].second << std::endl;
    };

    auto checkExistence = [](std::vector<std::pair<std::string, uint64_t>> & result, std::string && key) {
        for (auto & entry : result) {
            if (key == entry.first) {
                std::cout << "key: " << entry.first << ", " << "value: " << entry.second << " found!" << std::endl;
                return;
            }
        }
        std::cout << "fail" << std::endl;
    };

    auto file = ssindex::IndexArchivedFile<std::string, uint64_t>(file_name, num_parts);
    size_t entryNum = 10000;
    for (size_t i = 0; i < entryNum; i++) {
        auto key = "key" + std::to_string(i);
        auto value = static_cast<uint64_t>(i);
        file.WriteData(4, key, value);
    }
    file.SyncData();
    file.PrintInfo();
    std::vector<std::pair<std::string, uint64_t>> res{};
    EXPECT_EQ(ssindex::Status::SUCCESS, file.ReadData(4, res));
    printResult(res);
    checkExistence(res, std::string("key0"));
    checkExistence(res, std::string("key9961"));
    //file.PrintInfo();
}