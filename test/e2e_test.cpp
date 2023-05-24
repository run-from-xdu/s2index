#include "../src/ssindex.hpp"

#include <gtest/gtest.h>
#include <ctime>

// TestSuite for unsigned int32 type
TEST(TEST, Uint32) {
    // Preprocess
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        std::filesystem::remove_all(ssindex::default_working_directory);
    }

    std::unordered_map<std::string, uint32_t> u32_map;

    auto u32ssindex = ssindex::SsIndex<std::string, uint32_t>(ssindex::default_working_directory);
    uint32_t entry_num = 400000;

    // Write data to the index and std::unordered_map
    for (uint32_t i = 0; i < entry_num; i++) {
        u32_map[std::to_string(i)] = i;
        u32ssindex.Set(std::to_string(i), i);
    }
    u32ssindex.WaitTaskComplete();

    // SsIndex read performance before Optimize
    auto correct = 0;
    auto wrong = 0;

    clock_t t1 = clock();

    for (uint32_t i = 0; i < entry_num; i++) {
        auto val = u32ssindex.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t2 = clock();

    std::cout << "SsIndex Uint32 Before Optimize: " << double(t2 - t1) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << u32ssindex.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;

    u32ssindex.Optimize();
    u32ssindex.WaitTaskComplete();

    // SsIndex read performance after Optimize
    correct = 0;
    wrong = 0;

    clock_t t3 = clock();

    for (uint32_t i = 0; i < entry_num; i++) {
        auto val = u32ssindex.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t4 = clock();

    std::cout << "SsIndex Uint32 After Optimize: " << double(t4 - t3) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << u32ssindex.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;

    // std::unordered_map space usage and read performance
    correct = 0;
    wrong = 0;

    auto map_size =  (u32_map.size() * (sizeof(uint32_t) + 6 + sizeof(void*)) + // data list
        u32_map.bucket_count() * (sizeof(void*) + sizeof(size_t))) // bucket index
        * 1.5; // estimated allocation overheads

    //std::cout << "std::unordered_map Size: " << map_size << " Bytes" << std::endl;

    clock_t t5 = clock();

    for (uint32_t i = 0; i < entry_num; i++) {
        auto val = u32_map[std::to_string(i)];
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t6 = clock();

    std::cout << "std::unordered_map: " << double(t6 - t5) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << map_size << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;
}

// TestSuite for unsigned int64 type
TEST(TEST, Uint64) {
    // Preprocess
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        std::filesystem::remove_all(ssindex::default_working_directory);
    }

    std::unordered_map<std::string, uint64_t> u64_map;

    // Write data to the index and std::unordered_map
    auto u64ssindex = ssindex::SsIndex<std::string, uint64_t>(ssindex::default_working_directory);
    uint64_t entry_num = 400000;

    for (uint64_t i = 0; i < entry_num; i++) {
        u64_map[std::to_string(i)] = i;
        u64ssindex.Set(std::to_string(i), i);
    }
    u64ssindex.WaitTaskComplete();

    // SsIndex read performance before Optimize
    auto correct = 0;
    auto wrong = 0;

    clock_t t1 = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        auto val = u64ssindex.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t2 = clock();

    std::cout << "SsIndex Uint64 Before Optimize: " << double(t2 - t1) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << u64ssindex.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;

    u64ssindex.Optimize();
    u64ssindex.WaitTaskComplete();

    // SsIndex read performance after Optimize
    correct = 0;
    wrong = 0;

    clock_t t3 = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        auto val = u64ssindex.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t4 = clock();

    std::cout << "SsIndex Uint64 After Optimize: " << double(t4 - t3) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << u64ssindex.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;

    // std::unordered_map space usage and read performance
    correct = 0;
    wrong = 0;

    auto map_size =  (u64_map.size() * (sizeof(uint64_t) + 6 + sizeof(void*)) + // data list
        u64_map.bucket_count() * (sizeof(void*) + sizeof(size_t))) // bucket index
        * 1.5; // estimated allocation overheads

    //std::cout << "std::unordered_map Size: " << map_size << " Bytes" << std::endl;

    clock_t t5 = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        auto val = u64_map[std::to_string(i)];
        if (val == i) correct++;
        else wrong++;
    }

    clock_t t6 = clock();

    std::cout << "std::unordered_map: " << double(t6 - t5) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << map_size << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;
}