#include "../src/ssindex.hpp"

#include <gtest/gtest.h>
#include <ctime>

TEST(TestE2E, Performance) {
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        //std::filesystem::remove(ssindex::default_working_directory);
        std::filesystem::remove_all(ssindex::default_working_directory);
    }

    auto index = ssindex::SsIndex<std::string, uint64_t>(ssindex::default_working_directory);
    uint64_t entry_num = 120000;
    for (uint64_t i = 0; i < entry_num; i++) {
        index.Set(std::to_string(i), i);
    }

    index.WaitTaskComplete();

    index.PrintInfo();

    auto correct = 0;
    auto wrong = 0;

    clock_t start = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        auto val = index.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t end = clock();
    std::cout << "SsIndex: " << double(end - start) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << index.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;

    struct TestKV {
        std::unordered_map<std::string, uint64_t> data;
        std::shared_mutex latch;

        void Set(const std::string & k, uint64_t v) {
            std::lock_guard<std::shared_mutex> w_latch{latch};
            data[k] = v;
        }

        uint64_t Get(const std::string & k) {
            std::shared_lock<std::shared_mutex> r_latch{latch};
            return data[k];
        }
    };

    TestKV kv{};
    for (uint64_t i = 0; i < entry_num; i++) {
        kv.Set(std::to_string(i), i);
    }

    /// NOTICE: I use dedicated program to calculate the memory usage of
    /// std::unordered_map, so just ignore those related code here.
    start = clock();
    for (uint64_t i = 0; i < entry_num; i++) {
        EXPECT_EQ(i, kv.Get(std::to_string(i)));
    }
    end = clock();
    std::cout << "Standard Unordered Map: " << double(end - start) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op" << std::endl;
}

TEST(TestCompaction, FindCandidates) {
    ssindex::BatchHolder<std::string, uint64_t> holder{};

    for (size_t i = 0; i < ssindex::CompactionThreshold; i++) {
        holder.AppendBatch(
                std::make_shared<ssindex::IndexArchivedFile<std::string, uint64_t>>("/tmp/test_file", 1),
                {ssindex::IndexBlock < uint64_t > {}}
                );
    }

    uint64_t st = 0;
    size_t cnt = 0;
    std::vector<ssindex::BatchItem<std::string, uint64_t>::Batch> cds{};
    auto nc = holder.FindCompactionCandidates(&st, &cnt, cds);
    std::cout << nc << " " << st << " " << cnt << " " << cds.size() << std::endl;

    for (size_t i = 0; i < ssindex::CompactionThreshold / 2; i++) {
        holder.AppendBatch(
                std::make_shared<ssindex::IndexArchivedFile<std::string, uint64_t>>("/tmp/test_file", 1),
                {ssindex::IndexBlock < uint64_t > {}}
        );
    }

    cds.clear();
    holder.items_[0].data_.first[0].level_ = 1;
    holder.items_[1].data_.first[0].level_ = 1;
    nc = holder.FindCompactionCandidates(&st, &cnt, cds);
    std::cout << nc << " " << st << " " << cnt << " " << cds.size() << std::endl;

    holder.CommitCompaction(st, cnt, std::make_shared<ssindex::IndexArchivedFile<std::string, uint64_t>>("/tmp/test_file", 1),
            {ssindex::IndexBlock < uint64_t > {}});
    std::cout << holder.items_.size() << std::endl;
}

TEST(TestE2E, Optimization) {
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        //std::filesystem::remove(ssindex::default_working_directory);
        std::filesystem::remove_all(ssindex::default_working_directory);
    }

    auto index = ssindex::SsIndex<std::string, uint64_t>(ssindex::default_working_directory);
    uint64_t entry_num = 99999;
    for (uint64_t i = 0; i < entry_num; i++) {
        index.Set(std::to_string(i), i);
    }

    index.WaitTaskComplete();

    index.PrintInfo();

    index.Optimize();

    index.PrintInfo();

    auto correct = 0;
    auto wrong = 0;

    clock_t start = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        auto val = index.Get(std::to_string(i));
        if (val == i) correct++;
        else wrong++;
    }

    clock_t end = clock();
    std::cout << "SsIndex: " << double(end - start) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << index.GetUsage() << " Bytes" << std::endl;
    std::cout << "Correct Rate: " << double(correct) / double(correct + wrong) << std::endl;
}