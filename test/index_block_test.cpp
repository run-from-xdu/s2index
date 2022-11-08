#include <gtest/gtest.h>

#include "../src/index_block.hpp"

TEST(TestIndexBlock, Basic) {
    using Block = ssindex::IndexBlock<uint64_t>;
    Block blk{};

    auto data_generator = [](size_t num, std::vector<ssindex::IndexEdge<uint64_t>> & query_set) -> std::vector<ssindex::IndexEdge<uint64_t>> {
        std::vector<ssindex::IndexEdge<uint64_t>> ret{};
        for (uint64_t i = 0; i < num; ++i) {
            auto key = std::to_string(i);
            ssindex::IndexEdge<uint64_t> entry{key.data(), key.size(), i, 0x12345678};
            ret.emplace_back(entry);
            if (i % 100 == 0) {
                query_set.emplace_back(entry);
            }
        }
        return ret;
    };

    std::vector<ssindex::IndexEdge<uint64_t>> query_data{};
    auto data = data_generator(1000, query_data);

    blk.TryBuild(data, 0x12345678, 1);
    //blk.TryBuild(data, 0x12345678, 1);

//    std::cout << "Key: 0, Value: " << blk.GetValue(query_data[0]) << std::endl;
//    std::cout << "Key: 300, Value: " << blk.GetValue(query_data[3]) << std::endl;
//    std::cout << "Key: 900, Value: " << blk.GetValue(query_data[9]) << std::endl;


    for (uint64_t i = 0; i < 1000; ++i) {
        auto str = std::to_string(i);
        ssindex::IndexEdge<uint64_t> ie(str.data(), str.size(), 0, 0x12345678);
        std::cout << blk.GetValue(ie) << std::endl;
    }
}