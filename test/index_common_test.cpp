#include <gtest/gtest.h>

#include "../src/index_common.hpp"

TEST(TestIndexCommon, Basic) {
    uint64_t value = 0xFFFF;
    auto ret = ssindex::IndexUtils<uint64_t>::mask(value, 4);
    std::cout << ret << std::endl;

    std::string data{"hello"};
    size_t length = 0;
    auto buf = ssindex::IndexUtils<std::string>::RawBuffer(data, &length);
    std::cout << buf << " " << length << std::endl;
}