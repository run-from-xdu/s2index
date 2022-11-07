#include <gtest/gtest.h>
#include <cstring>

#include "../src/encoding.hpp"

TEST(TestEncoding, Basic) {
    uint64_t data = 114514;
    uint64_t verify = 0;
    char buf[100];

    ssindex::Codec<uint64_t>::EncodeValue(data, buf, 100);
    auto res = *reinterpret_cast<uint64_t *>(buf);
    EXPECT_EQ(data, res);

    ssindex::Codec<uint64_t>::DecodeValue(buf, &verify);
    EXPECT_EQ(verify, data);

    std::string str_data{"hello world"};
    std::string str_verify{};
    char buff[100];
    ssindex::Codec<std::string>::EncodeValue(str_data, buff, 100);
    ssindex::Codec<std::string>::DecodeValue(buff, &str_verify);
    EXPECT_EQ(str_data, str_verify);

    std::string str_data_2{"xxxxxxxxxxxxxxxxxxxxxxxxxx"};
    char bufff[10];
    EXPECT_EQ(ssindex::Status::PAGE_FULL, ssindex::Codec<std::string>::EncodeValue(str_data_2, bufff, 10));

    std::string key{"key"};
    uint64_t value = 100;
    char buffff[100];
    auto ss = ssindex::Codec<std::pair<std::string, uint64_t>>::EncodeValue(std::make_pair(key, value), buffff, 100);
    EXPECT_EQ(ssindex::Status::SUCCESS, ss);
    //std::cout << "...." << std::endl;
    std::pair<std::string, uint64_t> kv_verify{};
    size_t used = 0;
    ssindex::Codec<std::pair<std::string, uint64_t>>::DecodeValue(buffff, &kv_verify, &used);
    std::cout << used << std::endl;
    std::cout << kv_verify.first << ", " << kv_verify.second << std::endl;
}