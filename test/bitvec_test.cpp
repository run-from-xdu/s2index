#include <gtest/gtest.h>
#include <bitset>

#include "../src/bitvec.hpp"

TEST(TestBitVec, Basic) {
    using BV = ssindex::BitVec<uint64_t>;

    BV bv(1000);
    //auto cnt = bv.BitsCount();
    //EXPECT_EQ(1000, cnt);

    bv.setBits(10, 10, 0xFF);
    //bv.setBitsU64(30, 10, 1);
    auto ret = bv.getBits(0, 40);
    std::cout << std::bitset<sizeof(ret)*8>(ret) << std::endl;

    //bv.printInfo();
}