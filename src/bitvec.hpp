#pragma once

#include <vector>
#include <fstream>
#include <iostream>

#include "index_common.hpp"

namespace ssindex {

template<typename ValueType>
class BitVec {
public:
    static constexpr size_t BitsNum = 64;

    explicit BitVec() = default;

    explicit BitVec(size_t size) { Resize(size); }

    auto Resize(size_t new_size) {
        data_.resize((new_size + BitsNum - 1) / BitsNum);
        fill(data_.begin(), data_.end(), 0);
    }

    auto BitsCount() const -> size_t {
        return data_.size() * BitsNum;
    }

    auto setBit(size_t pos) {
        data_[(pos / BitsNum) % data_.size()] |= (1LLU << (pos % BitsNum));
    }

    auto setBits(size_t pos, size_t len, const ValueType & bits) {
        uint64_t index = pos / BitsNum;
        uint64_t offset = pos % BitsNum;
        if (offset + len < BitsNum) {
            auto v1 = (data_[index] & (~(((1LLU << len) - 1) << offset)));
            auto v2 = uint64_t(bits) << offset;
            std::cout << std::bitset<sizeof(v1)*8>(v1) << ", " << std::bitset<sizeof(v2)*8>(v2) << std::endl;
            data_[index] = (data_[index] & (~(((1LLU << len) - 1) << offset))) | uint64_t(bits) << offset;
            return;
        }
        data_[index] = IndexUtils<uint64_t>::mask(data_[index], offset);
        data_[index++] |= uint64_t(bits) << offset;
        offset = BitsNum - offset;
        len -= offset;
        while (len > BitsNum) {
            data_[index++] = uint64_t(bits >> offset);
            offset += BitsNum;
            len -= BitsNum;
        }
        if (len) {
            data_[index] = (data_[index] & (~((1LLU << len) - 1))) | uint64_t(bits >> offset);
        }
    }

    auto setBitsU64(size_t pos, size_t len, const uint64_t bits) {
        uint64_t index = pos / BitsNum;
        uint64_t offset = pos % BitsNum;
        if (offset + len < BitsNum) {
            data_[index] = (data_[index] & (~(((1LLU << len) - 1) << offset))) | bits << offset;
            return;
        }
        data_[index] = IndexUtils<uint64_t>::mask(data_[index], offset);
        data_[index++] |= bits << offset;
        offset = BitsNum - offset;
        len -= offset;
        if (len) {
            data_[index] = (data_[index] & (~((1LLU << len) - 1))) | bits >> offset;
        }
    }

    auto getBit(size_t pos) const -> bool {
        return (data_[(pos / BitsNum) % data_.size()] >> (pos % BitsNum)) & 1LLU;
    }

    auto getBits(size_t pos, size_t len) const -> ValueType {
        uint64_t index = pos / BitsNum;
        uint64_t offset = pos % BitsNum;
        if (offset + len < BitsNum) {
            return IndexUtils<uint64_t>::mask(data_[index] >> offset, len);
        }
        ValueType ret = data_[index++] >> offset;
        offset = BitsNum - offset;
        len -= offset;
        while (len >= BitsNum) {
            ret |= ValueType(data_[index++]) << offset;
            offset += BitsNum;
            len -= BitsNum;
        }
        if (len) {
            ret |= ValueType(IndexUtils<uint64_t>::mask(data_[index], len)) << offset;
        }
        return ret;
    }

    auto getBitsU64(size_t pos, size_t len) const -> uint64_t {
        uint64_t index = pos / BitsNum;
        uint64_t offset = pos % BitsNum;
        if (offset + len < BitsNum) {
            return IndexUtils<uint64_t>::mask(data_[index] >> offset, len);
        }
        uint64_t ret = data_[index++] >> offset;
        offset = BitsNum - offset;
        len -= offset;
        if (len) {
            ret |= IndexUtils<uint64_t>::mask(data_[index], len) << offset;
        }
        return ret;
    }

    auto write(std::ofstream & ofs) const {
        auto data_size = static_cast<uint64_t>(data_.size());
        ofs.write((const char *)(&data_size), sizeof(data_size));
        ofs.write((const char *)(&data_[0]), sizeof(data_[0]) * data_size);
    }

    auto read(std::ifstream & ifs) {
        uint64_t data_size = 0;
        ifs.read((char *)(&data_size), sizeof(data_size));
        data_.resize(data_size);
        ifs.read((char *)(&data_[0]), sizeof(data_[0]) * data_size);
    }

    auto printInfo() {
        for (auto & e : data_) {
            std::cout << std::bitset<sizeof(e)*8>(e) << std::endl;
        }
    }
private:
    std::vector<uint64_t> data_;
};

}  // namespace ssindex
