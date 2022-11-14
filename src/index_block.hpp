#pragma once

#include "bitvec.hpp"
#include "index_edge.hpp"

namespace ssindex {

template<typename ValueType>
class IndexBlock {
public:
    /// Redundancy for bit array
    static constexpr double kScale = 1.3;
    static constexpr uint64_t intercept = 10;
    /// Default redundancy bits for false positive validation
    static constexpr uint64_t default_fp_bits = 0;

    explicit IndexBlock()
        : entry_num_(0)
        , min_value_(0)
        , max_value_(0)
        , bits_occupied_by_value_(0)
        , bits_occupied_by_fp_(default_fp_bits)
        , seed_(0x12345678)
        , level_(0)
        , num_v_(0) {}

    auto GetValue(const IndexEdge<ValueType> & edge) const -> ValueType;

    auto TryBuild(std::vector<IndexEdge<ValueType>> & edges,
                  uint64_t seed,
                  uint64_t fp_bits) -> Status;

    /// Number of bytes used by the block
    auto GetFootprint() const -> size_t {
        return data_.BitsCount() / 8 + sizeof(uint64_t) * 5 + sizeof(ValueType) * 2;
    }

    auto write(std::ofstream & ofs) const {
        ofs.write((const char *)(&entry_num_), sizeof(entry_num_));
        ofs.write((const char *)(&min_value_), sizeof(min_value_));
        ofs.write((const char *)(&max_value_), sizeof(max_value_));
        ofs.write((const char *)(&bits_occupied_by_value_), sizeof(bits_occupied_by_value_));
        ofs.write((const char *)(&bits_occupied_by_fp_), sizeof(bits_occupied_by_fp_));
        ofs.write((const char *)(&seed_), sizeof(seed_));
        ofs.write((const char *)(&num_v_), sizeof(num_v_));

        data_.write(ofs);
    }

    auto read(std::ifstream & ifs) {
        ifs.read((char *)(&entry_num_), sizeof(entry_num_));
        ifs.read((char *)(&min_value_), sizeof(min_value_));
        ifs.read((char *)(&max_value_), sizeof(max_value_));
        ifs.read((char *)(&bits_occupied_by_value_), sizeof(bits_occupied_by_value_));
        ifs.read((char *)(&bits_occupied_by_fp_), sizeof(bits_occupied_by_fp_));
        ifs.read((char *)(&seed_), sizeof(seed_));
        ifs.read((char *)(&num_v_), sizeof(num_v_));

        data_.read(ifs);
    }

    /// The level of this block
    int level_;

private:
    /// Succinct representation of the internal data
    BitVec<ValueType> data_;

    /// Minimum value in the block
    ValueType min_value_;

    /// Maximum value in the block
    ValueType max_value_;

    /// Number of bits used per value for storing the data
    uint64_t bits_occupied_by_value_;

    /// Number of bits used per value for false positive validation
    uint64_t bits_occupied_by_fp_;

    /// Seed for randomization
    uint64_t seed_;

    /// Number of vertices of each block in the hyper graph
    uint64_t num_v_;

    /// Number of key/value pairs in this block
    uint64_t entry_num_;
};

}  // namespace ssindex
