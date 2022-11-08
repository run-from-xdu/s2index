#pragma once

#include <string>
#include <fstream>

#include "index_common.hpp"

namespace ssindex {

template<typename ValueType>
class IndexEdge {
public:
    explicit IndexEdge() : code_(0) {}

    explicit IndexEdge(const char * str, const size_t len, const ValueType & code, const uint64_t seed)
        : code_(code) {
        HASH(str, len, seed, v_[0], v_[1], v_[2]);
    }

    auto get(uint64_t i, uint64_t partition_id) const -> uint64_t {
        return (v_[i] % partition_id) + partition_id * i;
    }

    auto operator < (const IndexEdge & other) const -> bool {
        for (uint64_t i = 0; i < NumHashFunctions; ++i) {
            if (v_[i] != other.v_[i]) return v_[i] < other.v_[i];
        }
        return false;
    }

    auto write(std::ofstream & ofs) const {
        ofs.write((const char *)(&code_), sizeof(code_));
        ofs.write((const char *)(&v_[0]), sizeof(v_[0]) * NumHashFunctions);
    }

    auto read(std::ifstream& ifs) {
        ifs.read((char *)(&code_), sizeof(code_));
        ifs.read((char *)(&v_[0]), sizeof(v_[0]) * NumHashFunctions);

    }
private:
    uint64_t v_[NumHashFunctions]{};

    ValueType code_;
};

}  // namespace ssindex