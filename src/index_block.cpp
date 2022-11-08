#include "index_block.hpp"

#include <queue>
#include <cassert>

namespace ssindex {

template<typename ValueType>
auto IndexBlock<ValueType>::GetValue(const IndexEdge<ValueType> & ie) const -> ValueType {
    if (data_.BitsCount() == 0) {
        return IndexUtils<ValueType>::KeyNotFound;
    }

    uint64_t block_size = bits_occupied_by_value_ + bits_occupied_by_fp_;
    if (bits_occupied_by_fp_ != 0) {
        uint64_t fp_check = 0;
        for (uint64_t i = 0; i < 3; ++i) {
            fp_check ^= data_.getBits(ie.get(i, num_v_) * block_size, bits_occupied_by_fp_);
            //std::cout << fp_check << " ";
        }
        if (fp_check != IndexUtils<uint64_t>::mask(ie.v_[0] ^ ie.v_[1], bits_occupied_by_fp_)) {
            //std::cout << IndexUtils<uint64_t>::maskCheckLen(ie.v_[0] ^ ie.v_[1], bits_occupied_by_fp_) << std::endl;
            return IndexUtils<ValueType>::KeyNotFound;
        }
    }

    ValueType result{};
    for (uint64_t i = 0; i < 3; ++i) {
        result ^= data_.getBits(ie.get(i, num_v_) * block_size + bits_occupied_by_fp_, bits_occupied_by_value_);
    }

    if (result > max_value_) {
        return IndexUtils<ValueType>::KeyNotFound;
    }

    return result + min_value_;
}

template<typename ValueType>
auto IndexBlock<ValueType>::TryBuild(std::vector<IndexEdge<ValueType>> & index_edges,
              uint64_t seed,
              uint64_t fp_bits) -> Status {
    entry_num_ = static_cast<uint64_t>(index_edges.size());
    seed_ = seed;
    bits_occupied_by_fp_ = fp_bits;

    min_value_ = static_cast<ValueType>(-1);
    max_value_ = 0;

    /// find minimum and maximum value
    for (size_t i = 0; i < index_edges.size(); ++i) {
        if (index_edges[i].value_ < min_value_) {
            min_value_ = index_edges[i].value_;
        }
        if (index_edges[i].value_ > max_value_) {
            max_value_ = index_edges[i].value_;
        }
    }

    /// normalization
    for (size_t i = 0; i < index_edges.size(); ++i) {
        index_edges[i].value_ -= min_value_;
    }

    /// average bit usage per value (after normalization)
    bits_occupied_by_value_ = IndexUtils<ValueType>::log2(max_value_ - min_value_);
    std::cout << "Value Bits: " << bits_occupied_by_value_ << std::endl;
    /// number of vertices
    num_v_ = static_cast<uint64_t>(entry_num_ * kScale / double(3) + intercept);

    uint64_t bits_per_value_with_fp = bits_occupied_by_value_ + bits_occupied_by_fp_;
    uint64_t points_per_entry = 1;

    /// set index_edges
    uint64_t space = 0;
    std::vector<uint8_t> degs(num_v_ * 3 + space);
    std::vector<uint64_t> offsets(num_v_ * 3 + space + 1);

    for (size_t i = 0; i < index_edges.size(); ++i) {
        const IndexEdge<ValueType> & ie = index_edges[i];
        uint64_t len = 1;
        for (uint64_t j = 0; j < 3; ++j) {
            uint64_t t = ie.get(j, num_v_);
            for (uint64_t k = 0; k < len; ++k) {
                if (degs[t + k] == 0xFF) {
                    return Status::ERROR;
                }
                ++degs[t + k];
            }
        }
    }

    /// set offsets
    uint64_t sum = 0;
    for (size_t i = 0; i < degs.size(); ++i) {
        offsets[i] = sum;
        sum += degs[i];
        degs[i] = 0;
    }
    offsets.back() = sum;

    /// set edges
    uint64_t total_edge_num = entry_num_ * 3;
    std::vector<uint64_t> edges(total_edge_num);
    for (size_t i = 0; i < index_edges.size(); ++i) {
        const IndexEdge<ValueType> & ie = index_edges[i];
        uint64_t len = 1;
        for (uint64_t j = 0; j < 3; ++j) {
            uint64_t t = ie.get(j, num_v_);
            for (uint64_t k = 0; k < len; ++k) {
                edges[offsets[t + k] + degs[t + k]++] = i * points_per_entry + k;
            }
        }
    }

    /// init queue
    std::queue<uint64_t> q{};
    for (size_t i = 0; i < degs.size(); ++i) {
        if (degs[i] == 1) {
            q.push(edges[offsets[i]]);
        }
    }

    std::vector<std::pair<uint64_t, uint8_t>> extracted_edges{};
    uint64_t assign_num = entry_num_ * points_per_entry;

    BitVec<uint64_t> visited_edges(assign_num);
    uint64_t deleted_num = 0;
    while (!q.empty()) {
        uint64_t v = q.front();
        q.pop();

        if (visited_edges.getBit(v)) continue;
        visited_edges.setBit(v);
        ++deleted_num;

        uint64_t keyID  = v / points_per_entry;
        uint64_t offset = v % points_per_entry;

        const IndexEdge<ValueType>& ie(index_edges[keyID]);
        int choosed = -1;
        for (uint64_t i = 0; i < 3; ++i)
        {
            const uint64_t t = ie.get(i, num_v_);
            --degs[t + offset];

            if (degs[t + offset] == 0)
            {
                choosed = i;
                continue;
            }
            else if (degs[t + offset] >= 2)
            {
                continue;
            }
            // degs[t] == 1
            const uint64_t end = offsets[t + offset + 1];
            for (uint64_t j = offsets[t + offset]; j < end; ++j)
            {
                if (!visited_edges.getBit(edges[j]))
                {
                    q.push(edges[j]);
                    break;
                }
            }
        }
        assert(choosed != -1);
        extracted_edges.emplace_back(std::make_pair(v, choosed));
    }

    if (deleted_num != entry_num_) {
        return Status::ERROR;
    }

    assert(q.empty());
    data_.Resize(num_v_ * bits_per_value_with_fp * 3);

    uint64_t block_size = bits_per_value_with_fp;
    BitVec<uint64_t> visited_vertices(assign_num * 3);
    std::reverse(extracted_edges.begin(),  extracted_edges.end());
    for (auto & extracted_edge : extracted_edges) {
        const uint64_t v = extracted_edge.first;
        uint64_t key_index = v / points_per_entry;
        uint64_t offset = v % points_per_entry;
        const IndexEdge<ValueType> & ie = index_edges[key_index];

        uint64_t signature = IndexUtils<uint64_t>::mask(ie.v_[0] ^ ie.v_[1], bits_occupied_by_fp_);
        uint64_t bits_write_value = bits_per_value_with_fp;
        uint64_t bits = (ie.value_ << bits_occupied_by_fp_) + signature;

        for (uint64_t i = 0; i < 3; ++i) {
            const uint64_t t = ie.get(i, num_v_);
            if (!(visited_vertices.getBit(t + offset))) {
                continue;
            }
            //signature ^= data_.getBitsU64(t * block_size + offset, bits_occupied_by_fp_);
            bits ^= data_.getBits(t * block_size + offset, bits_write_value);
        }

        const uint64_t set_pos = ie.get(extracted_edge.second, num_v_);
        data_.setBits(set_pos * block_size + offset, bits_write_value, bits);
        visited_vertices.setBit(set_pos + offset);

//        uint64_t signature = IndexUtils<uint64_t>::maskCheckLen(ie.v_[0] ^ ie.v_[1], bits_occupied_by_fp_);
//
//        ValueType bits = ie.value_;
//        for (uint64_t i = 0; i < 3; ++i) {
//            const uint64_t t = ie.get(i, num_v_);
//            if (!(visited_vertices.getBit(t + offset))) {
//                continue;
//            }
//            signature ^= data_.getBitsU64(t * block_size + offset, bits_occupied_by_fp_);
//            bits ^= data_.getBits(t * block_size + offset + bits_occupied_by_fp_, bits_per_value_with_fp);
//        }
//
//        const uint64_t set_pos = ie.get(extracted_edge.second, num_v_);
//        data_.setBitsU64(set_pos * block_size + offset, bits_occupied_by_fp_, signature);
//        data_.setBits(set_pos * block_size + offset + bits_occupied_by_fp_, bits_per_value_with_fp, bits);
//        visited_vertices.setBit(set_pos + offset);
    }

    return Status::SUCCESS;
}

template class IndexBlock<uint64_t>;
template class IndexBlock<uint32_t>;
template class IndexBlock<uint16_t>;
template class IndexBlock<uint8_t>;

}  // namespace ssindex
