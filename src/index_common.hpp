#pragma once

#include <string>
#include <cassert>
#include <atomic>

namespace ssindex {

static const std::string default_working_directory = "/tmp/ssindex/";
static constexpr auto GetFullPath(const std::string & file_name) -> std::string {
    return default_working_directory + file_name;
}

static std::atomic_uint64_t file_sequence_number = 0;
static auto FetchNextArchivedFileName() -> std::string {
    return GetFullPath(std::to_string(file_sequence_number.fetch_add(1)) + ".arc");
}

static std::atomic_uint64_t memtable_sequence_number = 0;
static auto FetchMemtableId() -> uint64_t {
    return memtable_sequence_number.fetch_add(1);
}

using WriteBuffer = const char *;
using ReadBuffer = char *;

/// Number of Hash in IndexBlock
static constexpr size_t NumHashFunctions = 3;
/// Threshold of flushing memtable to the disk
static constexpr size_t MemtableFlushThreshold = 10000;
/// Threshold of compaction
static constexpr size_t CompactionThreshold = 4;
/// Default number of partitions
static constexpr uint64_t DefaultPartitionNum = 8;
/// Default false positive validation bits
static constexpr uint64_t DefaultFpBits = 16;

enum Status : int {
    ERROR = -1,
    SUCCESS = 0,
    PAGE_FULL = 1
};

#define BOB_MIX(a, b, c) \
    a -= b; a -= c; a ^= (c>>43); \
    b -= c; b -= a; b ^= (a<<9); \
    c -= a; c -= b; c ^= (b>>8); \
    a -= b; a -= c; a ^= (c>>38); \
    b -= c; b -= a; b ^= (a<<23); \
    c -= a; c -= b; c ^= (b>>5); \
    a -= b; a -= c; a ^= (c>>35); \
    b -= c; b -= a; b ^= (a<<49); \
    c -= a; c -= b; c ^= (b>>11); \
    a -= b; a -= c; a ^= (c>>12); \
    b -= c; b -= a; b ^= (a<<18); \
    c -= a; c -= b; c ^= (b>>22);

static auto HASH(const char * str, size_t len, uint64_t seed, uint64_t & a, uint64_t & b, uint64_t & c) {
    assert(str != nullptr);
    //std::cout << str << " " << len << std::endl;
    const auto * data = reinterpret_cast<const uint64_t *>(str);
    const uint64_t * end = data + len / 24;

    a = 0x9e3779b97f4a7c13LLU;
    b = seed;
    c = seed;
    while (data != end) {
        a += data[0];
        b += data[1];
        c += data[2];
        BOB_MIX(a, b, c)
        data += 3;
    }

    len -= 24 * (end - data);
    c += static_cast<uint64_t>(len);

    const auto * p = reinterpret_cast<const uint8_t*>(data);

    switch(len) {
        /// all the case statements fall through
        case 23 : c += (uint64_t)p[22] << 56;
        case 22 : c += (uint64_t)p[21] << 48;
        case 21 : c += (uint64_t)p[20] << 40;
        case 20 : c += (uint64_t)p[19] << 32;
        case 19 : c += (uint64_t)p[18] << 24;
        case 18 : c += (uint64_t)p[17] << 16;
        case 17 : c += (uint64_t)p[16] << 8;
        /// the first byte of c is reserved for the length
        case 16 : b += (uint64_t)p[15] << 56;
        case 15 : b += (uint64_t)p[14] << 48;
        case 14 : b += (uint64_t)p[13] << 40;
        case 13 : b += (uint64_t)p[12] << 32;
        case 12 : b += (uint64_t)p[11] << 24;
        case 11 : b += (uint64_t)p[10] << 16;
        case 10 : b += (uint64_t)p[ 9] << 8;
        case  9 : b += (uint64_t)p[ 8];
        case  8 : a += (uint64_t)p[ 7] << 56;
        case  7 : a += (uint64_t)p[ 6] << 48;
        case  6 : a += (uint64_t)p[ 5] << 40;
        case  5 : a += (uint64_t)p[ 4] << 32;
        case  4 : a += (uint64_t)p[ 3] << 24;
        case  3 : a += (uint64_t)p[ 2] << 16;
        case  2 : a += (uint64_t)p[ 1] << 8;
        case  1 : a += (uint64_t)p[ 0];
        default : break;
    }

    BOB_MIX(a, b, c)
}

static auto HASH(const char * buf, size_t len) -> uint64_t {
    const auto * data = reinterpret_cast<const uint8_t *>(buf);
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = len * m;

    auto get64bit = [](const uint8_t * v) -> uint64_t {
        return (uint64_t)v[0] | ((uint64_t)v[1] << 8) | ((uint64_t)v[2] << 16) | ((uint64_t)v[3] << 24) |
               ((uint64_t)v[4] << 32) | ((uint64_t)v[5] << 40) | ((uint64_t)v[6] << 48) | ((uint64_t)v[7] << 56);
    };

    while (len >= 8){
        uint64_t k = get64bit(data);

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
        data += 8;
        len -= 8;
    }

    switch(len & 7) {
        case 7: h ^= uint64_t(data[6]) << 48;
        case 6: h ^= uint64_t(data[5]) << 40;
        case 5: h ^= uint64_t(data[4]) << 32;
        case 4: h ^= uint64_t(data[3]) << 24;
        case 3: h ^= uint64_t(data[2]) << 16;
        case 2: h ^= uint64_t(data[1]) << 8;
        case 1: h ^= uint64_t(data[0]);
            h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

template<typename ValueType>
struct IndexUtils {
    static auto log2(const ValueType & x) -> uint64_t;

    static auto mask(const ValueType & x, const uint64_t & len) -> ValueType;

    static auto maskCheckLen(const ValueType & x, const uint64_t & len) -> ValueType;

    /// |key_not_found| will be returned as the result if key not found
    static constexpr ValueType KeyNotFound = static_cast<ValueType>(-1);

    static auto RawBuffer(const ValueType & value, size_t * length) -> std::unique_ptr<char>;
};

template<typename ValueType>
auto IndexUtils<ValueType>::RawBuffer(const ValueType & value, size_t * length) -> std::unique_ptr<char> {
    auto ret = std::make_unique<char>(sizeof(ValueType));
    *length = sizeof(ValueType);
    memcpy(ret.get(), &value, length);
    return ret;
}

template<>
inline auto IndexUtils<std::string>::RawBuffer(const std::string & value, size_t * length) -> std::unique_ptr<char> {
    *length = value.size();
    auto ret = std::make_unique<char>(value.size());
    memcpy(ret.get(), value.data(), value.size());
    return ret;
}

template<typename ValueType>
auto IndexUtils<ValueType>::log2(const ValueType & x) -> uint64_t {
    return 64 - __builtin_clzll(x);
}

template<>
inline auto IndexUtils<uint64_t>::log2(const uint64_t & x) -> uint64_t {
    return 64 - __builtin_clzll(x);
}

template<typename ValueType>
auto IndexUtils<ValueType>::mask(const ValueType & x, const uint64_t & len) -> ValueType {
    return x & ((static_cast<ValueType>(1) << len) - 1);
}

template<typename ValueType>
auto IndexUtils<ValueType>::maskCheckLen(const ValueType & x, const uint64_t & len) -> ValueType {
    if (len >= sizeof(ValueType) * 8) return x;
    return mask(x, len);
}

}  // namespace ssindex
