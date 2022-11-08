#pragma once

#include <string>

namespace ssindex {

static const std::string default_working_directory = "/tmp/temp_data/";
static constexpr auto GetFullPath(const std::string & file_name) -> std::string {
    return default_working_directory + file_name;
}

using WriteBuffer = const char *;
using ReadBuffer = char *;

static constexpr size_t NumHashFunctions = 3;

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

template<typename ValueType>
struct IndexUtils {
    static auto log2(const ValueType & x) -> uint64_t;

    static auto mask(const ValueType & x, const uint64_t & len) -> ValueType;

    static auto maskCheckLen(const ValueType & x, const uint64_t & len) -> ValueType;

    /// |KeyNotFound| will be returned as the result if key not found
    static constexpr ValueType KeyNotFound = static_cast<ValueType>(-1);
};

template<typename ValueType>
auto IndexUtils<ValueType>::log2(const ValueType & x) -> uint64_t {
    return 0;
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
