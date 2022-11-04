#pragma once

#include <string>
#include <iostream>

#include "index_common.hpp"

namespace ssindex {

template<typename ValueType>
class Codec {
public:
    static auto EncodeValue(ValueType src, char * dest, size_t space, size_t * used = nullptr) -> Status {
        if (sizeof(ValueType) > space) {
            return Status::ERROR;
        }
        memcpy(dest, &src, sizeof(ValueType));

        if (used != nullptr) {
            *used = sizeof(ValueType);
        }
        return Status::SUCCESS;
    }

    static auto DecodeValue(const char * src, ValueType * dest, size_t * used = nullptr) {
        *dest = *reinterpret_cast<const ValueType *>(src);
        if (used != nullptr) {
            *used = sizeof(ValueType);
        }
    }
};

template<>
class Codec<std::string> {
public:
    static auto EncodeValue(std::string src, char * dest, size_t space, size_t * used = nullptr) -> Status {
        if (src.size() > space) {
            return Status::ERROR;
        }
        uint64_t length = src.size();
        Codec<uint64_t>::EncodeValue(length, dest, sizeof(uint64_t));

        strcpy(dest + sizeof(uint64_t), src.data());

        if (used != nullptr) {
            *used = sizeof(uint64_t) + static_cast<size_t>(length);
        }
        return Status::SUCCESS;
    }

    static auto DecodeValue(const char * src, std::string * dest, size_t * used = nullptr) {
        char * src_ptr = const_cast<char *>(src);
        uint64_t length = 0;
        Codec<uint64_t>::DecodeValue(src_ptr, &length);
        src_ptr += sizeof(uint64_t);
        dest->assign(src_ptr, length);

        if (used != nullptr) {
            *used = sizeof(uint64_t) + static_cast<size_t>(length);
        }
    }
};


}  // namespace ssindex
