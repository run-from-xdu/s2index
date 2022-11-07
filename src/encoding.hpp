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
            return Status::PAGE_FULL;
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
        if (src.size() + sizeof(uint64_t) >= space) {
            return Status::PAGE_FULL;
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
        //std::cout << length << std::endl;
        src_ptr += sizeof(uint64_t);
        //dest->assign(src_ptr, length);
        //*dest = std::string{src_ptr};

        char * temp = new char[length + 1];
        memcpy(temp, src_ptr, length + 1);
        *dest = std::string{temp};

        if (used != nullptr) {
            *used = sizeof(uint64_t) + static_cast<size_t>(length);
        }
    }
};

template<>
class Codec<std::pair<std::string, uint64_t>> {
public:
    static auto EncodeValue(
            const std::pair<std::string, uint64_t> & entry,
            char * dest,
            size_t space,
            size_t * used = nullptr) -> Status {
        auto & key = entry.first;
        if (key.size() + 2 * sizeof(uint64_t) >= space) {
            return Status::PAGE_FULL;
        }
        uint64_t length = key.size();
        Codec<uint64_t>::EncodeValue(length, dest, sizeof(uint64_t));

        strcpy(dest + sizeof(uint64_t), key.data());

        Codec<uint64_t>::EncodeValue(entry.second, dest + sizeof(uint64_t) + length, sizeof(uint64_t));

        if (used != nullptr) {
            *used = 2 * sizeof(uint64_t) + static_cast<size_t>(length);
        }
        return Status::SUCCESS;
    }

    static auto DecodeValue(
            const char * src,
            std::pair<std::string, uint64_t> * dest,
            size_t * used = nullptr
            ) {
        std::string key{};
        uint64_t value;
        size_t used1 = 0, used2 = 0;
        Codec<std::string>::DecodeValue(src, &key, &used1);
        Codec<uint64_t>::DecodeValue(src + used1, &value, &used2);
        *dest = std::make_pair(std::move(key), value);
        if (used != nullptr) {
            *used = used1 + used2;
            //std::cout << *used << " xxx" << std::endl;
        }
    }
};


}  // namespace ssindex
