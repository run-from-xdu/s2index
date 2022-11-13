#include "../src/ssindex.hpp"

#include <gtest/gtest.h>
#include <ctime>

// total bytes allocated
static size_t bytes = 0;

// total number of malloc calls
static size_t allocations = 0;

// total number free calls
static size_t frees = 0;

// whether or not to track & print allocations
static bool output = false;

// static vector for tracking allocation sizes
//static std::vector<std::pair<void*, size_t>> sizes;

static void* alloc(size_t size) {
    void* p = std::malloc(size);
    if (output && p != nullptr) {
        // update stats
        bytes += size;
        ++allocations;
        //sizes.emplace_back(p, size);
        // for debugging
        // std::cout << "- allocating " << size << " bytes at memory location " << p << std::endl;
    }
    return p;
}

static void dealloc(void* p) {
    if (p == nullptr) {
        return;
    }
    if (output) {
        // update the stats
        ++frees;
//        size_t size = 0;
//        for (auto it = sizes.begin(); it != sizes.end(); ++it) {
//            if ((*it).first == p) {
//                size = (*it).second;
//                sizes.erase(it);
//                break;
//            }
//        }
        // for debugging
        // std::cout << "- freeing " << size << " bytes at memory location " << p << std::endl;
    }
    std::free(p);
}

static void enableOutput() {
    output = true;
    allocations = 0;
    frees = 0;
    bytes = 0;
    //sizes.clear();
}

static void disableOutput() {
    output = false;
    size_t size = 0;
//    for (auto it = sizes.begin(); it != sizes.end(); ++it) {
//        size += (*it).second;
//    }
    std::cout << "=> " << size << " bytes allocd at end - total: " << bytes << " bytes mallocd, " << allocations << " malloc(s), " << frees << " free(s)" << std::endl;
}


void* operator new(size_t size) {
    return alloc(size);
}

void* operator new(size_t size, std::nothrow_t const&) noexcept {
    return alloc(size);
}

void* operator new[](size_t size) {
    return alloc(size);
}

void* operator new[](size_t size, std::nothrow_t const&) noexcept {
    return alloc(size);
}

void operator delete(void* p) noexcept {
    dealloc(p);
}

void operator delete(void* p, std::nothrow_t const&) noexcept {
    dealloc(p);
}

void operator delete[](void* p) noexcept {
    dealloc(p);
}

void operator delete[](void* p, std::nothrow_t const&) noexcept {
    dealloc(p);
}


TEST(TestE2E, Flushing) {
    auto fileExists = [](const std::string & file_name) -> bool {
        std::ifstream f(file_name.c_str());
        return f.good();
    };
    if (fileExists(ssindex::default_working_directory)) {
        //std::filesystem::remove(ssindex::default_working_directory);
        std::filesystem::remove_all(ssindex::default_working_directory);
    }

    auto index = ssindex::SsIndex<std::string, uint64_t>(ssindex::default_working_directory);
    uint64_t entry_num = 50000;
    for (uint64_t i = 0; i < entry_num; i++) {
        index.Set(std::to_string(i), i);
        //std::cout << i << std::endl;
    }

    index.WaitTaskComplete();

    index.PrintInfo();

    auto correct = 0;
    auto wrong = 0;

    clock_t start = clock();

    for (uint64_t i = 0; i < entry_num; i++) {
        //std::cout << i << std::endl;
        auto val = index.Get(std::to_string(i));
        //EXPECT_EQ(val, i);
        if (val == i) correct++;
        else wrong++;
    }

    clock_t end = clock();
    std::cout << "SsIndex: " << double(end - start) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | ";
    std::cout << "Memory Usage: " << index.GetUsage() << " Bytes" << std::endl;
    //std::cout << correct << " " << wrong << std::endl;

    struct TestKV {
        std::unordered_map<std::string, uint64_t> data;
        std::shared_mutex latch;

        void Set(const std::string & k, uint64_t v) {
            std::lock_guard<std::shared_mutex> w_latch{latch};
            data[k] = v;
        }

        uint64_t Get(const std::string & k) {
            std::shared_lock<std::shared_mutex> r_latch{latch};
            return data[k];
        }
    };

    TestKV kv{};
    for (uint64_t i = 0; i < entry_num; i++) {
        kv.Set(std::to_string(i), i);
    }

    /// NOTICE: I use dedicated program to calculate the memory usage of
    /// std::unordered_map, so just ignore those related code here.
    //sizes.reserve(4096);
    //enableOutput();
    start = clock();
    for (uint64_t i = 0; i < entry_num; i++) {
        EXPECT_EQ(i, kv.Get(std::to_string(i)));
    }
    end = clock();
    std::cout << "Standard Unordered Map: " << double(end - start) / CLOCKS_PER_SEC * 1000 * 1000 / double(entry_num) << " us/op | Memory Usage: " << bytes << std::endl;
    //disableOutput();

}