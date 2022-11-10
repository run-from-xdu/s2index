cmake_minimum_required(VERSION 3.20)
project(s2index)

set(CMAKE_CXX_STANDARD 20)
set(libs2index_src
    src/file_manager.hpp
    src/file_manager.cpp
    src/ssindex.hpp
    src/ssindex.cpp
    src/index_archived_file.hpp
    src/index_archived_file.cpp
    src/index_block.hpp
    src/index_block.cpp
    src/encoding.hpp
    src/encoding.cpp

    src/index_edge.hpp
    src/index_common.hpp
    src/bitvec.hpp
        src/scheduler.hpp
        src/task_flush_memtable.hpp
)


include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

FetchContent_MakeAvailable(googletest)

add_library(libs2index ${libs2index_src})

add_executable(main main.cpp)

# Test

enable_testing()

add_executable(file_manager_test test/file_manager_test.cpp ${libs2index_src})
target_link_libraries(file_manager_test GTest::gtest_main)

add_executable(encoding_test test/encoding_test.cpp ${libs2index_src})
target_link_libraries(encoding_test GTest::gtest_main)

add_executable(index_archived_file_test test/index_archived_file_test.cpp ${libs2index_src})
target_link_libraries(index_archived_file_test GTest::gtest_main)

add_executable(bitvec_test test/bitvec_test.cpp ${libs2index_src})
target_link_libraries(bitvec_test GTest::gtest_main)

add_executable(index_common_test test/index_common_test.cpp ${libs2index_src})
target_link_libraries(index_common_test GTest::gtest_main)

add_executable(index_block_test test/index_block_test.cpp ${libs2index_src})
target_link_libraries(index_block_test GTest::gtest_main)

add_executable(scheduler_test test/scheduler_test.cpp ${libs2index_src})
target_link_libraries(scheduler_test GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(
        file_manager_test
        encoding_test
        index_archived_file_test
        bitvec_test
        index_block_test
)