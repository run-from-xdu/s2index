cmake_minimum_required(VERSION 3.20)
project(s2index)

set(CMAKE_CXX_STANDARD 20)
set(libs2index_src
    src/bitvec.cpp
    src/file_manager.cpp
    src/ssindex.cpp
        src/index_archived_file.cpp
    src/index_block.cpp
        src/encoding.cpp
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

include(GoogleTest)
gtest_discover_tests(file_manager_test encoding_test index_archived_file_test)