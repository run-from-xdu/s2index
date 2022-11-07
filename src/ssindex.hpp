#pragma once

#include <string>
#include <utility>

#include "index_archived_file.hpp"
#include "index_block.hpp"

namespace ssindex {

/// Space-Saving Index
///
/// |SsIndex| supports basic key/value operations (e.g. set, get), if
/// a value associated with a key does exist, it would always be returned.
/// Occasionally, a random result would be returned if the given key
/// does not exist (i.e. false positive), but the FP rate is tunable.
template<typename KeyType, typename ValueType>
class SsIndex {
public:
    /// |KeyNotFound| will be returned as the result if key not found
    static constexpr ValueType KeyNotFound = static_cast<ValueType>(-1);

    explicit SsIndex(std::string directory) : working_directory_(std::move(directory)) {}

    auto Set(const KeyType & key, const ValueType & value);

    auto Get(const KeyType & key) -> ValueType;
private:
    /// Directory that stores all the files generated
    /// by the index
    std::string working_directory_;

    /// Archived files ｜ TODO: replace with the real implementation
    std::vector<std::unique_ptr<IndexArchivedFile<KeyType, ValueType>>> files_;

    /// in-memory hashtable
    std::unordered_map<KeyType, ValueType> memtable_;

    /// index blocks
    std::vector<std::unique_ptr<IndexBlock<ValueType>>> index_blocks_;


};

}  // namespace ssindex
