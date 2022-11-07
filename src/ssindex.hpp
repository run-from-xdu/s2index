#pragma once

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

private:

};

}  // namespace ssindex
