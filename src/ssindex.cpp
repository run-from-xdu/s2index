#include "ssindex.hpp"
#include "index_common.hpp"
#include "task_flush_memtable.hpp"

namespace ssindex {

template<typename KeyType, typename ValueType>
void SsIndex<KeyType, ValueType>::Set(const KeyType & key, const ValueType & value) {
    std::lock_guard<std::shared_mutex> w_latch{memtable_mutex_};
    memtable_.data_->insert_or_assign(key, value);
    if (memtable_.data_->size() == MemtableFlushThreshold) {
        /// schedule a flush task and reset the memtable
        std::lock_guard<std::shared_mutex> q_r_latch{waiting_queue_mutex_};
        waiting_queue_.emplace_back(std::move(memtable_));
        memtable_.id_ = FetchMemtableId();
        memtable_.data_ = std::make_shared<std::unordered_map<KeyType, ValueType>>();
        std::cout << "Enqueue Immutable | Current Size: " << waiting_queue_.size() << std::endl;
        auto partitioner = [this](const KeyType & key) -> uint64_t {
            size_t len = 0;
            auto buf = IndexUtils<KeyType>::RawBuffer(key, &len);
            return GetBlockPartition(buf.get(), len);
        };

        auto task = std::make_unique<FlushMemtableTask<KeyType, ValueType>>(*waiting_queue_.back().data_, waiting_queue_.back().id_, partition_num_, partitioner, seed_, fp_bits_);
        auto task_id = task->memtable_id_;
        auto pre = [task_id]() {
            std::cout << "Start flushing memtable, id: " << task_id << std::endl;
        };
        //auto memtable_id = task->memtable_id_;
        auto * raw_ptr = task.get();
        auto updateIndex = [this, raw_ptr]() {
            std::lock_guard<std::shared_mutex> q_w_latch{waiting_queue_mutex_};
            for (auto iter = waiting_queue_.begin(); iter != waiting_queue_.end(); iter++) {
                if (iter->id_ == raw_ptr->memtable_id_) {
                    waiting_queue_.erase(iter);
                    break;
                }
            }

            std::lock_guard<std::shared_mutex> imm_w_latch{immutable_part_mutex_};
            files_.emplace_back(std::move(raw_ptr->file_handle_));
            index_blocks_.emplace_back(std::move(raw_ptr->blocks_));
        };
        task->SetPreExecute(pre);
        task->SetPostExecute(updateIndex);
        scheduler_->ScheduleTask(std::move(task));
    }
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Get(const KeyType & key) -> ValueType {
    std::shared_lock<std::shared_mutex> mem_r_latch{memtable_mutex_};
    if (auto iter = memtable_.data_->find(key); iter != memtable_.data_->end()) {
        return iter->second;
    }
    mem_r_latch.unlock();

    std::shared_lock<std::shared_mutex> q_r_latch{waiting_queue_mutex_};
    for (auto & imm : waiting_queue_) {
        if (auto iter = imm.data_->find(key); iter != imm.data_->end()) {
            return iter->second;
        }
    }
    q_r_latch.unlock();

    size_t key_buf_len = 0;
    auto buf = IndexUtils<KeyType>::RawBuffer(key, &key_buf_len);
    uint64_t partition = GetBlockPartition(buf.get(), key_buf_len);
    assert(partition >= 0 && partition < partition_num_);
    IndexEdge<ValueType> ie{buf.get(), key_buf_len, 0, seed_};

    std::shared_lock<std::shared_mutex> imm_r_latch{immutable_part_mutex_};
    for (auto iter = index_blocks_.rbegin(); iter != index_blocks_.rend(); iter++) {
        //std::cout << iter->size() << std::endl;
        assert(iter->size() == partition_num_);
        auto ret = iter->at(partition).GetValue(ie);
        if (ret != key_not_found) {
            return ret;
        }
    }
//    for (size_t i = index_blocks_.size() - 1; i >= 0; --i) {
//        auto ret = index_blocks_[i][partition].GetValue(ie);
//        if (ret != key_not_found) {
//            return ret;
//        }
//    }

    return key_not_found;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::FlushAndBuildIndexBlocks() -> Status {
    if (memtable_.data_->empty()) {
        return Status::SUCCESS;
    }



    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::buildBlock(
        std::vector<std::pair<KeyType, ValueType>> & kvs,
        IndexBlock<ValueType> & block) -> Status {
    return Status::SUCCESS;
}

template<typename KeyType, typename ValueType>
auto SsIndex<KeyType, ValueType>::Flush() -> Status {
//    assert(!memtable_.empty());
//    std::unordered_map<KeyType, ValueType> raw_data{};
//    raw_data.swap(memtable_);
//
//    std::string file_name{};
//    auto file = std::make_unique<IndexArchivedFile<KeyType, ValueType>>(file_name, partition_num_);
//
//    for (auto iter = raw_data.begin(); iter != raw_data.end(); ++iter) {
//        auto & key = iter->first;
//        auto & value = iter->second;
//        uint64_t partition = GetBlockPartition();
//        file->WriteData(static_cast<size_t>(partition), key, value);
//    }

    return Status::SUCCESS;
}

template class SsIndex<std::string, uint64_t>;

}  // namespace ssindex
