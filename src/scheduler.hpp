#pragma once

#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <thread>
#include <atomic>
#include <utility>
#include <unistd.h>

#include "index_common.hpp"

namespace ssindex {

struct Task {
    explicit Task() : pre_exec_([]{}), post_exec_([]{}) {}

    virtual ~Task() = default;

    virtual Status Execute() = 0;

    std::function<void()> pre_exec_;
    std::function<void()> post_exec_;

    void SetPreExecute(std::function<void()> && pre_exec) {
        pre_exec_ = std::move(pre_exec);
    }

    void SetPostExecute(std::function<void()> && post_exec) {
        post_exec_ = std::move(post_exec);
    }

    void PreExecute() const {
        pre_exec_();
    }

    void PostExecute() const {
        post_exec_();
    }
};

class Worker {
public:
    explicit Worker()
        : task_queue_(std::queue<std::unique_ptr<Task>>{}),
          closed_(false),
          pending_task_(0),
          thread_(new std::thread([](Worker * w) { w->Run(); }, this)) {}

    ~Worker() {
        delete thread_;
    }

    static auto MakeNewWorker() -> Worker * {
        return new Worker();
    }

    void Run() {
        while (!closed_.load()) {
            std::unique_lock latch{ task_queue_mutex_ };
            while(!closed_.load() && task_queue_.empty()) {
                task_queue_cv_.wait(latch);
            }
            if (closed_.load()) {
                break;
            }

            std::unique_ptr<Task> task{};
            task.swap(task_queue_.front());
            task_queue_.pop();
            latch.unlock();

            std::cout << "PreExecuting a Task" << std::endl;
            task->PreExecute();
            std::cout << "Running a Task" << std::endl;
            auto s = task->Execute();
            if (s != Status::SUCCESS) {
                std::cerr << "Task failed" << std::endl;
                assert(false);
            }
            std::cout << "PostExecuting a Task" << std::endl;
            task->PostExecute();
            std::cout << "Finished a Task" << std::endl;

            std::lock_guard<std::mutex> platch{ pending_mutex_ };
            pending_task_--;
            if (pending_task_ == 0) {
                task_finished_cv_.notify_all();
            }
        }
    }

    void Stop() {
        std::lock_guard<std::mutex> latch{task_queue_mutex_};
        closed_.store(true);
        task_queue_cv_.notify_all();
        std::cout << "Worker Stopped ......" << std::endl;
    }

    void Wait() {
        std::unique_lock<std::mutex> latch{pending_mutex_};
        while (!closed_.load() && pending_task_ != 0) {
            task_finished_cv_.wait(latch);
        }
        std::cout << "Worker waited ......" << std::endl;
    }

    void Join() {
        thread_->join();
    }

    void PushTask(std::unique_ptr<Task> && task) {
        std::lock_guard<std::mutex> latch{task_queue_mutex_};
        std::lock_guard<std::mutex> platch{pending_mutex_};
        task_queue_.push(std::move(task));
        pending_task_++;
        task_queue_cv_.notify_one();
    }
private:
    //std::queue<std::function<void()>> task_queue_;
    std::queue<std::unique_ptr<Task>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_queue_cv_;

    std::thread * thread_;
    std::atomic_bool closed_;

    uint64_t pending_task_;
    std::mutex pending_mutex_;
    std::condition_variable task_finished_cv_;
};

//template<typename KeyType, typename ValueType>
class Scheduler {
public:
    explicit Scheduler(size_t worker_num) : worker_num_(worker_num) {
        for (size_t i = 0; i < worker_num; ++i) {
            workers_.emplace_back(Worker::MakeNewWorker());
        }
        current_idx_ = 0;
    }

    auto ScheduleTask(std::unique_ptr<Task> && task) {
        FetchWorker()->PushTask(std::move(task));
    }

    auto Wait() {
        std::cout << "Waiting ......" << std::endl;
        for (auto & worker : workers_) {
            worker->Wait();
        }
    }

    auto Stop() {
        for (auto * worker : workers_) {
            worker->Stop();
            worker->Join();
            delete worker;
        }
        std::cout << "Stopped ......" << std::endl;
    }
private:
    auto FetchWorker() -> Worker * {
        return workers_[current_idx_.fetch_add(1) % worker_num_];
    }

    std::atomic_uint64_t current_idx_;

    size_t worker_num_;

    std::vector<Worker *> workers_;
};

}  // namespace ssindex