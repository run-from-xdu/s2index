#pragma once

#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <thread>
#include <atomic>

namespace ssindex {

struct Task {
    //virtual Task() = 0;

    virtual ~Task() = 0;

    virtual Status Execute() = 0;

    virtual void SetPreExecute(std::function<void()> pre_exec) = 0;

    virtual void SetPostExecute(std::function<void()> post_exec) = 0;
};

class Worker {
public:
    explicit Worker()
        : task_queue_(std::queue<std::unique_ptr<Task>>{}),
          closed_(false),
          thread_(new std::thread([](Worker * w) { w->Run(); }, this)) {}

    ~Worker() {
        delete thread_;
    }

    static auto MakeNewWorker() -> std::unique_ptr<Worker> {
        return std::make_unique<Worker>();
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

            auto & task = task_queue_.front();
            task_queue_.pop();
            latch.unlock();

            std::cout << "Running a Task" << std::endl;
            auto s = task->Execute();
            if (s != Status::SUCCESS) {
                std::cerr << "Task failed" << std::endl;
                assert(false);
            }
            std::cout << "Finished a Task" << std::endl;
        }
    }

    void Stop() {
        std::lock_guard<std::mutex> lock{task_queue_mutex_};
        closed_ = true;
        task_queue_cv_.notify_all();
    }

    void Join() {
        thread_->join();
    }

    void PushTask(std::unique_ptr<Task> task) {
        std::lock_guard<std::mutex> lock{task_queue_mutex_};
        task_queue_.push(std::move(task));
        task_queue_cv_.notify_one();
    }
private:
    //std::queue<std::function<void()>> task_queue_;
    std::queue<std::unique_ptr<Task>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_queue_cv_;

    std::thread * thread_;
    std::atomic_bool closed_;
};

template<typename KeyType, typename ValueType>
class Scheduler {

};

}  // namespace ssindex