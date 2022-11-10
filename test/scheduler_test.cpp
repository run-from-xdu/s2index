#include <gtest/gtest.h>
#include <iostream>

#include "../src/scheduler.hpp"
#include "../src/index_common.hpp"

namespace ssindex {

struct SumTask : public ssindex::Task {
    explicit SumTask(uint64_t o1, uint64_t o2) : operand_1_(o1), operand_2_(o2), result_(new uint64_t) {}

    ~SumTask() override {
        delete result_;
    }

    Status Execute() override {
        *result_ = operand_1_ + operand_2_;
        return Status::SUCCESS;
    }

    uint64_t operand_1_;
    uint64_t operand_2_;

    uint64_t * result_;
};

}

TEST(TestScheduler, Basic) {
    ssindex::Scheduler s{2};

    for (uint64_t i = 0; i < 100; i += 2) {
        auto task = std::make_unique<ssindex::SumTask>(i, i + 1);
        auto op1 = task->operand_1_;
        auto op2 = task->operand_2_;
        task->SetPreExecute([op1, op2]{
            std::cout << "Summing " << op1 << " + " << op2 << std::endl;
        });
        auto * result = task->result_;
        task->SetPostExecute([op1, op2, result]{
            std::cout << op1 << " + " << op2 << " = " << *(result) << std::endl;
        });
        s.ScheduleTask(std::move(task));
    }

    s.Wait();
    s.Stop();

    std::cout << "SUCCESS" << std::endl;
}