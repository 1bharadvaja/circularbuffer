#include <pthread.h>
#include <mach/mach_init.h>
#include <mach/thread_policy.h>
#include <mach/thread_act.h>
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>

struct ringbuffer {
    //
    std::vector<int> data_;
    alignas(64) std::atomic<size_t> readIdx_{0};
    alignas(64) size_t writeIdxCached_{0};
    alignas(64) std::atomic<size_t> writeIdx_{0};
    alignas(64) size_t readIdxCached_{0};

    ringbuffer(size_t capacity): data_(capacity, 0) {}

    bool push(int val) {
        auto const writeIdx = writeIdx_.load(std::memory_order_relaxed);
        auto nextWriteIdx = writeIdx+1;
        if (nextWriteIdx == data_.size()) {
            nextWriteIdx = 0;
        }
        if (nextWriteIdx == readIdxCached_) {
            readIdxCached_ = readIdx_.load(std::memory_order_acquire);
            if (nextWriteIdx == readIdxCached_) {
            //our queue is full, we havent read any of the previously added
            //items yet
                return false;
            }
        }
        data_[writeIdx] = val;
        writeIdx_.store(nextWriteIdx, std::memory_order_release);
        return true;
    }

    //pop is a bool because we might need to block on popping when 
    //buffer is empty, i.e when readIdx and writeIdx are same

    bool pop(int& val) {
        auto const readIdx = readIdx_.load(std::memory_order_relaxed);
        if (readIdx == writeIdxCached_) {
            writeIdxCached_ = writeIdx_.load(std::memory_order_acquire);
            if (readIdx == writeIdxCached_) {
                return false;
            }
        }
        val = data_[readIdx];
        auto nextReadIdx = readIdx+1;
        if (nextReadIdx == data_.size()){
            nextReadIdx = 0;
        }
        
        readIdx_.store(nextReadIdx, std::memory_order_release);
        return true;

    }
    //important note here, we pass in a ref to an integer so we can
    //atomically update w/ val of popped data
    //and dont need to return back the actual value


};


void setMacAffinityTag(int tag) {
        thread_affinity_policy_data_t policy = { tag };
        thread_policy_set(
            mach_thread_self(), 
            THREAD_AFFINITY_POLICY, 
            (thread_policy_t)&policy, 
            THREAD_AFFINITY_POLICY_COUNT
        );
    }

void requestPerformanceCore() {
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
}

void requestEfficiencyCore() {
    pthread_set_qos_class_self_np(QOS_CLASS_BACKGROUND, 0);
}

template <typename T> void bench() {
    const size_t queueSize = 100000;
    const int64_t iters = 1000000;
    T q(queueSize);
    int shared_tag = 1;
    std::thread producer([&]() {
        setMacAffinityTag(shared_tag);
        requestPerformanceCore();

        for (int i = 0; i < 1000000; ++i) {
            while (!q.push(i)) { asm volatile("yield");}
        }


    });

    //main thread is the consumer

    setMacAffinityTag(shared_tag);
    requestPerformanceCore();
    auto start = std::chrono::steady_clock::now();
    int val;
    for (int i = 0; i < 1000000; ++i) {
        while (!q.pop(val)) { /* spin */ }
    }
    while (q.readIdx_.load(std::memory_order_relaxed) !=
         q.writeIdx_.load(std::memory_order_relaxed))
    ;
    auto stop = std::chrono::steady_clock::now();
    producer.join();
    std::cout << iters * 1000000000 /
                   std::chrono::duration_cast<std::chrono::nanoseconds>(stop -
                                                                        start)
                       .count()
            << " ops/s" << std::endl;

}

int main() {
    bench<ringbuffer>();
    return 0;
