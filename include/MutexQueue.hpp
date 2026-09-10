#pragma once
#include <deque>
#include <mutex>

// The baseline this project benchmarks the lock-free ring buffer against:
// the "obvious" way to hand messages between two threads. Deliberately
// given the same polling shape as SpscRingBuffer (the consumer busy-loops
// on try_pop rather than blocking on a condition_variable) so the
// comparison isolates the cost of the synchronization primitive itself --
// mutex + deque vs. a pair of atomics -- rather than also mixing in the
// (real, but separate) wake-latency cost a condvar-based design would add.
template <typename T>
class MutexQueue {
public:
    void push(const T &item) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(item);
    }

    bool try_pop(T &out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        out = queue_.front();
        queue_.pop_front();
        return true;
    }

private:
    std::mutex mutex_;
    std::deque<T> queue_;
};
