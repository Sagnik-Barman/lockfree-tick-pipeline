#pragma once
#include <array>
#include <atomic>
#include <cstddef>

// A single-producer, single-consumer lock-free ring buffer.
//
// This is the classic SPSC design (the same shape as Herb Sutter's
// "Lock-Free Queue" writeup and folly's ProducerConsumerQueue): because
// there is exactly one writer and one reader, each side only ever writes
// its *own* index and only ever reads the *other* side's index, so a pair
// of atomics with acquire/release ordering is enough -- no CAS loop, no
// ABA problem, no hazard pointers. That's the whole reason SPSC is worth
// reaching for instead of a general MPMC lock-free queue whenever your
// threading topology actually is single-producer/single-consumer: you get
// the same wait-free guarantee for a fraction of the complexity.
//
// head_ and tail_ are each pinned to their own cache line (`alignas(64)`)
// so the producer writing tail_ and the consumer writing head_ never cause
// false sharing -- without that padding, both atomics would very likely
// land on the same 64-byte cache line and every push/pop would bounce that
// line between the two cores' L1 caches, which is exactly the kind of
// hidden cost a naive lock-free implementation doesn't save you from.
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>, "T should be trivially copyable for a hot-path queue");

public:
    // Producer side. Returns false if the buffer is full (caller decides
    // whether to spin, drop, or apply backpressure -- this type never blocks).
    bool try_push(const T &item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = (tail + 1) & mask_;
        // Acquire: must see every slot the consumer has already freed up
        // (i.e. every head_ advance) before deciding we're full.
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[tail] = item;
        // Release: publishes the write above -- the consumer's matching
        // acquire load of tail_ is guaranteed to see buffer_[tail] too.
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Consumer side. Returns false if the buffer is empty.
    bool try_pop(T &out) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false; // empty
        }
        out = buffer_[head];
        head_.store((head + 1) & mask_, std::memory_order_release);
        return true;
    }

    static constexpr std::size_t capacity() { return Capacity; }

private:
    static constexpr std::size_t mask_ = Capacity - 1;
    std::array<T, Capacity> buffer_{};
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};
