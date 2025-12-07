#pragma once

#include <jemalloc/jemalloc.h>
#include <malloc.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "inc/Core/Common.h"

namespace SPTAG {
namespace Helper {

enum MEMORY_SOURCE {
    UNKNOWN = 0,
    SPANN_META,
    // SPMerge runtime metadata
    SPMergeMeta,
    // Recovery buffer
    SPMergeRecovery,
    /* Hard to classify memory usage from basic data structure to logical usage,
     * so record each data structure separately
     */
    // Basic data structure
    SPMergeVIDList,
    SPMergeCIDList,
    // Upper level data structure
    SPMergeCContent,
    SPMergeCAssign,
    SPMergePatch,
    MAX_SOURCES,
};

constexpr uint64_t KB = 1024;
constexpr uint64_t MB = 1024 * 1024;
constexpr uint64_t GB = 1024 * 1024 * 1024;

// TODO: add memory limitations
class MemoryTracker {
   public:
    static void init();

    static inline bool is_full() {
        return system_memory_available_ > 0 &&
               used_memory_size_.load() >= system_memory_available_;
    }

    static void *alloc(uint64_t size, int src);

    static void *aligned_alloc(uint64_t alignment, uint64_t size, int src);

    static int memalign(void **ptr, int alignment, uint64_t size, int src);

    static void memalign_free(void *ptr, int src);

    static void *realloc(void *ptr, uint64_t size, int src);

    static void free(void *ptr, int src);

    static void *alloc64B(uint64_t nbytes, int src) {
        uint64_t len = (nbytes + 63) & ~63;
        auto p = MemoryTracker::aligned_alloc(1 << 6, len, src);
        return p;
    }

    static inline void addUsed(uint64_t size,
                               int src = MEMORY_SOURCE::UNKNOWN) {
        local_used_memory_size_ += size;
        auto cur_total = used_memory_size_ += size;
        auto cur_source = used_memory_by_source_[src] += size;
        peak_memory_size_ = std::max(peak_memory_size_.load(), cur_total);
        peak_memory_by_source_[src] =
            std::max(peak_memory_by_source_[src].load(), cur_source);
    }

    static inline void addExternalUsed(uint64_t size, int src) {
        auto cur_total = used_memory_size_ + size;
        auto cur_source = used_memory_by_source_[src] + size;
        peak_memory_size_ = std::max(peak_memory_size_.load(), cur_total);
        peak_memory_by_source_[src] =
            std::max(peak_memory_by_source_[src].load(), cur_source);
    }

    static inline void subUsed(uint64_t size,
                               int src = MEMORY_SOURCE::UNKNOWN) {
        // FIXME:
        if (src == MEMORY_SOURCE::UNKNOWN and
            used_memory_by_source_[MEMORY_SOURCE::UNKNOWN] < size) {
            // auto diff = size -
            // used_memory_by_source_[MEMORY_SOURCE::UNKNOWN]; if
            // (system_used_memory_ >= diff)
            //     system_used_memory_ -= diff;
            // else
            //     system_used_memory_ = 0;
            size = used_memory_by_source_[MEMORY_SOURCE::UNKNOWN];
        }
        local_used_memory_size_ -= size;
        used_memory_size_ -= size;
        used_memory_by_source_[src] -= size;
    }

    static inline uint64_t getLocalUsedMemorySize() {
        return local_used_memory_size_;
    }

    static uint64_t malloc_usable_size(void *ptr) {
        return ::malloc_usable_size(ptr);
    }

    static inline uint64_t getMaxMemorySize() { return max_memory_size_; }

    static inline uint64_t getSystemMemoryAvailable() {
        return system_memory_available_;
    }

    static inline uint64_t getUsedMemorySize() {
        return used_memory_size_.load();
    }

    static inline uint64_t getFreeMemorySize() {
        if (is_full()) return 0;
        return system_memory_available_ - used_memory_size_.load();
    }

    static void dumpCurrentStats();
    static void dumpPeakStats();
    static void checkStats(std::string desc);

    static uint64_t getMemoryUsage(int src) {
        return used_memory_by_source_[src];
    }

   private:
    static thread_local uint64_t local_used_memory_size_;
    static std::atomic<uint64_t> used_memory_size_ __attribute__((aligned(64)));
    static std::atomic<uint64_t>
        used_memory_by_source_[MEMORY_SOURCE::MAX_SOURCES];
    // peak usage
    static std::atomic<uint64_t> peak_memory_size_ __attribute__((aligned(64)));
    static std::atomic<uint64_t>
        peak_memory_by_source_[MEMORY_SOURCE::MAX_SOURCES];
    // limitations
    static uint64_t max_memory_size_;
    static uint64_t system_used_memory_;
    static uint64_t system_memory_available_;
    static uint64_t reserved_memory_;
};

template <typename T, int src = MEMORY_SOURCE::UNKNOWN>
class TrackerAllocator {
   public:
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T &;
    using const_reference = const T &;
    using value_type = T;

    TrackerAllocator() = default;
    template <typename U>
    constexpr TrackerAllocator(const TrackerAllocator<U, src> &) noexcept {}

    template <typename U>
    struct rebind {
        typedef TrackerAllocator<U, src> other;
    };

    pointer allocate(size_type n) {
        if (auto p = static_cast<pointer>(
                MemoryTracker::alloc(n * sizeof(T), src))) {
            return p;
        }
        throw std::bad_alloc();
    }

    void deallocate(pointer p, size_type) noexcept {
        MemoryTracker::free(p, src);
    }

    void construct(pointer p, const_reference value) {
        new (p) value_type(value);
    }

    void destroy(pointer p) { p->~value_type(); }
};

template <typename T, int TS, typename U, int US>
bool operator==(const TrackerAllocator<T, TS> &a,
                const TrackerAllocator<U, US> &b) {
    return TS == US;
}

template <typename T, int TS, typename U, int US>
bool operator!=(const TrackerAllocator<T, TS> &a,
                const TrackerAllocator<U, US> &b) {
    return TS != US;
}

template <typename T, int S, typename U>
bool operator==(const TrackerAllocator<T, S> &a, const std::allocator<U> &b) {
    return false;
}

template <typename T, int S, typename U>
bool operator!=(const TrackerAllocator<T, S> &a, const std::allocator<U> &b) {
    return true;
}

class MemoryUsageGuard {
   public:
    MemoryUsageGuard(std::atomic<uint64_t> &usage) : usage_(usage) {
        before_ = MemoryTracker::getLocalUsedMemorySize();
    }

    ~MemoryUsageGuard() {
        uint64_t after = MemoryTracker::getLocalUsedMemorySize();
        usage_.fetch_add(after - before_);
    }

   private:
    std::atomic<uint64_t> &usage_;
    uint64_t before_;
};

class ExternalMemUsageGuard {
   public:
    ExternalMemUsageGuard(std::atomic<uint64_t> &usage, MEMORY_SOURCE src);

    ~ExternalMemUsageGuard();

   private:
    MEMORY_SOURCE src;
    std::atomic<uint64_t> &usage;
    uint64_t initial_state;
};

class JemallocProfiler {
   public:
    static bool Initialize() {
        bool enabled = false;
        size_t len = sizeof(bool);
        if (mallctl("opt.prof", &enabled, &len, nullptr, 0) != 0 or
            not enabled) {
            throw std::runtime_error("Error: prof not enabled in jemalloc");
        }

        StartProfiling();

        return true;
    }

    static void StartProfiling() {
        bool enabled = true;
        mallctl("prof.active", nullptr, nullptr, &enabled, sizeof(bool));
    }

    static void StopProfiling() {
        bool enabled = false;
        mallctl("prof.active", nullptr, nullptr, &enabled, sizeof(bool));
    }

    static void DumpProfile(const char *filename) {
        uint64_t epoch = 1;
        size_t sz = sizeof(epoch);
        mallctl("epoch", &epoch, &sz, &epoch, sz);

        if (mallctl("prof.dump", nullptr, nullptr, &filename,
                    sizeof(const char *)) != 0) {
            throw std::runtime_error("Error: couldn't dump profile");
        } else {
            LOG_INFO("Profile dumped to %s\n", filename);
        }
    }
};

template <typename T>
using vector_recovery =
    std::vector<T, TrackerAllocator<T, MEMORY_SOURCE::SPMergeRecovery>>;

template <typename K, typename V>
using unordered_map_recovery = std::unordered_map<
    K, V, std::hash<K>, std::equal_to<K>,
    TrackerAllocator<std::pair<const K, V>, MEMORY_SOURCE::SPMergeRecovery>>;

template <typename K>
using unordered_set_recovery =
    std::unordered_set<K, std::hash<K>, std::equal_to<K>,
                       TrackerAllocator<K, MEMORY_SOURCE::SPMergeRecovery>>;

using tracked_buffer = std::unique_ptr<char[], std::function<void(char *)>>;
tracked_buffer make_tracked_buffer(
    uint64_t size, MEMORY_SOURCE src = MEMORY_SOURCE::SPMergeRecovery);

template <typename T>
inline void free_space(T &container) {
    T().swap(container);
}

}  // namespace Helper
}  // namespace SPTAG