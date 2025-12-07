#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "inc/Core/Common.h"
#include "inc/Helper/Concurrent.h"

namespace SPTAG {
namespace Helper {
class EventTimer {
   public:
    static int64_t as_hour(
        const std::chrono::high_resolution_clock::duration &duration) {
        auto hours = std::chrono::duration_cast<std::chrono::hours>(duration);
        return hours.count();
    }

    static int64_t as_minutes(
        const std::chrono::high_resolution_clock::duration &duration,
        bool mod = false) {
        auto minutes =
            std::chrono::duration_cast<std::chrono::minutes>(duration);
        if (mod) {
            minutes %= std::chrono::hours(1);
        }
        return minutes.count();
    }

    static int64_t as_seconds(
        const std::chrono::high_resolution_clock::duration &duration,
        bool mod = false) {
        auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(duration);
        if (mod) {
            seconds %= std::chrono::minutes(1);
        }
        return seconds.count();
    }

    static int64_t as_ms(
        const std::chrono::high_resolution_clock::duration &duration,
        bool mod = false) {
        auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        if (mod) {
            ms %= std::chrono::seconds(1);
        }
        return ms.count();
    }

    static int64_t as_us(
        const std::chrono::high_resolution_clock::duration &duration,
        bool mod = false) {
        auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(duration);
        if (mod) {
            us %= std::chrono::milliseconds(1);
        }
        return us.count();
    }

    static std::string formatDuration(
        const std::chrono::high_resolution_clock::duration &duration,
        bool show_us = false) {
        auto hours = as_hour(duration);
        auto minutes = as_minutes(duration, true);
        auto seconds = as_seconds(duration, true);
        auto ms = as_ms(duration, true);
        auto us = as_us(duration, true);

        std::stringstream ss;
        if (hours > 0) ss << hours << "h";
        if (minutes > 0) ss << minutes << "min";
        if (seconds > 0) ss << seconds << "s";
        if (show_us) {
            if (ms > 0) ss << ms << "ms";
            ss << us << "us";
        } else {
            ss << ms << "ms";
        }
        return ss.str();
    }

    static EventTimer generate_average_timer(
        const std::vector<EventTimer> timer_arr) {
        EventTimer timer;
        timer.eventOrder_ = timer_arr[0].eventOrder_;
        for (auto &other : timer_arr) {
            for (auto &[ename, point] : other.timePoints_) {
                timer.timePoints_[ename].duration += point.duration;
                timer.timePoints_[ename].completed = true;
            }
        }
        for (auto &[_, point] : timer.timePoints_) {
            point.duration /= timer_arr.size();
        }
        return timer;
    }

    EventTimer() = default;
    ~EventTimer() = default;

    void add_lock() { lock_.reset(new Concurrent::SpinLock); }
    void merge_from_other(const EventTimer &other) {
        if (eventOrder_.empty()) {
            eventOrder_ = other.eventOrder_;
            timePoints_ = other.timePoints_;
            return;
        }
        for (auto &[ename, point] : other.timePoints_) {
            if (timePoints_.find(ename) == timePoints_.end()) {
                LOG_WARN("event [%s] not found\n", ename.c_str());
                continue;
            }
            timePoints_[ename].duration += point.duration;
            timePoints_[ename].completed = true;
        }
    }

    void event_start(std::string event_name, bool log_start = true) {
        if (lock_) lock_->Lock();
        if (timePoints_.find(event_name) == timePoints_.end()) {
            eventOrder_.push_back(event_name);
            if (log_start) {
                LOG_INFO("Start: %s\n", event_name.c_str());
            }
        }
        timePoints_[event_name].start =
            std::chrono::high_resolution_clock::now();
        if (lock_) lock_->Unlock();
    }

    void event_stop(std::string event_name) {
        if (lock_) lock_->Lock();
        auto stop = std::chrono::high_resolution_clock::now();
        if (timePoints_.find(event_name) != timePoints_.end()) {
            timePoints_[event_name].stop = stop;
            timePoints_[event_name].duration +=
                stop - timePoints_[event_name].start;
            timePoints_[event_name].completed = true;
        } else {
            LOG_WARN("event_name %s not found in timer\n", event_name.c_str());
        }
        if (lock_) lock_->Unlock();
    }

    std::chrono::high_resolution_clock::duration get_duration(
        std::string event_name) {
        if (lock_) lock_->Lock();
        auto dur = std::chrono::high_resolution_clock::duration(0);
        if (timePoints_.find(event_name) == timePoints_.end() ||
            !timePoints_.at(event_name).completed) {
        } else {
            dur = timePoints_.at(event_name).duration;
        }
        if (lock_) lock_->Unlock();
        return dur;
    }

    std::string get_formatted_duration(std::string event_name) const {
        if (timePoints_.find(event_name) == timePoints_.end() ||
            !timePoints_.at(event_name).completed) {
            return "Timer not found or not completed";
        }

        auto duration = timePoints_.at(event_name).duration;
        return formatDuration(duration);
    }

    // log events according to insertion order
    void log_all_event_duration(bool show_us = true) const {
        for (const auto &event_name : eventOrder_) {
            const auto &timer = timePoints_.at(event_name);
            if (timer.completed) {
                LOG_INFO("event %s elapsed %s\n", event_name.c_str(),
                         formatDuration(timer.duration, show_us).c_str());
            }
        }
    }

    void log_all_event_duration_and_clear(bool show_us = false) {
        for (const auto &event_name : eventOrder_) {
            const auto &timer = timePoints_.at(event_name);
            if (timer.completed) {
                LOG_INFO("event %s elapsed %s\n", event_name.c_str(),
                         formatDuration(timer.duration, show_us).c_str());
            }
        }
        clear();
    }

    void clear() {
        timePoints_.clear();
        eventOrder_.clear();
    }

    struct TimePoint {
        std::chrono::high_resolution_clock::time_point start;
        std::chrono::high_resolution_clock::time_point stop;
        std::chrono::high_resolution_clock::duration duration{0};
        bool completed = false;
    };

    std::map<std::string, TimePoint> &get_timePoints_() { return timePoints_; }

   private:
    std::map<std::string, TimePoint> timePoints_;
    std::vector<std::string> eventOrder_;
    std::shared_ptr<Concurrent::SpinLock> lock_ = nullptr;
};
}  // namespace Helper
}  // namespace SPTAG