// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_LOGGING_H_
#define _SPTAG_HELPER_LOGGING_H_

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <atomic>
#include <ctime>
#include <filesystem>
#include <fstream>

#pragma warning(disable : 4996)

namespace SPTAG {
namespace Helper {
enum class LogLevel {
    LL_Debug = 0,
    LL_Info,
    LL_Status,
    LL_Warning,
    LL_Error,
    LL_Assert,
    LL_Count,
    LL_Empty
};
static const char* LogLevelStr[] = {"debug", "info",   "status", "warning",
                                    "error", "assert", "count",  ""};

class Logger {
   public:
    virtual void Logging(const char* title, LogLevel level, const char* file,
                         int line, const char* func, const char* format,
                         ...) = 0;

   protected:
    void update_time() {
        time_t rawtime;
        struct tm* timeinfo;

        time(&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(time_buffer, 32, "%Y-%m-%d %H:%M:%S", timeinfo);
    }
    char time_buffer[32];
};

class LoggerHolder {
#if ((defined(_MSVC_LANG) && _MSVC_LANG >= 202002L) || __cplusplus >= 202002L)
   private:
    std::atomic<std::shared_ptr<Logger>> m_logger;

   public:
    LoggerHolder(std::shared_ptr<Logger> logger) : m_logger(logger) {}

    void SetLogger(std::shared_ptr<Logger> p_logger) { m_logger = p_logger; }

    std::shared_ptr<Logger> GetLogger() { return m_logger; }
#else
   private:
    std::shared_ptr<Logger> m_logger;

   public:
    LoggerHolder(std::shared_ptr<Logger> logger) : m_logger(logger) {}

    void SetLogger(std::shared_ptr<Logger> p_logger) {
        std::atomic_store(&m_logger, p_logger);
    }

    std::shared_ptr<Logger> GetLogger() { return std::atomic_load(&m_logger); }
#endif
};

class SimpleLogger : public Logger {
   public:
    SimpleLogger(LogLevel level) : m_level(level) {}

    virtual void Logging(const char* title, LogLevel level, const char* file,
                         int line, const char* func, const char* format, ...) {
        if (level < m_level) return;

        update_time();
        auto p = std::filesystem::path(file).filename();
        printf("[%s] [%s] [%s:%d] ", time_buffer, LogLevelStr[int(level)],
               p.c_str(), line);

        va_list args;
        va_start(args, format);

        vprintf(format, args);
        fflush(stdout);

        va_end(args);
    }

   private:
    LogLevel m_level;
};

class FileLogger : public Logger {
   public:
    FileLogger(LogLevel level, const char* file) : m_level(level) {
        m_handle.reset(new std::fstream(file, std::ios::out));
    }

    ~FileLogger() {
        if (m_handle != nullptr) m_handle->close();
    }

    virtual void Logging(const char* title, LogLevel level, const char* file,
                         int line, const char* func, const char* format, ...) {
        if (level < m_level || m_handle == nullptr || !m_handle->is_open())
            return;

        va_list args;
        va_start(args, format);

        char buffer[1024];
        int ret = vsprintf(buffer, format, args);
        if (ret > 0) {
            m_handle->write(buffer, strlen(buffer));
        } else {
            std::string msg("Buffer size is not enough!\n");
            m_handle->write(msg.c_str(), msg.size());
        }

        m_handle->flush();
        va_end(args);
    }

   private:
    LogLevel m_level;
    std::unique_ptr<std::fstream> m_handle;
};
}  // namespace Helper
}  // namespace SPTAG

#endif  // _SPTAG_HELPER_LOGGING_H_
