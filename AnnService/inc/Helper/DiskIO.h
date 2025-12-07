// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_DISKIO_H_
#define _SPTAG_HELPER_DISKIO_H_

#include <fcntl.h>
#include <string.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace SPTAG {
struct SimpleIOHelper;

namespace Helper {
enum class DiskIOScenario {
    DIS_BulkRead = 0,
    DIS_UserRead,
    DIS_HighPriorityUserRead,
    DIS_BulkWrite,
    DIS_UserWrite,
    DIS_HighPriorityUserWrite,
    DIS_Count
};

struct AsyncReadRequest {
    std::uint64_t m_offset;
    std::uint64_t m_readSize;
    char* m_buffer;
    std::function<void(bool)> m_callback;
    int m_status;

    // Carry items like counter for callback to process.
    void* m_payload;
    bool m_success;

    // Carry exension metadata needed by some DiskIO implementations
    void* m_extension;

    AsyncReadRequest()
        : m_offset(0),
          m_readSize(0),
          m_buffer(nullptr),
          m_status(0),
          m_payload(nullptr),
          m_success(false),
          m_extension(nullptr) {}
};

class DiskIO {
   public:
    DiskIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

    virtual ~DiskIO() {}

    virtual bool Initialize(const char* filePath, int openMode,
                            // Max read/write buffer size.
                            std::uint64_t maxIOSize = (1 << 20),
                            std::uint32_t maxReadRetries = 2,
                            std::uint32_t maxWriteRetries = 2,
                            std::uint16_t threadPoolSize = 4) = 0;

    virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer,
                                     std::uint64_t offset = UINT64_MAX) = 0;

    virtual std::uint64_t WriteBinary(std::uint64_t writeSize,
                                      const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) = 0;

    virtual std::uint64_t ReadString(std::uint64_t& readSize,
                                     std::unique_ptr<char[]>& buffer,
                                     char delim = '\n',
                                     std::uint64_t offset = UINT64_MAX) = 0;

    virtual std::uint64_t WriteString(const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) = 0;

    virtual bool ReadFileAsync(AsyncReadRequest& readRequest) { return false; }

    // interface method for waiting for async read to complete when underlying
    // callback support is not available.
    virtual void Wait(AsyncReadRequest& readRequest) { return; }

    virtual bool BatchReadFile(AsyncReadRequest* readRequests,
                               std::uint32_t requestCount) {
        return false;
    }

    virtual bool BatchCleanRequests(
        SPTAG::Helper::AsyncReadRequest* readRequests,
        std::uint32_t requestCount) {
        return false;
    }

    virtual std::uint64_t TellP() = 0;

    virtual void SeekP(std::uint64_t offset) {}

    virtual void ShutDown() = 0;

    virtual int GetFD() { return -1; }
};

class SimpleFileIO : public DiskIO {
   public:
    SimpleFileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

    virtual ~SimpleFileIO() { ShutDown(); }

    virtual bool Initialize(const char* filePath, int openMode,
                            // Max read/write buffer size.
                            std::uint64_t maxIOSize = (1 << 20),
                            std::uint32_t maxReadRetries = 2,
                            std::uint32_t maxWriteRetries = 2,
                            std::uint16_t threadPoolSize = 4) {
        m_handle.reset(
            new std::fstream(filePath, (std::ios::openmode)openMode));
        return m_handle->is_open();
    }

    virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer,
                                     std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->seekg(offset, std::ios::beg);
        m_handle->read((char*)buffer, readSize);
        return m_handle->gcount();
    }

    virtual std::uint64_t WriteBinary(std::uint64_t writeSize,
                                      const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->seekp(offset, std::ios::beg);
        m_handle->write((const char*)buffer, writeSize);
        if (m_handle->fail() || m_handle->bad()) return 0;
        return writeSize;
    }

    virtual std::uint64_t ReadString(std::uint64_t& readSize,
                                     std::unique_ptr<char[]>& buffer,
                                     char delim = '\n',
                                     std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->seekg(offset, std::ios::beg);
        std::uint64_t readCount = 0;
        for (int _Meta = m_handle->get();; _Meta = m_handle->get()) {
            if (_Meta == '\r') _Meta = '\n';

            if (readCount >= readSize) {  // buffer full
                readSize *= 2;
                std::unique_ptr<char[]> newBuffer(new char[readSize]);
                memcpy(newBuffer.get(), buffer.get(), readCount);
                buffer.swap(newBuffer);
            }

            if (_Meta == EOF) {  // eof
                buffer[readCount] = '\0';
                break;
            } else if (_Meta ==
                       delim) {  // got a delimiter, discard it and quit
                buffer[readCount++] = '\0';
                if (delim == '\n' && m_handle->peek() == '\n') {
                    readCount++;
                    m_handle->ignore();
                }
                break;
            } else {  // got a character, add it to string
                buffer[readCount++] =
                    std::char_traits<char>::to_char_type(_Meta);
            }
        }
        return readCount;
    }

    virtual std::uint64_t WriteString(const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        return WriteBinary(strlen(buffer), (const char*)buffer, offset);
    }

    virtual std::uint64_t TellP() { return m_handle->tellp(); }
    virtual void SeekP(std::uint64_t offset) {
        m_handle->seekp(offset, std::ios::beg);
    }

    virtual void ShutDown() {
        if (m_handle != nullptr) m_handle->close();
    }

   private:
    std::unique_ptr<std::fstream> m_handle;
};

namespace extended_flags {
static const std::ios_base::openmode direct = std::ios_base::openmode(1 << 17);
}

class NativeFileIO : public DiskIO {
   private:
    int m_fd = -1;
    bool m_isDirect = false;
    size_t m_blockSize;

   public:
    NativeFileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

    virtual ~NativeFileIO() { ShutDown(); }

    virtual bool Initialize(const char* filePath, int openMode,
                            std::uint64_t maxIOSize = (1 << 20),
                            std::uint32_t maxReadRetries = 2,
                            std::uint32_t maxWriteRetries = 2,
                            std::uint16_t threadPoolSize = 4) {
        int flags;
        if (openMode & std::ios::in and openMode & std::ios::out)
            flags = O_RDWR;
        else if (openMode & std::ios::in) {
            flags = O_RDONLY;
        } else if (openMode & std::ios::out) {
            flags = O_WRONLY;
        } else {
            throw std::logic_error("no access specified");
        }

        if (openMode & std::ios::out) flags |= O_CREAT;
        if (openMode & std::ios::app) flags |= O_APPEND;
        if (openMode & std::ios::trunc) flags |= O_TRUNC;
        if (openMode & extended_flags::direct) flags |= O_DIRECT;

        m_fd = open(filePath, flags, 0644);
        m_blockSize = get_block_size();
        return m_fd != -1;
    }

    virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer,
                                     std::uint64_t offset = UINT64_MAX) {
        if (!buffer || m_fd == -1) return 0;
        check_direct_io_alignment(buffer, readSize, offset);

        ssize_t bytesRead;
        if (offset != UINT64_MAX) {
            bytesRead = pread(m_fd, buffer, readSize, offset);
        } else {
            bytesRead = read(m_fd, buffer, readSize);
        }

        return bytesRead > 0 ? bytesRead : 0;
    }

    virtual std::uint64_t WriteBinary(std::uint64_t writeSize,
                                      const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        if (!buffer || m_fd == -1) return 0;
        check_direct_io_alignment(buffer, writeSize, offset);

        ssize_t bytesWritten;
        if (offset != UINT64_MAX) {
            bytesWritten = pwrite(m_fd, buffer, writeSize, offset);
        } else {
            bytesWritten = write(m_fd, buffer, writeSize);
        }

        return bytesWritten > 0 ? bytesWritten : 0;
    }

    virtual std::uint64_t ReadString(std::uint64_t& readSize,
                                     std::unique_ptr<char[]>& buffer,
                                     char delim = '\n',
                                     std::uint64_t offset = UINT64_MAX) {
        return 0;
    }

    virtual std::uint64_t WriteString(const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        check_direct_io_alignment(buffer, strlen(buffer), offset);
        return WriteBinary(strlen(buffer), buffer, offset);
    }

    virtual std::uint64_t TellP() { return lseek(m_fd, 0, SEEK_CUR); }

    virtual void SeekP(std::uint64_t offset) { lseek(m_fd, offset, SEEK_SET); }

    virtual void ShutDown() {
        if (m_fd != -1) {
            close(m_fd);
            m_fd = -1;
        }
        m_isDirect = false;
    }

    virtual int GetFD() { return m_fd; }

    size_t get_block_size() const {
        struct statvfs st;
        if (fstatvfs(m_fd, &st) == -1) {
            return 512;
        }
        return st.f_bsize;
    }

   private:
    void inline check_direct_io_alignment(const char* buffer, uint64_t io_size,
                                          uint64_t offset) const {
#ifdef DEBUG
        if (m_isDirect) {
            if (io_size % m_blockSize) {
                throw std::runtime_error("read size not aligned!");
            }
            if ((uint64_t)buffer % m_blockSize) {
                throw std::runtime_error("buffer start address not aligned!");
            }
            if (offset != UINT64_MAX and offset % m_blockSize) {
                throw std::runtime_error("offset not aligned!");
            }
        }
#endif
    }
};

class SimpleBufferIO : public DiskIO {
   public:
    struct streambuf : public std::basic_streambuf<char> {
        streambuf() {}

        streambuf(char* buffer, size_t size) {
            setg(buffer, buffer, buffer + size);
            setp(buffer, buffer + size);
        }

        std::uint64_t tellp() {
            if (pptr()) return pptr() - pbase();
            return 0;
        }
    };

    SimpleBufferIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

    virtual ~SimpleBufferIO() { ShutDown(); }

    virtual bool Initialize(const char* filePath, int openMode,
                            // Max read/write buffer size.
                            std::uint64_t maxIOSize = (1 << 20),
                            std::uint32_t maxReadRetries = 2,
                            std::uint32_t maxWriteRetries = 2,
                            std::uint16_t threadPoolSize = 4) {
        if (filePath != nullptr)
            m_handle.reset(new streambuf((char*)filePath, maxIOSize));
        else
            m_handle.reset(new streambuf());
        return true;
    }

    virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer,
                                     std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->pubseekpos(offset);
        return m_handle->sgetn((char*)buffer, readSize);
    }

    virtual std::uint64_t WriteBinary(std::uint64_t writeSize,
                                      const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->pubseekpos(offset);
        if ((std::uint64_t)m_handle->sputn((const char*)buffer, writeSize) <
            writeSize)
            return 0;
        return writeSize;
    }

    virtual std::uint64_t ReadString(std::uint64_t& readSize,
                                     std::unique_ptr<char[]>& buffer,
                                     char delim = '\n',
                                     std::uint64_t offset = UINT64_MAX) {
        if (offset != UINT64_MAX) m_handle->pubseekpos(offset);
        std::uint64_t readCount = 0;
        for (int _Meta = m_handle->sgetc();; _Meta = m_handle->snextc()) {
            if (_Meta == '\r') _Meta = '\n';

            if (readCount >= readSize) {  // buffer full
                readSize *= 2;
                std::unique_ptr<char[]> newBuffer(new char[readSize]);
                memcpy(newBuffer.get(), buffer.get(), readCount);
                buffer.swap(newBuffer);
            }

            if (_Meta == EOF) {  // eof
                buffer[readCount] = '\0';
                break;
            } else if (_Meta ==
                       delim) {  // got a delimiter, discard it and quit
                buffer[readCount++] = '\0';
                m_handle->sbumpc();
                if (delim == '\n' && m_handle->sgetc() == '\n') {
                    readCount++;
                    m_handle->sbumpc();
                }
                break;
            } else {  // got a character, add it to string
                buffer[readCount++] =
                    std::char_traits<char>::to_char_type(_Meta);
            }
        }
        return readCount;
    }

    virtual std::uint64_t WriteString(const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        return WriteBinary(strlen(buffer), (const char*)buffer, offset);
    }

    virtual std::uint64_t TellP() { return m_handle->tellp(); }

    virtual void ShutDown() {}

   private:
    std::unique_ptr<streambuf> m_handle;
};

class ChunkedBufferIO : public DiskIO {
   public:
    ChunkedBufferIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}
    virtual ~ChunkedBufferIO() { ShutDown(); }

    virtual bool Initialize(const char* filePath, int openMode,
                            // Max read/write buffer size.
                            std::uint64_t maxIOSize = (1 << 20),
                            std::uint32_t maxReadRetries = 2,
                            std::uint32_t maxWriteRetries = 2,
                            std::uint16_t threadPoolSize = 4) {
        return Initialize(maxIOSize);
    }

    bool Initialize(std::uint64_t totalSize, int threads = -1,
                    size_t chunkSize = 1ull << 31);

    virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer,
                                     std::uint64_t offset = UINT64_MAX) {
        size_t read = 0;
        if (offset != UINT64_MAX) m_pos = offset;
        if (m_pos + readSize > m_totalSize) {
            throw std::logic_error("Read overflow");
        }
        while (read < readSize) {
            auto info = locate(m_pos);
            size_t toRead = std::min(readSize - read, info.left);

            memcpy(buffer + read, info.ptr, toRead);

            read += toRead;
            m_pos += toRead;
        }
        return read;
    }
    virtual std::uint64_t ReadBinaryDirect(char* buffer, std::uint64_t readSize,
                                           std::uint64_t offset) {
        size_t read = 0;
        if (offset + readSize > m_totalSize) {
            throw std::logic_error("Read overflow");
        }
        while (read < readSize) {
            auto info = locate(offset + read);
            size_t toRead = std::min(readSize - read, info.left);

            std::memcpy(buffer + read, info.ptr, toRead);

            read += toRead;
        }
        return read;
    }

    virtual std::uint64_t WriteBinary(std::uint64_t writeSize,
                                      const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        size_t written = 0;
        if (offset != UINT64_MAX) m_pos = offset;
        if (m_pos + writeSize > m_totalSize) {
            throw std::logic_error("Write overflow");
        }
        while (written < writeSize) {
            auto info = locate(m_pos);
            size_t toWrite = std::min(writeSize - written, info.left);

            std::memcpy(info.ptr, buffer + written, toWrite);

            written += toWrite;
            m_pos += toWrite;
        }
        return written;
    }

    virtual std::uint64_t WriteBinaryDirect(const char* buffer,
                                            std::uint64_t writeSize,
                                            std::uint64_t offset) {
        size_t written = 0;
        if (offset + writeSize > m_totalSize) {
            throw std::logic_error("Write overflow");
        }
        while (written < writeSize) {
            auto info = locate(offset + written);
            size_t toWrite = std::min(writeSize - written, info.left);

            std::memcpy(info.ptr, buffer + written, toWrite);

            written += toWrite;
        }
        return written;
    }

    virtual std::uint64_t ReadString(std::uint64_t& readSize,
                                     std::unique_ptr<char[]>& buffer,
                                     char delim = '\n',
                                     std::uint64_t offset = UINT64_MAX) {
        return 0;
    }
    virtual std::uint64_t WriteString(const char* buffer,
                                      std::uint64_t offset = UINT64_MAX) {
        return 0;
    }

    void SeekP(std::uint64_t offset) {
        if (offset >= m_totalSize) {
            throw std::logic_error("offset >= m_totalSize");
        }
        m_pos = offset;
    }

    virtual std::uint64_t TellP() { return m_pos; }

    struct LocateInfo {
        char* ptr;
        size_t left;
        uint64_t off;

        LocateInfo(char* p = nullptr, size_t l = 0, uint64_t o = 0)
            : ptr(p), left(l), off(o) {}
    };

    virtual void ShutDown() {
        m_totalSize = m_chunkSize = m_pos = 0;
        m_chunks.clear();
    };

    std::vector<LocateInfo> report() const {
        std::vector<LocateInfo> info_vec;
        uint64_t off = 0;
        for (size_t i = 0; i < m_chunks.size(); i++) {
            auto size = chunk_size(i);
            info_vec.emplace_back(m_chunks[i].get(), size, off);
            off += size;
        }
        return info_vec;
    }

    inline size_t getTotalSize() const { return m_totalSize; }
    void Output(SimpleIOHelper& sio, uint64_t start_offset = 0,
                int num_threads = 3, size_t bs = 1024 * 1024);

   private:
    inline LocateInfo locate(uint64_t offset) const {
        size_t chunck_idx = offset / m_chunkSize;
        size_t chunk_off = offset % m_chunkSize;

        LocateInfo info;
        info.ptr = m_chunks[chunck_idx].get() + chunk_off;
        info.left = chunk_size(chunck_idx) - chunk_off;

        return info;
    }

    inline size_t chunk_size(size_t idx) const {
        if (idx == m_chunks.size() - 1) {
            return m_totalSize - idx * m_chunkSize;
        }
        return m_chunkSize;
    }

   private:
    uint64_t m_totalSize;
    uint64_t m_chunkSize;
    uint64_t m_pos;

    std::vector<std::shared_ptr<char[]>> m_chunks;
};

}  // namespace Helper
}  // namespace SPTAG

#endif  // _SPTAG_HELPER_DISKIO_H_
