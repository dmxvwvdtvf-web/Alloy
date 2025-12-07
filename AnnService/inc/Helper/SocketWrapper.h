#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <mutex>
#include <stdexcept>
#include <thread>

#include "inc/Core/Common.h"

namespace SPTAG {
namespace Helper {

inline void f_read_wrapper(int fd, void* buf, size_t size) {
    size_t readed = 0;
    while (readed < size) {
        auto sub_ptr = (char*)buf + readed;
        auto sub_size = size - readed;
        auto ret = read(fd, sub_ptr, sub_size);
        if (ret == -1) {
            LOG_ERROR("read with size=%llu failed: %s\n", sub_size,
                      strerror(errno));
            throw std::runtime_error("");
        }
        readed += ret;
    }
};

inline void f_write_wrapper(int sockfd, const void* buf, size_t size) {
    size_t written = 0;
    while (written < size) {
        auto sub_ptr = (char*)buf + written;
        auto sub_size = size - written;
        auto ret = write(sockfd, sub_ptr, sub_size);
        if (ret == -1) {
            LOG_ERROR("write with size=%llu failed: %s\n", sub_size,
                      strerror(errno));
            throw std::runtime_error("");
        }
        written += ret;
    }
};

inline void f_sendfile64_wrapper(int sockfd, int ifd, size_t size,
                                 __off64_t* off = nullptr) {
    size_t sent = 0;
    while (sent < size) {
        auto sub_size = size - sent;
        auto ret = sendfile64(sockfd, ifd, off, sub_size);
        if (ret == -1) {
            LOG_ERROR("sendfile64 with size=%llu failed: %s\n", sub_size,
                      strerror(errno));
            throw std::runtime_error("");
        }
        sent += ret;
    }
};

struct SocketManager {
    std::mutex socket_list_lock;
    std::vector<int> opened_socket;

    int listen_socket = -1;

    ~SocketManager() { close_all_socket(); }

    int create_listen_socket(int recv_port) {
        if (listen_socket != -1) {
            LOG_WARN("listen socket already set, overwriting it\n");
        }

        listen_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_socket < 0) {
            LOG_ERROR("Socket creation failed\n");
            return false;
        }
        std::lock_guard guard(socket_list_lock);
        opened_socket.push_back(listen_socket);
        // allow fast reuse of port
        const int on = 1;
        if (int ret = setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on,
                                 sizeof(on));
            ret == -1) {
            LOG_ERROR("setsockopt with SO_REUSEADDR on failed: %s\n",
                      strerror(errno));
            throw std::runtime_error("");
        }

        struct sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_addr.s_addr = INADDR_ANY;
        serverAddr.sin_port = htons(recv_port);
        if (bind(listen_socket, (struct sockaddr*)&serverAddr,
                 sizeof(serverAddr)) < 0) {
            LOG_ERROR("Bind failed: %s\n", strerror(errno));
            throw std::runtime_error("");
        }
        if (listen(listen_socket, SOMAXCONN) < 0) {
            throw std::runtime_error("Listen failed\n");
        }
        return listen_socket;
    }

    int f_accept_wrapper() {
        if (listen_socket == -1) {
            throw std::logic_error(
                "no listen socket now, should create one first!!");
        }
        struct sockaddr_in addr;
        socklen_t len = sizeof(addr);
        int socket = accept(listen_socket, (struct sockaddr*)&addr, &len);
        if (socket < 0) {
            close_all_socket();
            throw std::runtime_error("Accept failed\n");
        }
        std::lock_guard guard(socket_list_lock);
        opened_socket.push_back(socket);

        return socket;
    };

    int f_connect_wrapper(std::string ip, int port, int retry = 5) {
        struct sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &serverAddr.sin_addr);

        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            close_all_socket();
            throw std::runtime_error("Socket creation failed\n");
        }

        {
            std::lock_guard guard(socket_list_lock);
            opened_socket.push_back(sockfd);
        }

        auto sleep_time = std::chrono::milliseconds(100);
        while (retry > 0 and connect(sockfd, (struct sockaddr*)&serverAddr,
                                     sizeof(serverAddr)) < 0) {
            LOG_ERROR("Connection failed with error: %s\n", strerror(errno));
            if (errno == ECONNREFUSED) {
                LOG_INFO("Sleep %llu ms then reconnect\n", sleep_time.count());
                std::this_thread::sleep_for(sleep_time);
            }
            retry--;
            sleep_time *= 2;
        }
        if (retry <= 0) {
            close_all_socket();
            throw std::runtime_error("Recovery given up...\n");
        }
        return sockfd;
    };

    void close_all_socket() {
        std::lock_guard guard(socket_list_lock);
        for (auto socket : opened_socket) close(socket);
        opened_socket.clear();
    };
};

}  // namespace Helper
}  // namespace SPTAG