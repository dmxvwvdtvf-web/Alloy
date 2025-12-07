// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SERVER_SERVICESTTINGS_H_
#define _SPTAG_SERVER_SERVICESTTINGS_H_

#include <string>

#include "../Core/Common.h"

namespace SPTAG {
namespace Service {

struct ServiceSettings {
    ServiceSettings();

    std::string m_vectorSeparator;

    std::string m_listenAddr;

    std::string m_listenPort;

    SizeType m_defaultMaxResultNumber;

    SizeType m_threadNum;

    SizeType m_socketThreadNum;
};

}  // namespace Service
}  // namespace SPTAG

#endif  // _SPTAG_SERVER_SERVICESTTINGS_H_
