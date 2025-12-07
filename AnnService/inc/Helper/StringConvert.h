// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_STRINGCONVERTHELPER_H_
#define _SPTAG_HELPER_STRINGCONVERTHELPER_H_

#include <cctype>
#include <cerrno>
#include <cstring>
#include <limits>
#include <sstream>
#include <string>

#include "CommonHelper.h"
#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"
#include "inc/SSDServing/main.h"

namespace SPTAG {
namespace Helper {
namespace Convert {

template <typename DataType>
inline bool ConvertStringTo(const char* p_str, DataType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

    std::istringstream sstream;
    sstream.str(p_str);
    if (p_str >> p_value) {
        return true;
    }

    return false;
}

template <typename DataType>
inline std::string ConvertToString(const DataType& p_value) {
    return std::to_string(p_value);
}

template <>
inline bool ConvertStringTo<SPMerge::RecoveryMethod>(
    const char* p_str, SPMerge::RecoveryMethod& p_value) {
    if (StrUtils::StrEqualIgnoreCase(p_str, "PureMemory")) {
        p_value = SPMerge::RecoveryMethod::PureMemory;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "GreedyLoad")) {
        p_value = SPMerge::RecoveryMethod::GreedyLoad;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "StupidLoad")) {
        p_value = SPMerge::RecoveryMethod::StupidLoad;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "ScanGather")) {
        p_value = SPMerge::RecoveryMethod::ScanGather;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "ScanWrite")) {
        p_value = SPMerge::RecoveryMethod::ScanWrite;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "ConcurrentRecovery")) {
        p_value = SPMerge::RecoveryMethod::ConcuRecov;
    } else {
        return false;
    }
    return true;
}

template <>
inline std::string ConvertToString<SPMerge::RecoveryMethod>(
    const SPMerge::RecoveryMethod& p_value) {
    switch (p_value) {
        case SPMerge::RecoveryMethod::PureMemory:
            return "PureMemory";
        case SPMerge::RecoveryMethod::GreedyLoad:
            return "GreedyLoad";
        case SPMerge::RecoveryMethod::StupidLoad:
            return "StupidLoad";
        case SPMerge::RecoveryMethod::ScanGather:
            return "ScanGather";
        case SPMerge::RecoveryMethod::ScanWrite:
            return "ScanWrite";
        case SPMerge::RecoveryMethod::ConcuRecov:
            return "ConcurrentRecovery";
        default:
            return "Undefined";
    }
}

template <>
inline bool ConvertStringTo<SSDServing::RPC_Method>(
    const char* p_str, SSDServing::RPC_Method& p_value) {
    if (StrUtils::StrEqualIgnoreCase(p_str, "Sync")) {
        p_value = SSDServing::RPC_Method::Sync;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "Callback")) {
        p_value = SSDServing::RPC_Method::Callback;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "ASync")) {
        p_value = SSDServing::RPC_Method::ASync;
    } else {
        return false;
    }
    return true;
}

template <>
inline std::string ConvertToString<SSDServing::RPC_Method>(
    const SSDServing::RPC_Method& p_value) {
    switch (p_value) {
        case SSDServing::RPC_Method::Sync:
            return "Sync";
        case SSDServing::RPC_Method::Callback:
            return "Callback";
        case SSDServing::RPC_Method::ASync:
            return "ASync";
        default:
            return "Undefined";
    }
}

template <>
inline bool ConvertStringTo<SSDServing::RPC_Way>(const char* p_str,
                                                 SSDServing::RPC_Way& p_value) {
    if (StrUtils::StrEqualIgnoreCase(p_str, "Replication")) {
        p_value = SSDServing::RPC_Way::Replication;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "LoadBalance")) {
        p_value = SSDServing::RPC_Way::LoadBalance;
    } else if (StrUtils::StrEqualIgnoreCase(p_str, "SplitQuery")) {
        p_value = SSDServing::RPC_Way::SplitQuery;
    } else {
        return false;
    }
    return true;
}

template <>
inline std::string ConvertToString<SSDServing::RPC_Way>(
    const SSDServing::RPC_Way& p_value) {
    switch (p_value) {
        case SSDServing::RPC_Way::Replication:
            return "Replication";
        case SSDServing::RPC_Way::LoadBalance:
            return "LoadBalance";
        case SSDServing::RPC_Way::SplitQuery:
            return "SplitQuery";
        default:
            return "Undefined";
    }
}

template <>
inline bool ConvertStringTo<std::vector<std::string>>(
    const char* p_str, std::vector<std::string>& p_value) {
    std::string s;
    std::istringstream iss(p_str);
    while (std::getline(iss, s, ',')) {
        p_value.push_back(s);
    }
    return true;
}

template <>
inline std::string ConvertToString<std::vector<std::string>>(
    const std::vector<std::string>& p_value) {
    std::string s;
    for (const auto& subs : p_value) {
        s += subs + ',';
    }
    if (not s.empty()) s.pop_back();
    return s;
}

template <>
inline bool ConvertStringTo<std::vector<int>>(const char* p_str,
                                              std::vector<int>& p_value) {
    std::string s;
    std::istringstream iss(p_str);
    while (std::getline(iss, s, ',')) {
        int v = std::stoi(s.c_str());
        p_value.push_back(v);
    }
    return true;
}

template <>
inline std::string ConvertToString<std::vector<int>>(
    const std::vector<int>& p_value) {
    std::string s;
    for (const auto& v : p_value) {
        s += std::to_string(v) + ',';
    }
    if (not s.empty()) s.pop_back();
    return s;
}

// Specialization of ConvertStringTo<>().

template <typename DataType>
inline bool ConvertStringToSignedInt(const char* p_str, DataType& p_value) {
    static_assert(
        std::is_integral<DataType>::value && std::is_signed<DataType>::value,
        "type check");

    if (nullptr == p_str) {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    auto val = std::strtoll(p_str, &end, 10);
    if (errno == ERANGE || end == p_str || *end != '\0') {
        return false;
    }

    if (val < (std::numeric_limits<DataType>::min)() ||
        val > (std::numeric_limits<DataType>::max)()) {
        return false;
    }

    p_value = static_cast<DataType>(val);
    return true;
}

template <typename DataType>
inline bool ConvertStringToUnsignedInt(const char* p_str, DataType& p_value) {
    static_assert(
        std::is_integral<DataType>::value && std::is_unsigned<DataType>::value,
        "type check");

    if (nullptr == p_str) {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    auto val = std::strtoull(p_str, &end, 10);
    if (errno == ERANGE || end == p_str || *end != '\0') {
        return false;
    }

    if (val > (std::numeric_limits<DataType>::max)()) {
        return false;
    }

    p_value = static_cast<DataType>(val);
    return true;
}

template <>
inline bool ConvertStringTo<std::string>(const char* p_str,
                                         std::string& p_value) {
    if (nullptr == p_str) {
        return false;
    }

    p_value = p_str;
    return true;
}

template <>
inline bool ConvertStringTo<float>(const char* p_str, float& p_value) {
    if (nullptr == p_str) {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    p_value = std::strtof(p_str, &end);
    return (errno != ERANGE && end != p_str && *end == '\0');
}

template <>
inline bool ConvertStringTo<double>(const char* p_str, double& p_value) {
    if (nullptr == p_str) {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    p_value = std::strtod(p_str, &end);
    return (errno != ERANGE && end != p_str && *end == '\0');
}

template <>
inline bool ConvertStringTo<std::int8_t>(const char* p_str,
                                         std::int8_t& p_value) {
    return ConvertStringToSignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::int16_t>(const char* p_str,
                                          std::int16_t& p_value) {
    return ConvertStringToSignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::int32_t>(const char* p_str,
                                          std::int32_t& p_value) {
    return ConvertStringToSignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::int64_t>(const char* p_str,
                                          std::int64_t& p_value) {
    return ConvertStringToSignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::uint8_t>(const char* p_str,
                                          std::uint8_t& p_value) {
    return ConvertStringToUnsignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::uint16_t>(const char* p_str,
                                           std::uint16_t& p_value) {
    return ConvertStringToUnsignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::uint32_t>(const char* p_str,
                                           std::uint32_t& p_value) {
    return ConvertStringToUnsignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<std::uint64_t>(const char* p_str,
                                           std::uint64_t& p_value) {
    return ConvertStringToUnsignedInt(p_str, p_value);
}

template <>
inline bool ConvertStringTo<bool>(const char* p_str, bool& p_value) {
    if (StrUtils::StrEqualIgnoreCase(p_str, "true")) {
        p_value = true;

    } else if (StrUtils::StrEqualIgnoreCase(p_str, "false")) {
        p_value = false;
    } else {
        return false;
    }

    return true;
}

template <>
inline bool ConvertStringTo<IndexAlgoType>(const char* p_str,
                                           IndexAlgoType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineIndexAlgo(Name)                              \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = IndexAlgoType::Name;                     \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineIndexAlgo

    return false;
}

template <>
inline bool ConvertStringTo<VectorFileType>(const char* p_str,
                                            VectorFileType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineVectorFileType(Name)                         \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = VectorFileType::Name;                    \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineVectorFileType

    return false;
}

template <>
inline bool ConvertStringTo<TruthFileType>(const char* p_str,
                                           TruthFileType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineTruthFileType(Name)                          \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = TruthFileType::Name;                     \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineTruthFileType

    return false;
}

template <>
inline bool ConvertStringTo<DistCalcMethod>(const char* p_str,
                                            DistCalcMethod& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineDistCalcMethod(Name)                         \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = DistCalcMethod::Name;                    \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineDistCalcMethod

    return false;
}

template <>
inline bool ConvertStringTo<VectorValueType>(const char* p_str,
                                             VectorValueType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineVectorValueType(Name, Type)                  \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = VectorValueType::Name;                   \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    return false;
}

template <>
inline bool ConvertStringTo<QuantizerType>(const char* p_str,
                                           QuantizerType& p_value) {
    if (nullptr == p_str) {
        return false;
    }

#define DefineQuantizerType(Name, Type)                    \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) { \
        p_value = QuantizerType::Name;                     \
        return true;                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineQuantizerType

    return false;
}

// Specialization of ConvertToString<>().

template <>
inline std::string ConvertToString<std::string>(const std::string& p_value) {
    return p_value;
}

template <>
inline std::string ConvertToString<bool>(const bool& p_value) {
    return p_value ? "true" : "false";
}

template <>
inline std::string ConvertToString<IndexAlgoType>(
    const IndexAlgoType& p_value) {
    switch (p_value) {
#define DefineIndexAlgo(Name) \
    case IndexAlgoType::Name: \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineIndexAlgo

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<QuantizerType>(
    const QuantizerType& p_value) {
    switch (p_value) {
#define DefineQuantizerType(Name, foo) \
    case QuantizerType::Name:          \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineQuantizerType

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<DistCalcMethod>(
    const DistCalcMethod& p_value) {
    switch (p_value) {
#define DefineDistCalcMethod(Name) \
    case DistCalcMethod::Name:     \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineDistCalcMethod

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<VectorValueType>(
    const VectorValueType& p_value) {
    switch (p_value) {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name:           \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<VectorFileType>(
    const VectorFileType& p_value) {
    switch (p_value) {
#define DefineVectorFileType(Name) \
    case VectorFileType::Name:     \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineVectorFileType

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<TruthFileType>(
    const TruthFileType& p_value) {
    switch (p_value) {
#define DefineTruthFileType(Name) \
    case TruthFileType::Name:     \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineTruthFileType

        default:
            break;
    }

    return "Undefined";
}

template <>
inline std::string ConvertToString<ErrorCode>(const ErrorCode& p_value) {
    switch (p_value) {
#define DefineErrorCode(Name, Code) \
    case ErrorCode::Name:           \
        return #Name;

#include "inc/Core/DefinitionList.h"
#undef DefineErrorCode

        default:
            break;
    }

    return "Undefined";
}

}  // namespace Convert
}  // namespace Helper
}  // namespace SPTAG

#endif  // _SPTAG_HELPER_STRINGCONVERTHELPER_H_
