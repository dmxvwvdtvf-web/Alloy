// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <map>
#include <string>

#include "inc/Core/Common.h"

namespace SPTAG {
namespace SSDServing {

enum class RPC_Method {
    Sync,
    Callback,
    ASync,
};

enum class RPC_Way {
    Replication,  // send 1 query directly to 3 server
    LoadBalance,  // send 1 query to qid % #server
    SplitQuery,   // split 1 query to 3 sub-queries and send
};

int BootProgram(
    bool forANNIndexTestTool,
    std::map<std::string, std::map<std::string, std::string>>* config_map,
    const char* configurationPath = nullptr,
    SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Undefined,
    SPTAG::DistCalcMethod distCalcMethod = SPTAG::DistCalcMethod::Undefined,
    const char* dataFilePath = nullptr, const char* indexFilePath = nullptr);

int MemoryIndeClient_BootProgram(
    bool forANNIndexTestTool,
    std::map<std::string, std::map<std::string, std::string>>* config_map,
    const char* configurationPath = nullptr,
    SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Undefined,
    SPTAG::DistCalcMethod distCalcMethod = SPTAG::DistCalcMethod::Undefined,
    const char* dataFilePath = nullptr, const char* indexFilePath = nullptr);

int SSDIndexServer_BootProgram(
    bool forANNIndexTestTool,
    std::map<std::string, std::map<std::string, std::string>>* config_map,
    const char* configurationPath = nullptr,
    SPTAG::VectorValueType valueType = SPTAG::VectorValueType::Undefined,
    SPTAG::DistCalcMethod distCalcMethod = SPTAG::DistCalcMethod::Undefined,
    const char* dataFilePath = nullptr, const char* indexFilePath = nullptr);

const std::string SEC_BASE = "Base";
const std::string SEC_SELECT_HEAD = "SelectHead";
const std::string SEC_BUILD_HEAD = "BuildHead";
const std::string SEC_BUILD_SSD_INDEX = "BuildSSDIndex";
const std::string SEC_SEARCH_SSD_INDEX = "SearchSSDIndex";
const std::string SEC_REDUNDANCY_MERGER = "RedundancyMerger";
const std::string SEC_SEPERATED_INDEX = "SeperatedIndex";
}  // namespace SSDServing
}  // namespace SPTAG
