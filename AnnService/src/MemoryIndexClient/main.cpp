// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/SSDServing/main.h"

#include <iostream>

#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/MemoryIndexClient.h"
#include "inc/SSDServing/Rpc.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SSDServing::SSDIndex;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");

    std::map<std::string, std::map<std::string, std::string>> config_map;
    VectorValueType value_type;
    bool search_pruning = false;
    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
        config_map[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
        config_map[SEC_BUILD_SSD_INDEX] =
            iniReader.GetParameters(SEC_BUILD_SSD_INDEX);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);
        config_map[SEC_SEPERATED_INDEX] =
            iniReader.GetParameters(SEC_SEPERATED_INDEX);

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
        bool buildSSD =
            iniReader.GetParameter(SEC_BUILD_SSD_INDEX, "isExecute", false);
        search_pruning = iniReader.GetParameter(
            SEC_BUILD_SSD_INDEX, "SearchPruning", search_pruning);

        for (auto& KV : iniReader.GetParameters(SEC_SEARCH_SSD_INDEX)) {
            std::string param = KV.first, value = KV.second;
            if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(
                                param.c_str(), "BuildSsdIndex"))
                continue;
            if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                                 "isExecute"))
                continue;
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "PostingPageLimit"))
                param = "SearchPostingPageLimit";
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "InternalResultNum"))
                param = "SearchInternalResultNum";
            config_map[SEC_BUILD_SSD_INDEX][param] = value;
        }
        config_map[SEC_BUILD_SSD_INDEX].at("isexecute") = "false";
    }

    std::shared_ptr<VectorIndex> index =
        VectorIndex::CreateInstance(IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        throw std::runtime_error("cannot create index!");
    }

    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }

    index->LoadHeadIndex();
    if (search_pruning) {
        LOG_INFO("client using optimization: frequency pruning\n");
        index->LoadPatchMeta();
    }

#define DefineVectorValueType(Name, Type)                                  \
    if (value_type == VectorValueType::Name) {                             \
        SSDIndex::Memory_Index_Search((SPANN::Index<Type>*)(index.get())); \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    return 0;
}
