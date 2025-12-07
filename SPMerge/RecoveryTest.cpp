#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/DefinitionList.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/main.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"
#include "Merger.h"
#include "tqdm.hpp"

using namespace SPTAG;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SPMerge;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");

    std::map<std::string, std::map<std::string, std::string>> config_map;
    VectorValueType value_type;
    MachineID N_MACHINE;
    std::string recovery_dir;
    MachineID cur_mid = InvalidMID;
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
        config_map[SEC_REDUNDANCY_MERGER].at("loadmergerresult") = "true";

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
        bool buildSSD =
            iniReader.GetParameter(SEC_BUILD_SSD_INDEX, "isExecute", false);
        N_MACHINE = iniReader.GetParameter(SEC_REDUNDANCY_MERGER, "NumMachine",
                                           N_MACHINE);
        recovery_dir = iniReader.GetParameter(SEC_REDUNDANCY_MERGER,
                                              "RecoveryPath", recovery_dir);
        cur_mid =
            iniReader.GetParameter(SEC_REDUNDANCY_MERGER, "MachineID", cur_mid);

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
    }

    std::shared_ptr<VectorIndex> index =
        VectorIndex::CreateInstance(IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        LOG_ERROR("Cannot create Index with ValueType %s!\n",
                  config_map[SEC_BASE]["ValueType"].c_str());
        throw std::runtime_error("");
    }

    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }

    auto f_recovery_from = [&](MachineID healthy_mid) {
        auto subdir = "from-machine" + std::to_string(healthy_mid);
        auto dir = (std::filesystem::path(recovery_dir) / subdir).string();
        index->SetParameter("RecoveryPath", dir, SEC_REDUNDANCY_MERGER);
        for (MachineID fail_mid = 0; fail_mid < N_MACHINE; fail_mid++) {
            if (healthy_mid == fail_mid) continue;
            index->LoadAndRecovery(healthy_mid, fail_mid);
            index->Deinit();
        }
    };
    if (cur_mid == InvalidMID) {
        for (MachineID healthy_mid = 0; healthy_mid < N_MACHINE; healthy_mid++)
            f_recovery_from(healthy_mid);

    } else {
        f_recovery_from(cur_mid);
    }
    return 0;
}