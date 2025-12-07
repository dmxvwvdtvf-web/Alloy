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
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/main.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"
#include "Merger.h"
#include "tqdm.hpp"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::Helper;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SPMerge;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");

    std::map<std::string, std::map<std::string, std::string>> config_map;
    VectorValueType value_type;

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
    Options options;
    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            options.SetParameter(sectionKV.first.c_str(), KV.first.c_str(),
                                 KV.second.c_str());
        }
    }

    std::shared_ptr<IExtraSearcher> disk_index(nullptr);
    switch (value_type) {
#define DefineVectorValueType(Name, Type)                            \
    case VectorValueType::Name: {                                    \
        disk_index.reset(new ExtraFullGraphSearcher<Type>(options)); \
        break;                                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default:
            break;
    }
    size_t m_numNodes = disk_index->GetNumNodes();

    options.m_curMid = 0;
    disk_index->SetRecoveryMode(true);
    disk_index->LoadIndex(options);

    auto& mr = disk_index->GetMergerResult();
    LOG_INFO("#cluster-vectors: %llu, #head-vectors: %llu\n",
             mr.cluster_vector_count, mr.head_count);

    auto outdir = std::filesystem::path(
        config_map.at(SEC_REDUNDANCY_MERGER).at("savepath"));
    {
        auto fpath = outdir / "patch.txt";
        std::ofstream ofs(fpath.string());
        for (uint mid = 0; mid < m_numNodes; mid++) {
            ofs << mid << std::endl;
            for (const auto vid : mr.patch[mid]) {
                ofs << vid << " ";
            }
            ofs << std::endl;
        }
        LOG_INFO("✓ %s\n", fpath.c_str());
    }
    {
        auto t = tq::tqdm(mr.cluster_content);
        t.set_prefix("output cluster content");
        auto fpath = outdir / "cluster-content.txt";
        std::ofstream ofs(fpath.string());
        for (const auto& [cid, vid_list] : t) {
            ofs << cid << std::endl;
            for (auto vid : vid_list) {
                ofs << vid << " ";
            }
            ofs << std::endl;
        }
        LOG_INFO("✓ %s\n", fpath.c_str());
    }
    {
        auto fpath = outdir / "cluster-assignment.txt";
        std::ofstream ofs(fpath.string());
        for (uint mid = 0; mid < m_numNodes; mid++) {
            LOG_INFO("machine %u has %lu clusters\n", mid,
                     mr.cluster_assignment[mid].size());
            ofs << mid << std::endl;
            for (auto cid : mr.cluster_assignment[mid]) {
                ofs << cid << " ";
            }
            ofs << std::endl;
        }
        LOG_INFO("✓ %s\n", fpath.c_str());
    }
    // {
    //     auto li_path = outdir / "list-info.txt";
    //     std::ofstream list_info_ofs(li_path.string());

    //     for (uint mid = 0; mid < m_numNodes; mid++) {
    //         options.m_curMid = mid;
    //         disk_index->DeInit(true);
    //         disk_index->SetRecoveryMode(true);
    //         disk_index->LoadIndex(options);

    //         const auto& order = disk_index->GetPostingOrder();
    //         const auto& info_map = disk_index->GetListInfoMap();
    //         list_info_ofs << mid << std::endl;
    //         for (auto cid : order) {
    //             const auto info = info_map.at(cid);
    //             auto list_position = info.listOffset + info.pageOffset;
    //             list_info_ofs << list_position << " ";
    //         }
    //         list_info_ofs << std::endl;
    //     }
    //     LOG_INFO("list info saved to %s\n", li_path.c_str());
    // }

    return 0;
}