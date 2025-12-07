#include <arpa/inet.h>

#include <stdexcept>
#include <string>

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/SSDServing/main.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SSDServing;

int main(int argc, char* argv[]) {
    if (argc < 7) {
        printf(
            "Usage: %s [config file] [fail mid] [fail ip] [fail port] "
            "[secondary ip] [secondary port]",
            argv[0]);
        return -1;
    }

    std::map<std::string, std::map<std::string, std::string>> config_map;

    VectorValueType value_type = VectorValueType::Undefined;
    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);
        config_map[SEC_SEPERATED_INDEX] =
            iniReader.GetParameters(SEC_SEPERATED_INDEX);

        config_map[SEC_REDUNDANCY_MERGER]["loadmergerresult"] = "true";
        config_map[SEC_REDUNDANCY_MERGER]["recoverymode"] = "true";

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);

        int num_split = -1;
        num_split = iniReader.GetParameter(SEC_REDUNDANCY_MERGER, "NumSplit",
                                           num_split);
        if (num_split <= 1) {
            LOG_ERROR("%s is used for split>=2, but NumSplit set to %d\n",
                      argv[0], num_split);
            throw std::logic_error("");
        }
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

    index->LoadSSDIndex();

    // NOTE: should receive from rpc
    SPMerge::MachineID mid = std::stoi(argv[2]);
    std::string fail_ip(argv[3]);
    int fail_port = std::stoi(argv[4]);
    std::string secondary_ip(argv[5]);
    int secondary_port = std::stoi(argv[6]);
    clear_page_cache();

    index->PrimaryHelpRecovery(mid, fail_ip, fail_port, secondary_ip,
                               secondary_port);

    return 0;
}
