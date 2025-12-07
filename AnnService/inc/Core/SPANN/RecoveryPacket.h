#pragma once

#include <cstdint>

namespace SPTAG {
namespace SPANN {

enum class RecoveryPacketType : uint8_t {
    UNDEFINED,
    HelpInfo,
    PatchData,
    Translate,
    IndexData,
    IAmReady,
};

#pragma pack(push, 1)
struct PacketHeader {
    PacketHeader() = default;
    ~PacketHeader() = default;

    RecoveryPacketType type = RecoveryPacketType::UNDEFINED;

    // default: data slice idx (in case packet size is limited by 2GB)
    // HelpInfo1: healthy machine ID
    // HelpInfo2: #additional resucers
    // HelpInfo3: healthy machine ID
    uint16_t idx = 0;

    // default: payload size of idx-th packet
    // HelpInfo1: healthy patch size
    // HelpInfo2: buffer size during vectorset translation
    uint64_t sub_size = 0;

    // HelpInfo1: healthy index size
    // IndexMeta: size of meta
    // PatchMeta: size of clusters
    // PatchData: size of full patch
    // IndexData: cluster id
    // Translate: number of head vec
    // HelpInfo2: #vectors to send (total)
    // HelpInfo3: #vectors to send (local)
    uint64_t n = 0;
};
#pragma pack(pop)

}  // namespace SPANN
}  // namespace SPTAG