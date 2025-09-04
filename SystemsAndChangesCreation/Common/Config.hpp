#pragma once
#include <cinttypes>

namespace Config {
    extern const uint64_t NUM_BLOCK_MAP_RESERVE;
    extern const uint64_t NUM_LOCAL_BLOCK_RESERVE;
    extern const uint64_t NUM_FS_FILES_RESERVE;
    extern const int32_t DEFAULT_INITIAL_BLOCK_SIZE;
    extern const int32_t FILES_SN_RESERVE_SIZE;

    namespace VOLUME_HEADER
    {
        extern const uint32_t HEADER_EXPECTED_SIZE;
    }

    namespace FS_HEADER
    {
        extern const uint32_t HEADER_EXPECTED_SIZE;
        extern const uint32_t INPUT_FILE_INDEX;
        extern const uint32_t NUM_FILES_INDEX;
        extern const uint32_t PHYSICAL_NUM_BLOCKS_INDEX;
        extern const uint32_t LOGICAL_NUM_BLOCKS_INDEX;
        extern const uint32_t USERNAME_INDEX;
        extern const uint32_t HOSTNAME_INDEX;
        extern const uint32_t WIN_VERSION_INDEX;
        extern const uint32_t SNAPSHOT_TIME_INDEX;
        extern const uint32_t LOGICAL_SIZE_INDEX;
        extern const uint32_t GEN_LEVEL_INDEX;
    }

    namespace BLOCK_LINE
    {
        extern const uint32_t LOCAL_SN_INDEX;
        extern const uint32_t FP_INDEX;
        extern const uint32_t NUM_FILES_INDEX;
        extern const uint32_t MIN_NUM_ELEMENT_IN_BLOCK_LINE;
    }

    namespace FILE_LINE
    {
        extern const uint32_t SN_INDEX;
        extern const uint32_t ID_INDEX;
        extern const uint32_t PARENT_SN_INDEX;
        extern const uint32_t NUM_BLOCKS_INDEX;
    }
};

