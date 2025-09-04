#pragma once

#include <cinttypes>

namespace Config
{
    extern uint64_t BLOCK_RESERVE;
    extern const char* const TRACE_EOF;
    namespace LOCAL_FILE_HEADER
    {
        extern uint32_t NUM_OF_CONST_FILES_HEADERS;
        extern uint32_t FILE_LOGICAL_SIZE_INDEX;
    }

    namespace TRACE_HEADER
    {
        extern uint64_t LINES_IN_TRACE_HEADER;
        extern uint32_t USERNAME_INDEX;
        extern uint32_t HOSTNAME_INDEX;
        extern uint32_t TIMESTAMP_INDEX;
        extern uint32_t WINDOWS_VERSION_INDEX;
    }
}