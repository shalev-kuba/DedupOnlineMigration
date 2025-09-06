#include "TraceFile.hpp"

#include "../Common/Utility.hpp"
#include "Config.hpp"

#include <stdexcept>
#include <vector>

TraceFile::TraceFile(const std::string &file_path) :
_input_stream(Utility::get_input_file_stream(file_path)),
_fs_headers(consume_headers(_input_stream, file_path)){
    _blocks.reserve(Config::BLOCK_RESERVE);
    while(consume_file());

    static constexpr uint32_t SINGLE_FILE_TRACE = 1; //each trace file is outputted as a single file
    _fs_headers.num_files = SINGLE_FILE_TRACE;
}

FSMetaData TraceFile::consume_headers(std::ifstream& input_stream, const std::string& file_path) {
    std::vector<std::string> headers_lines;
    headers_lines.reserve(Config::TRACE_HEADER::LINES_IN_TRACE_HEADER);

    for(int i=0; i < Config::TRACE_HEADER::LINES_IN_TRACE_HEADER; ++i)
    {
        headers_lines.emplace_back(Utility::get_trace_header_line(input_stream));
    }

    //line separator
    Utility::get_line(input_stream);

    static constexpr uint64_t INITIAL_LOGICAL_SIZE = 0;
    static constexpr uint64_t INITIAL_NUM_LOGICAL_BLOCKS = 0;
    static constexpr uint64_t INITIAL_NUM_PHYSICAL_BLOCKS = 0;
    static constexpr uint64_t INITIAL_NUM_FILES = 0;
    static constexpr uint64_t INITIAL_NUM_LOGICAL_FILES = 0;
    static constexpr uint64_t HEX_BASE = 16;

    return {{},
            Utility::get_file_name(file_path),
            static_cast<uint64_t>(std::stoull(headers_lines[Config::TRACE_HEADER::USERNAME_INDEX], nullptr,HEX_BASE)),
            static_cast<uint64_t>(std::stoull(headers_lines[Config::TRACE_HEADER::HOSTNAME_INDEX], nullptr, HEX_BASE)),
            Utility::get_date(static_cast<uint64_t>(std::stoull(headers_lines[Config::TRACE_HEADER::TIMESTAMP_INDEX]))),
            headers_lines[Config::TRACE_HEADER::WINDOWS_VERSION_INDEX],
            INITIAL_LOGICAL_SIZE,
            INITIAL_NUM_LOGICAL_BLOCKS,
            INITIAL_NUM_PHYSICAL_BLOCKS,
            INITIAL_NUM_LOGICAL_FILES,
            INITIAL_NUM_FILES,
            0};
}

bool TraceFile::consume_file()
{
    std::vector<std::string> file_headers_line;
    file_headers_line.reserve(Config::LOCAL_FILE_HEADER::NUM_OF_CONST_FILES_HEADERS);

    for(int i=0; i< Config::LOCAL_FILE_HEADER::NUM_OF_CONST_FILES_HEADERS; ++i)
    {
        const std::string line = Utility::get_line(_input_stream);

        if(is_trace_eof(line))
        {
            if(i!=0)
                throw std::runtime_error("got trace eof (\"LOGCOMPLETE\") in the middle of file header");

            return false; // Finished trace file
        }

        file_headers_line.emplace_back(line);
    }

    _fs_headers.logical_size += static_cast<uint64_t>(std::stoull(
            file_headers_line[Config::LOCAL_FILE_HEADER::FILE_LOGICAL_SIZE_INDEX]));

    _fs_headers.num_logical_files ++;

    consume_file_blocks();

    return true;
}

void TraceFile::update_with_block(const std::string& block_line)
{
    //consume block
    static constexpr char BLOCK_DELIMITER = ':';
    if(block_line.find(BLOCK_DELIMITER) != std::string::npos)
    {
        //split to block size and hash
        const std::string block_hash = block_line.substr(0, block_line.find(BLOCK_DELIMITER));
        int32_t block_size = std::stoi(block_line.substr(block_line.find(BLOCK_DELIMITER)+1));
        if(_blocks.find(block_hash) == _blocks.cend())
        {
            // new block
            _blocks[block_hash] = {_fs_headers.num_physical_blocks, block_size};
            _fs_headers.num_physical_blocks++;
        }

        _fs_headers.num_logical_blocks ++;
    }
}

void TraceFile::consume_file_blocks(){
    std::string line = Utility::get_line(_input_stream);
    while(!line.empty())
    {
        if(is_block_line(line))
        {
            update_with_block(line);
        }

        line = Utility::get_line(_input_stream);
    }

    if(_fs_headers.num_physical_blocks !=  _blocks.size())
    {
        throw std::runtime_error("_fs_headers.num_physical_blocks != _blocks.size()");
    }
}

void TraceFile::print_fs_header(std::ofstream& out_file) const{
    out_file<<"#Output type: block-level"<<std::endl;
    out_file<<"#Input File: "<<_fs_headers.input_file<<std::endl;
    out_file<<"#Target depth: 0"<<std::endl;
    out_file<<"#Num files: "<<_fs_headers.num_files<<std::endl;
    out_file<<"#Num logical files: "<<_fs_headers.num_logical_files<<std::endl;
    out_file<<"#Num directories: 0"<<std::endl;
    out_file<<"#Num Physical Blocks: "<<_fs_headers.num_physical_blocks<<std::endl;
    out_file<<"#Num Logical Blocks: "<<_fs_headers.num_logical_blocks<<std::endl;
    out_file<<"#User: "<<_fs_headers.username<<std::endl;
    out_file<<"#Host name: "<<_fs_headers.host_name<<std::endl;
    out_file<<"#Windows Version: "<<_fs_headers.windows_version<<std::endl;
    out_file << "#Snapshot Date: " << _fs_headers.snapshot_date << std::endl;
    out_file<<"#Logical File System Size: "<<_fs_headers.logical_size<<std::endl;
    out_file<<std::endl;
}

void TraceFile::print_blocks(std::ofstream& out_file){
    static constexpr uint64_t LOCAL_FILE_SN_IS_ZERO = 0;

    for(const auto& block : _blocks)
    {
        static const uint64_t ONLY_ONE_FILE_SHARED = 1;
        out_file<<"B, "<<block.second.local_sn<<", "<<block.first<<", "<<ONLY_ONE_FILE_SHARED<<", "<<LOCAL_FILE_SN_IS_ZERO<<std::endl;
    }
}

void TraceFile::print_fs_file(std::ofstream& out_file){
    static constexpr uint64_t LOCAL_SN_IS_ZERO = 0;
    static constexpr uint64_t PARENT_LOCAL_SN_IS_ZERO = 0;
    out_file<<"F, "<<LOCAL_SN_IS_ZERO<<", "<<_fs_headers.input_file<<", "<<PARENT_LOCAL_SN_IS_ZERO<<", "
    <<_fs_headers.num_physical_blocks<<", ";

    for(const auto& block : _blocks)
    {
        out_file<<block.second.local_sn<<", "<<block.second.size<<", ";
    }

    out_file<<std::endl;
}

void TraceFile::print_to_file(const std::string &out_dir) {

    static constexpr char DIR_SEPARATOR = '/';

    std::string user_dir = out_dir;
    if(out_dir[out_dir.size()-1] != DIR_SEPARATOR)
    {
        user_dir+=DIR_SEPARATOR;
    }

    user_dir += std::to_string(_fs_headers.username);

    Utility::createDir(user_dir);

    std::string full_path = user_dir + DIR_SEPARATOR + get_file_name();

    std::ofstream outfile(full_path);
    if(!outfile.is_open())
    {
        throw std::runtime_error("Could not open out file " + out_dir);
    }

    print_fs_header(outfile);
    print_blocks(outfile);
    print_fs_file(outfile);
}

std::string TraceFile::get_file_name() const{
    return "U" + std::to_string(_fs_headers.username) + "_H" + std::to_string(_fs_headers.host_name) + "_IF" +
    _fs_headers.input_file + "_TS" +  _fs_headers.snapshot_date;
}

bool TraceFile::is_trace_eof(const std::string &line) {
    return line.find(Config::TRACE_EOF)!= std::string::npos;
}

bool TraceFile::is_block_line(const std::string &line) {
    return !(line.find("SV") == 0 || line.find('A') == 0|| line.find('V') == 0);
}

TraceFile::~TraceFile() {
    try {
        _input_stream.close();
    }
    catch(...)
    {
    }
}
