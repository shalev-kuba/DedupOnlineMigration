#include "FSFile.hpp"
#include "../Common/Config.hpp"
#include "../Common/Utility.hpp"

#include <iostream>
#include <utility>
#include <sstream>

FSFile::FSFile(const std::string &file_path,
               std::shared_ptr<std::unordered_map<std::string, GlobalBlockInfo>> &global_fp_to_info,
               uint64_t global_file_sn,
               bool filter_blocks) :
               _filter_blocks(filter_blocks),
               _global_file_sn(global_file_sn),
               _input_stream(Utility::get_input_file_stream(file_path)),
               _fs_headers(consume_header(_input_stream)),
               _fp_to_block_info(global_fp_to_info),
               _block_local_sn_to_fp(),
               _fs_global_blocks_sn_order(std::make_unique<std::vector<uint64_t>>())
               {
    _block_local_sn_to_fp.reserve(Config::NUM_LOCAL_BLOCK_RESERVE);
    load_blocks();
    parse_file();
}

std::string FSFile::get_header_value(const std::string& raw_header) {
    static const uint32_t DELIMITER_LENGTH = 2;
    return raw_header.substr(raw_header.find(": ") + DELIMITER_LENGTH);
}

FSMetaData FSFile::consume_header(std::ifstream& input_stream) {
    std::vector<std::string> headers_lines;
    headers_lines.reserve(Config::FS_HEADER::HEADER_EXPECTED_SIZE);

    for(int i=0; i< Config::FS_HEADER::HEADER_EXPECTED_SIZE; ++i)
    {
        std::string line = Utility::get_line(input_stream);
        if(line.size() == 0 || line[0] != '#'){
            break;
        }
        headers_lines.emplace_back(line);
    }

    std::streamoff last_position = input_stream.tellg();

    std::string line = Utility::get_line(input_stream);
    if(!line.empty()) {
        input_stream.clear();
        input_stream.seekg(last_position);
    }

    return {headers_lines,
            get_header_value(headers_lines[Config::FS_HEADER::INPUT_FILE_INDEX]),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::USERNAME_INDEX]))),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::HOSTNAME_INDEX]))),
            get_header_value(headers_lines[Config::FS_HEADER::SNAPSHOT_TIME_INDEX]),
            get_header_value(headers_lines[Config::FS_HEADER::WIN_VERSION_INDEX]),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::LOGICAL_SIZE_INDEX]))),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::LOGICAL_NUM_BLOCKS_INDEX]))),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::PHYSICAL_NUM_BLOCKS_INDEX]))),
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::NUM_FILES_INDEX]))),
            headers_lines.size() < Config::FS_HEADER::HEADER_EXPECTED_SIZE? 0 :
            static_cast<uint64_t>(std::stoull(get_header_value(headers_lines[Config::FS_HEADER::GEN_LEVEL_INDEX])))};
}

void FSFile::load_blocks() {
    static const char* const DELIMITER = ", ";
    while(!_input_stream.eof())
    {
        static const bool DONT_THROW = false;

        const std::string line = Utility::get_line(_input_stream, DONT_THROW);

        if(line.empty() || line[0] != 'B')
        {
            continue;
        }


        std::vector<std::string> split_line = Utility::split_str(line, DELIMITER);
        if(split_line.size() < Config::BLOCK_LINE::MIN_NUM_ELEMENT_IN_BLOCK_LINE)
        {
            throw std::runtime_error("block line doesn't contain enough element, bad input file");
        }

        const std::string& fp = split_line[Config::BLOCK_LINE::FP_INDEX];

        if(_filter_blocks && is_block_filtered(fp)){
            _fs_headers.num_logical_blocks --;
            auto find_res_local = _block_local_sn_to_fp.find(std::stoul(split_line[Config::BLOCK_LINE::LOCAL_SN_INDEX]));
            if(find_res_local == _block_local_sn_to_fp.cend())
            {
                _fs_headers.num_physical_blocks --;
            }

            _block_local_sn_to_fp[std::stoul(split_line[Config::BLOCK_LINE::LOCAL_SN_INDEX])] = fp;
            continue;
        }

        _block_local_sn_to_fp[std::stoul(split_line[Config::BLOCK_LINE::LOCAL_SN_INDEX])] = fp;

        auto find_res = _fp_to_block_info->find(fp);
        if(find_res != _fp_to_block_info->cend())
        {
            continue;
        }

        //new global fp
        const uint32_t new_global_block_sn = _fp_to_block_info->size() + 1;
        (*_fp_to_block_info)[fp] = {new_global_block_sn, Config::DEFAULT_INITIAL_BLOCK_SIZE, {}};
        (*_fp_to_block_info)[fp].files_sn.reserve(Config::FILES_SN_RESERVE_SIZE);
    }
}

bool FSFile::is_block_filtered(const std::string& fp) {
    if(fp[0] == '0' && fp[1] == '0'&& fp[2] == '0'){
        unsigned int x;
        std::stringstream ss;
        ss << std::hex << fp[3];
        ss >> x;

        return x>7;
    }

    return true;
}

void FSFile::parse_file() {
    static constexpr uint64_t START_OF_FILE = 0;
    _input_stream.clear();
    _input_stream.seekg(START_OF_FILE);

    std::streamoff last_position = _input_stream.tellg();
    while(!_input_stream.eof())
    {
        static const bool DONT_THROW = false;
        const std::string line = Utility::get_line(_input_stream, DONT_THROW);

        if(line.empty() || line[0]!='F')
        {
            last_position = _input_stream.tellg();
            continue;
        }

        //go back before F line
        _input_stream.clear();
        _input_stream.seekg(last_position);


        for(int32_t i=0; i< Config::FILE_LINE::NUM_BLOCKS_INDEX; ++i)
        {
            std::string current_token = Utility::get_comma_token(_input_stream, i!=0);
        }

        last_position = _input_stream.tellg();
        std::string file_num_blocks = Utility::get_comma_token(_input_stream);

        if(file_num_blocks.find('\n') != std::string::npos)
        {
            _input_stream.clear();
            _input_stream.seekg(last_position);
            file_num_blocks = Utility::get_line(_input_stream);
        }

        const uint64_t num_blocks = std::stoull(file_num_blocks);

        for(uint64_t i=0; i< num_blocks; ++i)
        {
            const uint64_t block_local_sn = std::stoull(Utility::get_comma_token(_input_stream));
            int32_t block_size = 0;
            if(i == num_blocks - 1 ){
                // If it's the last block, we need to get line or else we will consume until the next
                // ',' in the next line. Since we have only one file in a FS file and the File is the last line this
                // is not that crucial but for good sake I added this fix...
                block_size = std::stoi( Utility::get_line(_input_stream));
            }else{
                block_size = std::stoi(Utility::get_comma_token(_input_stream));
            }

            std::string& fp = _block_local_sn_to_fp[block_local_sn];

            if(_filter_blocks && is_block_filtered(fp)){
                continue;
            }

            auto find_res = _fp_to_block_info->find(fp);
            if(find_res == _fp_to_block_info->cend())
            {
                throw std::runtime_error("block fp is not found in _fp_to_block_info");
            }

            _fs_global_blocks_sn_order->emplace_back(find_res->second.global_sn);
            find_res->second.files_sn.emplace_back(_global_file_sn);

            if((*find_res).second.size == -1){
                (*find_res).second.size = block_size;
            } else{
                if((*find_res).second.size != block_size){
                    std::cout<< "gout different sizes for same fp. fp="<<fp<<
                    ". expected size ="<<(*find_res).second.size << " current size ="<<block_size<<std::endl;
                }
            }

        }

        last_position = _input_stream.tellg();
    }
}

std::shared_ptr<FSSummary> FSFile::get_fs_summary() {
    std::shared_ptr<FSSummary> summary = std::make_shared<FSSummary>();
    summary->global_file_sn = _global_file_sn;
    summary->fs_metadata = _fs_headers;
    summary->blocks_global_sn = _fs_global_blocks_sn_order;

    return std::move(summary);
}

FSFile::~FSFile()
{
    try
    {
        _input_stream.close();
    }
    catch(...)
    {

    }
}


