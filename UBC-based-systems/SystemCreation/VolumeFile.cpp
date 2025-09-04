#include "VolumeFile.hpp"
#include "../Common/Utility.hpp"
#include "../Common/Config.hpp"

#include <utility>
#include <sstream>


VolumeFile::VolumeFile(const std::string &in_vol_path, const std::string &out_vol_path,
                       std::unordered_map<std::string, uint64_t>& fp_to_sn,
                       const uint64_t file_sn_start, const uint32_t vol_id) :
                       _input_stream(Utility::get_input_file_stream(in_vol_path)),
                       _output_stream(Utility::get_output_file_stream(out_vol_path)),
                       _fp_to_sn(fp_to_sn),
                       _sn_files_start(file_sn_start),
                       _vol_id(vol_id),
                       _num_files(0),
                       _local_sn_to_fp()
                       {
    copy_header();
    translate_blocks();
    translate_files();
}

VolumeFile::~VolumeFile() {

    try
    {
        _input_stream.close();
        _output_stream.close();
    }
    catch(...)
    {

    }
}

void VolumeFile::copy_header() {

    for(int i=0; i< Config::VOLUME_HEADER::HEADER_EXPECTED_SIZE; ++i){
        {
            _output_stream << Utility::get_line(_input_stream) << std::endl;
        }
    }

    std::string line = Utility::get_line(_input_stream);
    if(!line.empty())
        throw std::runtime_error("copy_header: Line separator after headers is missing");
}

void VolumeFile::translate_blocks() {
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
            throw std::runtime_error("block line doesn't contain enough element, bad input volume");
        }

        const std::string& fp = split_line[Config::BLOCK_LINE::FP_INDEX];
        const std::string& block_local_sn = split_line[Config::BLOCK_LINE::LOCAL_SN_INDEX];

        _local_sn_to_fp[std::stoull(block_local_sn)] = fp;
        if(_fp_to_sn.find(fp) == _fp_to_sn.cend())
        {
            // new block
            _fp_to_sn[fp] = _fp_to_sn.size() + 1;
        }

        const uint64_t block_new_sn = _fp_to_sn[fp];
        const uint64_t num_files = std::stoull(split_line[Config::BLOCK_LINE::NUM_FILES_INDEX]);

        _output_stream <<"B, " << block_new_sn << ", " << fp << ", " <<  num_files;
        for(uint32_t i=1 ; i<= num_files; ++i)
        {
            const uint32_t index_in_line = i + Config::BLOCK_LINE::NUM_FILES_INDEX;
            const std::string& file_sn = split_line[index_in_line];
            const uint64_t file_new_sn = std::stoull(file_sn) + _sn_files_start;
            _output_stream<<", " << file_new_sn;
        }

        _output_stream<<std::endl;
    }
}

void VolumeFile::translate_files() {
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


        Utility::get_comma_token(_input_stream, false);
        std::string file_sn = Utility::get_comma_token(_input_stream);
        std::string file_id = Utility::get_comma_token(_input_stream);
        std::string file_parent_sn = Utility::get_comma_token(_input_stream);

        last_position = _input_stream.tellg();

        std::string file_num_blocks = Utility::get_comma_token(_input_stream);

        if(file_num_blocks.find('\n') != std::string::npos)
        {
            _input_stream.clear();
            _input_stream.seekg(last_position);
            file_num_blocks = Utility::get_line(_input_stream);
        }

        const uint64_t num_blocks = std::stoull(file_num_blocks);
        const unsigned long long file_new_sn = std::stoull(file_sn) + _sn_files_start;
        ++_num_files;

        _output_stream << "F, " <<file_new_sn << ", " << _vol_id << "_"+file_id << ", " << file_new_sn << ", " <<num_blocks;

        for(uint64_t i=0; i < num_blocks; ++i)
        {
            const std::string& block_sn = Utility::get_comma_token(_input_stream);
            const uint64_t block_new_sn = _fp_to_sn[_local_sn_to_fp[std::stoull(block_sn)]];

            std::string block_size;
            if(i==num_blocks-1){
                const std::string& block_size = Utility::get_line(_input_stream);
                _output_stream<<", "<<block_new_sn << ", "<< block_size;
            } else{
                const std::string& block_size = Utility::get_comma_token(_input_stream);
                _output_stream<<", "<<block_new_sn << ", "<< block_size;
            }

        }

        _output_stream<< std::endl;

        last_position = _input_stream.tellg();
    }
}
