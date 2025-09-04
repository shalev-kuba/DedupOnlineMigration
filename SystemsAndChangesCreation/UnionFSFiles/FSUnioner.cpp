#include "FSUnioner.hpp"
#include "../Common/Config.hpp"
#include "../Common/Utility.hpp"


FSUnioner::FSUnioner(const std::string &out_path, const bool is_filter_union) :
_is_filter_union(is_filter_union),
_output_stream(Utility::get_output_file_stream(out_path)),
_global_file_sn(0),
_global_fp_to_info(std::make_shared<std::unordered_map<std::string, GlobalBlockInfo>>()),
_global_block_sn_to_fp(std::make_shared<std::unordered_map<uint64_t , std::string>>()){

    _global_fp_to_info->reserve(Config::NUM_BLOCK_MAP_RESERVE);
    _global_block_sn_to_fp->reserve(Config::NUM_BLOCK_MAP_RESERVE);
    _fs_summaries.reserve(Config::NUM_FS_FILES_RESERVE);
}
void FSUnioner::add_fs(const std::string& fs_path)
{
    FSFile fs_file(fs_path, _global_fp_to_info, _global_file_sn, _is_filter_union);

    _fs_summaries.emplace_back(fs_file.get_fs_summary());
    _global_file_sn++;
}

void FSUnioner::print_to_file() {
    summary_results();
    print_header_to_file();
    print_blocks_to_file();
    print_files_to_file();
}

void FSUnioner::print_header_to_file() {

    std::string input_files;
    for(auto& in_file : _metadata.input_files)
    {
        input_files += (input_files.empty()? "": ", ") + in_file;
    }

    std::string users;
    for(auto& user : _metadata.users)
    {
        users += (users.empty()? "": ", ") + std::to_string(user);
    }

    std::string hosts;
    for(auto& host : _metadata.hosts)
    {
        hosts += (hosts.empty()? "": ", ") + std::to_string(host);
    }

    _output_stream<<"#Output type: block-level"<<std::endl;
    _output_stream<<"#Input Files: "<<input_files<<std::endl;
    _output_stream<<"#Target depth: 1"<<std::endl;
    _output_stream<<"#Num files: "<< _metadata.input_files.size()<<std::endl;
    _output_stream<<"#Num directories: 0"<<std::endl;
    _output_stream<<"#Num Physical Blocks: "<<_metadata.num_physical_blocks<<std::endl;
    _output_stream<<"#Num Summed Physical Blocks: "<<_metadata.sum_of_separated_physical_blocks<<std::endl;
    _output_stream<<"#Num Logical Blocks: "<<_metadata.num_logical_blocks<<std::endl;
    _output_stream<<"#Users: "<<users<<std::endl;
    _output_stream<<"#Hosts names: "<<hosts<<std::endl;
    _output_stream<<"#Logical Size: "<<_metadata.logical_size<<std::endl;
    _output_stream<<"#Physical Size: "<<_metadata.physical_size<<std::endl;

    _output_stream<<std::endl;
}


void FSUnioner::summary_results() {
    _metadata.num_logical_blocks = 0;
    _metadata.num_physical_blocks = 0;
    _metadata.logical_size = 0;
    _metadata.physical_size = 0;
    _metadata.sum_of_separated_physical_blocks = 0;

    for(auto& summary: _fs_summaries)
    {
        _metadata.num_logical_blocks += summary->fs_metadata.num_logical_blocks;
        _metadata.logical_size += summary->fs_metadata.logical_size;
        _metadata.sum_of_separated_physical_blocks += summary->fs_metadata.num_physical_blocks;
        _metadata.users.insert(summary->fs_metadata.username);
        _metadata.hosts.insert(summary->fs_metadata.host_name);
        _metadata.input_files.insert(summary->fs_metadata.input_file);
    }

    _metadata.num_physical_blocks = _global_fp_to_info->size();
    for(auto& block: *_global_fp_to_info)
    {
        _metadata.physical_size += block.second.size;
    }
}

void FSUnioner::print_blocks_to_file() {
    for(auto& block: *_global_fp_to_info)
    {
        (*_global_block_sn_to_fp)[block.second.global_sn] = block.first;
        _output_stream << "B, " << block.second.global_sn << ", " << block.first << ", " << block.second.files_sn.size();
        for(auto file_sn : block.second.files_sn)
        {
            _output_stream<<", " << file_sn;
        }

        _output_stream<<std::endl;
    }

}

void FSUnioner::print_files_to_file() {
    for(auto& summary: _fs_summaries)
    {
        _output_stream << "F, " << summary->global_file_sn << ", " << summary->fs_metadata.input_file << ", " << summary->global_file_sn << ", "
        <<summary->blocks_global_sn->size();
        for(auto& block: *(summary->blocks_global_sn))
        {
            _output_stream<<", "<<block << ", "<< (*_global_fp_to_info)[(*_global_block_sn_to_fp)[block]].size;
        }
        _output_stream << std::endl;
    }
}

FSUnioner::~FSUnioner() {
    try
    {
        _output_stream.close();
    }
    catch(...)
    {
    }
}





