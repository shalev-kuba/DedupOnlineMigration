#include "VolumeCalcInfo.hpp"

#include <utility>
#include "Utils.hpp"
#include "Cache.hpp"

map<string, VolumeCalcInfo> VolumeCalcInfo::_cache;
VolumeCalcInfo::VolumeCalcInfo(string  volume_name, const set<int> &initial_files, const set<int> &final_files,
                               const set<int> &added_files, const set<int> &removed_files, const vector<vector<bool>> &appearances_matrix,
                               const map<int, int>& block_to_size, const bool use_cache, const string& cache_path) :
        m_volume_name(std::move(volume_name)),
        m_init_volume_size(0),
        m_received_bytes(0),
        m_num_bytes_deleted(0),
        m_overlap_mig_changes_traffic(0),
        m_only_mig_traffic(0),
        m_mig_reuse_blocks_spare_traffic_bytes(0),
        m_migration_aborted_traffic(0)
{
    initializeVolumeCalculations(appearances_matrix, initial_files, final_files, added_files, removed_files, block_to_size, use_cache,
                                 cache_path);
}

VolumeCalcInfo::VolumeCalcInfo(string  volume_name, const  long long int init_volume_size,
                               const long long int received_bytes,const  long long int delete_bytes,
                               const  long long int traffic,
                               const  long long int overlap_mig_changes_traffic,
                               const  long long int mig_reuse_blocks_spare_traffic_bytes,
                               const  long long int migration_aborted_traffic) :
        m_volume_name(std::move(volume_name)),
        m_init_volume_size(init_volume_size),
        m_received_bytes(received_bytes),
        m_num_bytes_deleted(delete_bytes),
        m_overlap_mig_changes_traffic(overlap_mig_changes_traffic),
        m_only_mig_traffic(traffic - migration_aborted_traffic/2 - overlap_mig_changes_traffic/2),
        m_mig_reuse_blocks_spare_traffic_bytes(mig_reuse_blocks_spare_traffic_bytes),
        m_migration_aborted_traffic(migration_aborted_traffic)
{
}

std::string VolumeCalcInfo::getMemoryCacheKey(const std::string& vol_name, const std::set<int>& init_files,
                                              const std::set<int>& final_files, const set<int> &added_files,
                                              const set<int> &removed_files){
    const string initial_volume_files = Utils::getSetString(init_files);
    const string final_volume_files = Utils::getSetString(final_files);
    const string added_volume_files = Utils::getSetString(added_files);
    const string removed_volume_files = Utils::getSetString(removed_files);
    const hash<string> hash_function;

    const std::string vol_name_hash = Utils::toString(hash_function(vol_name));
    const std::string initial_files_hash = Utils::toString(hash_function(initial_volume_files));
    const std::string final_files_hash = Utils::toString(hash_function(final_volume_files));
    const std::string added_files_hash = Utils::toString(hash_function(added_volume_files));
    const std::string removed_files_hash = Utils::toString(hash_function(removed_volume_files));
    return Utils::toString(hash_function(vol_name_hash + "-" + initial_files_hash + "-" + final_files_hash +
    "-" + added_files_hash + "-" + removed_files_hash));
}

void VolumeCalcInfo::initializeVolumeCalculations(const vector<vector<bool>> &appearances_matrix,
                                                  const set<int> &initial_files, const set<int> &final_files,
                                                  const set<int> &added_files, const set<int> &removed_files,
                                                  const map<int, int> &block_to_size,
                                                  const bool use_cache, const string& cache_path) {

    // ----------------- start of cache -----------------
    if(cache_path == MEMORY_CACHE_PATH){
        std::string cache_key = getMemoryCacheKey(m_volume_name, initial_files, final_files,added_files, removed_files);
        auto find = _cache.find(cache_key);
        if(find != _cache.end()){
            m_init_volume_size = find->second.m_init_volume_size;
            m_num_bytes_deleted = find->second.m_num_bytes_deleted;
            m_received_bytes = find->second.m_received_bytes;
            m_overlap_mig_changes_traffic = find->second.m_overlap_mig_changes_traffic;
            m_only_mig_traffic = find->second.m_only_mig_traffic;
            m_mig_reuse_blocks_spare_traffic_bytes = find->second.m_mig_reuse_blocks_spare_traffic_bytes;
            m_migration_aborted_traffic = find->second.m_migration_aborted_traffic;
            return;
        }
    }
    // ----------------- End of cache -----------------

    // ------------ Get State Before Changes ----------
    std::set<int> final_files_without_changes;
    std::set_union(final_files.begin(), final_files.end(), removed_files.begin(), removed_files.end(),
                   std::inserter(final_files_without_changes, final_files_without_changes.begin()));

    for (const int& file : added_files) {
        final_files_without_changes.erase(file);
    }

    std::set<int> init_files_with_remove_changes(initial_files.begin(), initial_files.end());
    for (const int& file : removed_files) {
        init_files_with_remove_changes.erase(file);
    }
    // --------- End Get State Before Changes ----------

    const set<int> init_without_removed_files = getBlocksInFiles(appearances_matrix, init_files_with_remove_changes);
    const set<int> final_without_changes = getBlocksInFiles(appearances_matrix, final_files_without_changes);
    const set<int> initial_files_blocks = getBlocksInFiles(appearances_matrix, initial_files);
    const set<int> final_files_blocks = getBlocksInFiles(appearances_matrix, final_files);
    const set<int> add_by_change_files_blocks = getBlocksInFiles(appearances_matrix, added_files);

    // Go over to find the initial size and deleted bytes
    for(const int initial_files_block : initial_files_blocks){
        m_init_volume_size += block_to_size.at(initial_files_block);

        if(final_files_blocks.find(initial_files_block) == final_files_blocks.cend())
            m_num_bytes_deleted += block_to_size.at(initial_files_block);
    }

    for(const int final_files_block : final_without_changes) {
        if(initial_files_blocks.find(final_files_block) != initial_files_blocks.cend()){
            const long long block_size = block_to_size.at(final_files_block);
            bool exists_init_no_removed_files = init_without_removed_files.find(final_files_block) != init_without_removed_files.cend();
            if(!exists_init_no_removed_files && final_files_blocks.find(final_files_block) != final_files_blocks.cend()){
                //  A block is reused by migration if:
                //     exists in final files && not exists in added files && not exists in initial files without removed files
                m_mig_reuse_blocks_spare_traffic_bytes+= block_size;
            }
            continue;
        }

        const long long block_size = block_to_size.at(final_files_block);
        if(final_files_blocks.find(final_files_block) == final_files_blocks.cend()){
            //  A block is aborted in middle of transfer if:
            //     exists in final files without changes && not exists in init state && not exists in final state
            m_migration_aborted_traffic += block_size;
            continue;
        }

        if(add_by_change_files_blocks.find(final_files_block) != add_by_change_files_blocks.cend()){
            //  A block is received by both migration and changes if:
            //     exists in final files without changes && not exists with init state && exists in added files
            m_overlap_mig_changes_traffic += block_size;
            m_received_bytes += block_size;
        }else{
            //  A block is received by migration if:
            //     exists in final files && not exists with init state && not exists in added files
            m_only_mig_traffic += block_size;
            m_received_bytes += block_size;
        }
    }

    // Go over to find the incoming traffic of blocks
    for(const int final_files_block : add_by_change_files_blocks){
        bool exists_initial_files = initial_files_blocks.find(final_files_block) != initial_files_blocks.cend();
        if(exists_initial_files || final_without_changes.find(final_files_block) != final_without_changes.cend()){
            continue;
        }

        const long long block_size = block_to_size.at(final_files_block);
        m_received_bytes += block_size;
    }

    // ----------------- start of cache -----------------
    if(cache_path == MEMORY_CACHE_PATH){
        std::string cache_key = getMemoryCacheKey(m_volume_name, initial_files, final_files, added_files, removed_files);
        auto find = _cache.find(cache_key);
        _cache.emplace(cache_key, *this);
    }
    // ----------------- End of cache -----------------
}

set<int> VolumeCalcInfo::getBlocksInFiles(const vector<vector<bool>> &appearances_matrix, const set<int>& file_indices){
    set<int> blocks_in_files;
    for(const int file_index: file_indices){
        const set<int> blocks_in_file = getBlocksInFile(appearances_matrix, file_index);
        blocks_in_files.insert(blocks_in_file.cbegin(), blocks_in_file.cend());
    }

    return std::move(blocks_in_files);
}

set<int> VolumeCalcInfo::getBlocksInFile(const vector<vector<bool>> &appearances_matrix, const int file_index) {
    set<int> blocks_in_file;
    for(int block_index=0; block_index < appearances_matrix[file_index].size(); ++block_index){
        if(appearances_matrix[file_index][block_index])
            blocks_in_file.insert(block_index);
    }

    return std::move(blocks_in_file);
}
