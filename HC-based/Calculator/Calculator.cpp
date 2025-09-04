#include "Calculator.hpp"
#include "Utils.hpp"
#include "Cache.hpp"

#include <iostream>
#include <fstream>
#include <exception>

namespace Calculator {
    static vector<string> getSortedVolumesNames(const map<string, set<int>>& system_clustering){
        vector<string> volumes_names_vector;
        volumes_names_vector.reserve(system_clustering.size());
        for(const auto& volume_files_mapping : system_clustering)
            volumes_names_vector.emplace_back(volume_files_mapping.first);

        sort(volumes_names_vector.begin(), volumes_names_vector.end());

        return std::move(volumes_names_vector);
    }

    static vector<shared_ptr<VolumeCalcInfo>> getVolumesCost(const vector<string>& sorted_volumes_names,
                                                             const vector<vector<bool>> &appearances_matrix,
                                                             const map<int, int>& block_to_size,
                                                             const map<string, set<int>>& initial_system_clustering,
                                                             const map<string, set<int>>& final_system_clustering,
                                                             const map<string, set<int>>& added_files_per_vol,
                                                             const map<string, set<int>>& removed_files_per_vol,
                                                             const bool use_cache, const string& cache_path){

        vector<shared_ptr<VolumeCalcInfo>> volumes_info;
        volumes_info.reserve(sorted_volumes_names.size());
        for(const string& vol_name : sorted_volumes_names){
            const set<int>& volume_initial_files = initial_system_clustering.at(vol_name);
            const set<int>& volume_final_files = final_system_clustering.at(vol_name);
            set<int> added_files = {};
            if(added_files_per_vol.find(vol_name) != added_files_per_vol.cend()){
                added_files = added_files_per_vol.at(vol_name);
            }

            set<int> removed_files = {};
            if(removed_files_per_vol.find(vol_name) != removed_files_per_vol.cend()){
                removed_files = removed_files_per_vol.at(vol_name);
            }

            volumes_info.emplace_back(make_shared<VolumeCalcInfo>(vol_name, volume_initial_files,
                                                                  volume_final_files, added_files, removed_files,
                                                                  appearances_matrix, block_to_size,
                                                                  use_cache, cache_path));
        }

        return volumes_info;
    }

    static vector<shared_ptr<VolumeCalcInfo>> loadVolumesCostFromCacheLine(const vector<string>& sorted_volumes_names,
                                                                           const Cache::SystemCacheLine& cache_result){

        vector<shared_ptr<VolumeCalcInfo>> volumes_info;
        volumes_info.reserve(sorted_volumes_names.size());
        for(int i=0; i< sorted_volumes_names.size(); ++i){
            const string& vol_name = sorted_volumes_names[i];
            volumes_info.emplace_back(make_shared<VolumeCalcInfo>(vol_name, cache_result.initial_volumes_sizes[i],
                                                                  cache_result.volumes_receive_bytes[i],
                                                                  cache_result.volumes_deletion[i],
                                                                  cache_result.volumes_traffic[i],
                                                                  cache_result.volumes_overlap_traffic[i],
                                                                  cache_result.volumes_block_reuse[i],
                                                                  cache_result.volumes_aborted_traffic[i]
                                                                  ));
        }

        return volumes_info;
    }

    static bool isAllInMargin(const vector<shared_ptr<VolumeCalcInfo>>& volumes_info,
                              const long long int final_system_size, const double margin,
                              const map<string, double>& lb_sizes, string& error_message){

        for(const auto& volume_info: volumes_info){
            const double volume_percentage = static_cast<double>(volume_info->getFinalVolumeSize()) /
                    static_cast<double>(final_system_size);

            const string volume_name = volume_info->getVolumeName();
            const double lb_size = lb_sizes.at(volume_name);
            if(volume_percentage * 100 > lb_size + margin|| volume_percentage * 100 < lb_size - margin){
                error_message = string("volume=") + volume_name + " size=" + to_string(volume_percentage * 100) +
                        "% is not between the corresponding lb_size margin";
                return false;
            }
        }

        return true;
    }

    static bool isTrafficValid(const long long int traffic_bytes, const long long int  allowed_traffic_bytes){
        return traffic_bytes <= allowed_traffic_bytes;
    }

    static double getLbScore(const vector<shared_ptr<VolumeCalcInfo>>& volumes_info, const map<string,double>& lb_sizes){
        vector<double> normalized_volumes_sizes = {};
        for(const auto& volume_info: volumes_info){
            const long long int final_volume_size = volume_info->getFinalVolumeSize();
            if(lb_sizes.empty())
                normalized_volumes_sizes.emplace_back(final_volume_size);
            else
                normalized_volumes_sizes.emplace_back((100.0/volumes_info.size())/lb_sizes.at(volume_info->getVolumeName()) * final_volume_size);
        }

        return  *min_element(normalized_volumes_sizes.cbegin(),normalized_volumes_sizes.cend())/
                *max_element(normalized_volumes_sizes.cbegin(),normalized_volumes_sizes.cend());
    }

    static shared_ptr<Calculator::CostResult> getCostFromCacheLine(
            const vector<string>& sorted_volumes_names, const Cache::SystemCacheLine& cache_result,
            const long long int allowed_traffic_bytes, const double margin, const bool load_balance,
            const map<string,double>& lb_sizes,  const bool dont_validate){
        const vector<shared_ptr<VolumeCalcInfo>> volumes_info = loadVolumesCostFromCacheLine(sorted_volumes_names,
                                                                                             cache_result);

        const long long int final_system_size = cache_result.initial_system_size + cache_result.receive_bytes -
                cache_result.num_bytes_deleted;

        const bool is_valid_traffic = !dont_validate && isTrafficValid(cache_result.traffic_bytes, allowed_traffic_bytes);

        string error_message;
        const bool is_valid_lb = !dont_validate && (!load_balance || isAllInMargin(volumes_info,  final_system_size, margin, lb_sizes, error_message));

        return std::make_shared<Calculator::CostResult>(volumes_info, is_valid_traffic, is_valid_lb, error_message, cache_result.initial_system_size,
                                                        cache_result.receive_bytes, cache_result.num_bytes_deleted, getLbScore(volumes_info, lb_sizes),
                                                        cache_result.traffic_bytes, cache_result.overlap_traffic,
                                                        cache_result.block_reuse, cache_result.aborted_traffic);
    }

    static void fillIndexingHashes(const vector<string>& sorted_volumes_names, const bool is_change,
                                   const std::string& change_input_file_path,
                                   const map<string, set<int>>& initial_system_clustering,
                                   const map<string, set<int>>& final_system_clustering,
                                   string& volumes_hash, string& initial_files_hash, string& final_files_hash){

        bool first = true;
        for(const string& volume_name : sorted_volumes_names){
            const string initial_volume_files = Utils::getSetString(initial_system_clustering.at(volume_name));
            const string final_volume_files = Utils::getSetString(final_system_clustering.at(volume_name));
            volumes_hash += string((first? "" : ",")) + volume_name;
            initial_files_hash += string((first? "" : ",")) + initial_volume_files;
            final_files_hash += string((first? "" : ",")) + final_volume_files;
            first = false;
        }
        const hash<string> hash_function;
        volumes_hash += ",ChangePath" + change_input_file_path + "," + (is_change? "1" :"0");

        volumes_hash = Utils::toString(hash_function(volumes_hash));
        initial_files_hash = Utils::toString(hash_function(initial_files_hash));
        final_files_hash = Utils::toString(hash_function(final_files_hash));
    }

    static shared_ptr<Calculator::CostResult> getCalculateCost(
            const vector<string>& sorted_volumes_names, const vector<vector<bool>> &appearances_matrix,
            const map<int, int>& block_to_size, const map<string, set<int>>& initial_system_clustering,
            const map<string, set<int>>& final_system_clustering,
            const map<string, set<int>>& change_added_file_per_vol,
            const map<string, set<int>>& change_removed_file_per_vol,
            const long long int allowed_traffic_bytes,
            const double margin, const bool load_balance, const map<string,double>& lb_sizes,
            vector<long long int>& init_vol_sizes, vector<long long int>& vol_traffics,
            vector<long long int>& vol_deletions, vector<long long int>& vol_receive_bytes,
            vector<long long int>& vol_overlap_traffic,
            vector<long long int>& vol_block_reuse,
            vector<long long int>& vol_aborted_traffic,const bool dont_validate, const bool use_cache,
            const string& cache_path){

        const vector<shared_ptr<VolumeCalcInfo>> volumes_info =
                getVolumesCost(sorted_volumes_names, appearances_matrix, block_to_size, initial_system_clustering,
                               final_system_clustering, change_added_file_per_vol, change_removed_file_per_vol,
                               use_cache, cache_path);

        long long int init_system_size = 0, traffic_bytes = 0, received_bytes = 0, num_bytes_deleted = 0;
        long long int overlap_traffic = 0, block_reuse = 0, aborted_traffic = 0;
        for(const auto& volume_info: volumes_info){
            init_system_size += volume_info->getInitVolumeSize();
            traffic_bytes += volume_info->getTrafficBytes();
            received_bytes += volume_info->getReceivedBytes();
            num_bytes_deleted += volume_info->getNumBytesDeleted();
            overlap_traffic += volume_info->getOverlapTrafficBytes();
            block_reuse += volume_info->getBlockReuseBytes();
            aborted_traffic += volume_info->getAbortedTrafficBytes();

            init_vol_sizes.emplace_back(volume_info->getInitVolumeSize());
            vol_traffics.emplace_back(volume_info->getTrafficBytes());
            vol_deletions.emplace_back(volume_info->getNumBytesDeleted());
            vol_receive_bytes.emplace_back(volume_info->getReceivedBytes());
            vol_overlap_traffic.emplace_back(volume_info->getOverlapTrafficBytes());
            vol_block_reuse.emplace_back(volume_info->getBlockReuseBytes());
            vol_aborted_traffic.emplace_back(volume_info->getAbortedTrafficBytes());
        }


        const bool is_valid_traffic = !dont_validate && isTrafficValid(traffic_bytes, allowed_traffic_bytes);

        string error_message;
        const bool is_valid_lb = !dont_validate &&(!load_balance || isAllInMargin(volumes_info,  init_system_size + received_bytes - num_bytes_deleted, margin, lb_sizes, error_message));

        const double lb_score = getLbScore(volumes_info, lb_sizes);

        return make_shared<Calculator::CostResult>(volumes_info, is_valid_traffic, is_valid_lb , error_message, init_system_size, received_bytes,
                                                   num_bytes_deleted,lb_score, traffic_bytes, overlap_traffic, block_reuse, aborted_traffic);
    }

    shared_ptr<Calculator::CostResult> getClusteringCost(const bool is_change,
            const bool use_cache, const vector<vector<bool>> &appearances_matrix,  const map<int, int>& block_to_size,
            const map<string, set<int>>& initial_system_clustering,
            const map<string, set<int>>& final_system_clustering,
            const map<string, set<int>>& change_added_files_per_vol,
            const map<string, set<int>>& change_removed_files_per_vol,
            const std::string& change_input_file_path,
            const long long int allowed_traffic_bytes, const bool dont_validate, const double margin,
            const bool load_balance,const map<string,double>& lb_sizes, const string& cache_path){

        const vector<string> sorted_volumes_names =  getSortedVolumesNames(initial_system_clustering);
        string volumes_hash, initial_files_hash, final_files_hash;
        fillIndexingHashes(sorted_volumes_names, is_change, change_input_file_path, initial_system_clustering,
                           final_system_clustering, volumes_hash, initial_files_hash, final_files_hash);

        if(use_cache)
        {
            try {
                const Cache::SystemCacheLine cache_result = Cache::getSystemResult(cache_path, volumes_hash,
                                                                                   initial_files_hash, final_files_hash);
                const bool result_exist = cache_result.initial_system_size >= 0;
                if(result_exist)
                    return getCostFromCacheLine(sorted_volumes_names, cache_result, allowed_traffic_bytes, margin,load_balance,lb_sizes, dont_validate);
            }catch (const runtime_error& e){
                if(string(e.what()).find("lock") == string::npos)
                    throw;
            }
        }

        vector<long long int> init_vol_sizes, vol_traffics, vol_deletions, vol_receive_bytes, vol_overlap_traffic,
                vol_block_reuse, vol_aborted_traffic;
        shared_ptr<Calculator::CostResult> cost_result =
                getCalculateCost(sorted_volumes_names, appearances_matrix, block_to_size, initial_system_clustering,
                                 final_system_clustering, change_added_files_per_vol,
                                 change_removed_files_per_vol,
                                 allowed_traffic_bytes, margin, load_balance, lb_sizes,
                                 init_vol_sizes, vol_traffics, vol_deletions,vol_receive_bytes,vol_overlap_traffic,
                                 vol_block_reuse, vol_aborted_traffic, dont_validate,
                                 use_cache, cache_path);

        if(use_cache)
        {
            Cache::insertSystemResult(cache_path, volumes_hash, initial_files_hash, final_files_hash,
                                      cost_result->init_system_size,
                                      init_vol_sizes, vol_traffics, vol_deletions, vol_receive_bytes, vol_overlap_traffic, vol_block_reuse, vol_aborted_traffic);
        }

        return cost_result;
    }
}
