#pragma once

#include "VolumeCalcInfo.hpp"

#include <string>
#include <map>
#include <set>
#include <utility>
#include <vector>
#include <memory>

using namespace std;

namespace Calculator {
    struct CostResult final {

        CostResult(vector<shared_ptr<VolumeCalcInfo>> volumes_info, const bool is_traffic_valid, const bool is_lb_valid,
                   string error_message, const long long int init_system_size,
                   const long long int received_bytes, const long long int num_bytes_deleted, const double lb_score,
                   const long long int traffic_bytes, const long long int overlap_traffic, const long long int  block_reuse,const long long int  aborted_traffic) :
                   is_traffic_valid(is_traffic_valid),
                   is_lb_valid(is_lb_valid),
                   error_message(std::move(error_message)),
                   init_system_size(init_system_size),
                   final_system_size(init_system_size + received_bytes - num_bytes_deleted),
                   traffic_bytes(traffic_bytes),
                   deletion_bytes(num_bytes_deleted - received_bytes),
                   lb_score(lb_score),
                   overlap_traffic(overlap_traffic),
                   block_reuse(block_reuse),
                   aborted_traffic(aborted_traffic),
                   received_bytes(received_bytes),
                   traffic_percentage(static_cast<double>(traffic_bytes) * 100/static_cast<double>(init_system_size)),
                   deletion_percentage(static_cast<double>(num_bytes_deleted - received_bytes) * 100/static_cast<double>(init_system_size)),
                   volumes_info(std::move(volumes_info))
                   {}

        const bool is_traffic_valid;
        const bool is_lb_valid;
        const string error_message;
        const long long int init_system_size;
        const long long int final_system_size;
        const long long int traffic_bytes;
        const long long int deletion_bytes;
        const long long int overlap_traffic;
        const long long int block_reuse;
        const long long int aborted_traffic;
        const long long int received_bytes;
        const double traffic_percentage;
        const double deletion_percentage;
        const double lb_score;
        vector<shared_ptr<VolumeCalcInfo>> volumes_info;

    };

    shared_ptr<Calculator::CostResult> getClusteringCost(const bool is_change,
            const bool use_cache, const vector<vector<bool>> &appearances_matrix, const map<int, int>& block_to_size,
            const map<string, set<int>>& initial_system_clustering, const map<string, set<int>>& final_system_clustering,
            const map<string, set<int>>& changes_added_files_per_vol,
            const map<string, set<int>>& changes_removed_files_per_vol,
            const std::string& change_input_file_path,
            const long long int allowed_traffic_bytes, const bool dont_validate, const double margin=5, const bool load_balance= false,
            const map<string, double>& lb_sizes = map<string, double>(),  const string& cache_path="cache.db");
};
