#pragma once

#include "Calculator.hpp"
#include "AlgorithmDSManager.hpp"
#include "MigrationPlan.hpp"

#include <string>
#include <map>
#include <vector>
#include <unordered_set>
#include <memory>

struct transfer_t {
    int file_index;
    int src_vol_index;
    int dst_vol_index;
    long long int replicated;
    std::shared_ptr<Calculator::CostResult> middle_iter_cost;
    std::shared_ptr<Calculator::CostResult> end_iter_cost;
};


namespace GreedySplit {
    enum TransferSort{
        HARD_DELETION,
        SOFT_DELETION,
        SOFT_LB,
        HARD_LB,
    };
    vector<std::shared_ptr<transfer_t>> getTransfers(map<std::string, int> &workload_path_to_index,
                                                     const vector<string> &workload_paths,
                                                     map<string, set<int>> initial_state,
                                                     map<string, set<int>> final_state);

    vector<std::shared_ptr<transfer_t>> getTransfers(shared_ptr<AlgorithmDSManager> &DSManager,
                                                     map<std::string, int> &workload_path_to_index,
                                                     const vector<string> &workload_paths,
                                                     const std::string &migration_plan_path,
                                                     std::shared_ptr<MigrationPlan> &migration_plan);

    map<string, set<int>> doNaiveIter(
            const AlgorithmDSManager &DSManager, const int64_t iter_traffic, double iter_margin,
            const map<string, double> &lb_sizes, vector<std::shared_ptr<transfer_t>> &remaining_transfers,
            const vector<string> &workload_paths, const map<string, set<int>> &iter_start_state);

    map<string, set<int>> doIter(const AlgorithmDSManager &DSManager, int64_t traffic, double margin,
                                 const map<string, double> &lb_sizes, vector<std::shared_ptr<transfer_t>> &remaining_transfers,
                                 const vector<string> &workload_paths, const map<string, set<int>> &init_state,
                                 TransferSort sort_order=SOFT_DELETION);

    void setReplicated(const AlgorithmDSManager &DSManager, const std::shared_ptr<transfer_t>& transfer);
}
