#pragma once

#include "GreedySplit.hpp"
#include "Utils.hpp"

#include <string>
#include <map>
#include <vector>
#include <set>
#include <memory>
#include <algorithm>
#include <iostream>

//relaxed is transfer valid
static bool is_transfer_valid_soft(const std::shared_ptr<transfer_t> &t1) {
    return t1->middle_iter_cost->is_traffic_valid;
}

//hard is transfer valid
static bool is_transfer_valid_hard(const std::shared_ptr<transfer_t> &t1) {
    return t1->middle_iter_cost->is_traffic_valid &&  t1->end_iter_cost->is_lb_valid;
}

/*static bool compareAfterTrafficValidationBestReclaim(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    const long long int deletion1 = t1->end_iter_cost->volumes_info[t1->src_vol_index]->getNumBytesDeleted();
    const long long int deletion2 = t1->end_iter_cost->volumes_info[t1->dst_vol_index]->getNumBytesDeleted(); // movement
    //existed =
    const long long int deletion = deletion1 + deletion2;

    t1->end_iter_cost->deletion_bytes;

    const long long int replication = t1->end_iter_cost->volumes_info[t1->dst_vol_index]->getReceivedBytes();
    double currentReclaim = replication / std::max(1.0, deletion);

    t1->end_iter_cost->deletion_bytes  + t1->end_iter_cost->received_bytes
    return t1->end_iter_cost->deletion_bytes > t2->end_iter_cost->deletion_bytes ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid && !t2->end_iter_cost->is_lb_valid) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid == t2->end_iter_cost->is_lb_valid &&
            t1->end_iter_cost->lb_score > t2->end_iter_cost->lb_score) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid == t2->end_iter_cost->is_lb_valid &&
            t1->end_iter_cost->lb_score == t2->end_iter_cost->lb_score &&
            t1->middle_iter_cost->traffic_bytes < t2->middle_iter_cost->traffic_bytes);
}*/

// regular
static bool compareAfterTrafficValidationTransferDeletion(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    return t1->end_iter_cost->deletion_bytes > t2->end_iter_cost->deletion_bytes ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid && !t2->end_iter_cost->is_lb_valid) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid == t2->end_iter_cost->is_lb_valid &&
            t1->end_iter_cost->lb_score > t2->end_iter_cost->lb_score) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid == t2->end_iter_cost->is_lb_valid &&
            t1->end_iter_cost->lb_score == t2->end_iter_cost->lb_score &&
            t1->middle_iter_cost->traffic_bytes < t2->middle_iter_cost->traffic_bytes);
}

//lb score motivated
static bool compareAfterTrafficValidationTransferLB(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    return t1->end_iter_cost->lb_score > t2->end_iter_cost->lb_score ||
           (t1->end_iter_cost->lb_score == t2->end_iter_cost->lb_score &&
            t1->end_iter_cost->deletion_bytes > t2->end_iter_cost->deletion_bytes) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->lb_score == t2->end_iter_cost->lb_score &&
            t1->end_iter_cost->is_lb_valid && !t2->end_iter_cost->is_lb_valid) ||
           (t1->end_iter_cost->deletion_bytes == t2->end_iter_cost->deletion_bytes &&
            t1->end_iter_cost->is_lb_valid == t2->end_iter_cost->is_lb_valid &&
            t1->end_iter_cost->lb_score == t2->end_iter_cost->lb_score &&
            t1->middle_iter_cost->traffic_bytes < t2->middle_iter_cost->traffic_bytes);
}


static bool compareTransferHardDeletion(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    bool is_t1_valid = is_transfer_valid_hard(t1);
    bool is_t2_valid = is_transfer_valid_hard(t2);
    if (is_t1_valid && !is_t2_valid) {
        return true;
    }

    if (!is_t1_valid && is_t2_valid) {
        return false;
    }

    // both are valid
    return compareAfterTrafficValidationTransferDeletion(t1, t2);
}

static bool compareTransferSoftDeletion(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    bool is_t1_valid = is_transfer_valid_soft(t1);
    bool is_t2_valid = is_transfer_valid_soft(t2);
    if (is_t1_valid && !is_t2_valid) {
        return true;
    }

    if (!is_t1_valid && is_t2_valid) {
        return false;
    }

    // both are valid
    return compareAfterTrafficValidationTransferDeletion(t1, t2);
}

static bool compareTransferSoftLB(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    bool is_t1_valid = is_transfer_valid_soft(t1);
    bool is_t2_valid = is_transfer_valid_soft(t2);
    if (is_t1_valid && !is_t2_valid) {
        return true;
    }

    if (!is_t1_valid && is_t2_valid) {
        return false;
    }

    // both are valid
    return compareAfterTrafficValidationTransferLB(t1, t2);
}

static bool compareTransferHardLB(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    bool is_t1_valid = is_transfer_valid_hard(t1);
    bool is_t2_valid = is_transfer_valid_hard(t2);
    if (is_t1_valid && !is_t2_valid) {
        return true;
    }

    if (!is_t1_valid && is_t2_valid) {
        return false;
    }

    // both are valid
    return compareAfterTrafficValidationTransferLB(t1, t2);
}

static bool compareTransferBestReclaim(const std::shared_ptr<transfer_t> &t1, const std::shared_ptr<transfer_t> &t2) {
    auto total_deletion1 = static_cast<double>(t1->end_iter_cost->deletion_bytes); //Total del = del - replicate
    auto total_deletion2 = static_cast<double>(t2->end_iter_cost->deletion_bytes);

    double only_deleted_blocks1 = total_deletion1 + static_cast<double>(t1->replicated);
    double only_deleted_blocks2 = total_deletion2 + static_cast<double>(t2->replicated);

    double reclaim1 = static_cast<double>(t1->replicated) / std::max(1.0, only_deleted_blocks1);
    double reclaim2 = static_cast<double>(t2->replicated) / std::max(1.0, only_deleted_blocks2);

    return reclaim1 < reclaim2;
}

static void sort_transfers(vector<std::shared_ptr<transfer_t>>& transfers, GreedySplit::TransferSort sort_order){
    switch (sort_order) {
        case GreedySplit::HARD_DELETION:
            std::sort(transfers.begin(), transfers.end(), compareTransferHardDeletion);
            break;
        case GreedySplit::SOFT_DELETION:
            std::sort(transfers.begin(), transfers.end(), compareTransferSoftDeletion);
            break;
        case GreedySplit::SOFT_LB:
            std::sort(transfers.begin(), transfers.end(), compareTransferSoftLB);
            break;
        case GreedySplit::HARD_LB:
            std::sort(transfers.begin(), transfers.end(), compareTransferHardLB);
            break;
    }
}


void GreedySplit::setReplicated(const AlgorithmDSManager &DSManager, const std::shared_ptr<transfer_t>& transfer){
    std::set<int> src_files = DSManager.getInitialClusteringAsVector()[transfer->src_vol_index];
    src_files.erase(transfer->file_index);
    std::set<int> src_blocks_after_transfer = VolumeCalcInfo::getBlocksInFiles(
            DSManager.getAppearancesMatrix(),src_files);
    std::set<int> dst_block = VolumeCalcInfo::getBlocksInFiles(
            DSManager.getAppearancesMatrix(),
            DSManager.getInitialClusteringAsVector()[transfer->dst_vol_index]);
    std::set<int> file_blocks = VolumeCalcInfo::getBlocksInFile(DSManager.getAppearancesMatrix(),
                                                               transfer->file_index);

    auto block_size_map = DSManager.getBlockToSizeMap();
    long long int replicated = 0;
    for(const int& block: file_blocks){
        if(src_blocks_after_transfer.find(block) != src_blocks_after_transfer.cend() && dst_block.find(block) == dst_block.cend()){
            replicated+= block_size_map[block];
        }
    }

    transfer->replicated = replicated;
}

map<string, set<int>> GreedySplit::doNaiveIter(
        const AlgorithmDSManager &DSManager, const int64_t iter_traffic, double iter_margin,
        const map<string, double> &lb_sizes, vector<std::shared_ptr<transfer_t>> &remaining_transfers,
        const vector<string> &workload_paths, const map<string, set<int>> &iter_start_state) {
    static const bool VALIDATE_RESULTS = false;
    static const bool DONT_USE_CACHE = false;
    static const bool LOAD_BALANCED = true;
    static const std::string CHANGE_FILE_NOT_RELEVANT_IF_NOT_CACHE = "not_relevant_change_file";

    vector<std::shared_ptr<transfer_t>> iter_chosen_transfers;
    auto iter_state = iter_start_state;
    static const map<string, set<int>> NO_CHANGES = {};
    int64_t iter_traffic_tmp = iter_traffic;
    while (iter_traffic_tmp > 0 && !remaining_transfers.empty()) // iter
    {
        vector<std::shared_ptr<transfer_t>> valid_traffic_transfers;
        valid_traffic_transfers.reserve(remaining_transfers.size());

        for (auto &transfer: remaining_transfers) {
            const map<int, int> &block_to_size_mapping = DSManager.getBlockToSizeMap();

            map<string, set<int>> final_state = iter_state;

            if(DSManager.isFileRemoved(transfer->file_index)){
                std::cout <<"Give up on moving file since it is removed: "<< transfer->file_index << std::endl;
                continue;
            }

            final_state[workload_paths[transfer->src_vol_index]].erase(transfer->file_index);
            final_state[workload_paths[transfer->dst_vol_index]].emplace(transfer->file_index);

            // In order to calc deletion we will compare between the start state of the iteration to the current
            // State of the iteration (without GC)
            transfer->end_iter_cost = Calculator::getClusteringCost(
                    false,
                    DONT_USE_CACHE,
                    DSManager.getAppearancesMatrix(),
                    block_to_size_mapping,
                    iter_state,
                    final_state,
                    NO_CHANGES,
                    NO_CHANGES,
                    CHANGE_FILE_NOT_RELEVANT_IF_NOT_CACHE,
                    iter_traffic_tmp,
                    VALIDATE_RESULTS, //should not pay attention to traffic validation since it's calculated in the middle_iter_cost
                    iter_margin,
                    LOAD_BALANCED,
                    lb_sizes,
                    VolumeCalcInfo::MEMORY_CACHE_PATH);

            if(!transfer->end_iter_cost->is_traffic_valid){
                continue;
            }

            setReplicated(DSManager, transfer);

            valid_traffic_transfers.emplace_back(transfer);
        }

        if (valid_traffic_transfers.empty()) {
            break;
        }

        std::sort(valid_traffic_transfers.begin(), valid_traffic_transfers.end(), compareTransferBestReclaim);

        iter_chosen_transfers.emplace_back(valid_traffic_transfers.front());

        iter_state[workload_paths[valid_traffic_transfers.front()->src_vol_index]].erase(
                valid_traffic_transfers.front()->file_index);
        iter_state[workload_paths[valid_traffic_transfers.front()->dst_vol_index]].emplace(
                valid_traffic_transfers.front()->file_index);

        iter_traffic_tmp -= valid_traffic_transfers.front()->end_iter_cost->traffic_bytes;

        auto it = find(remaining_transfers.begin(), remaining_transfers.end(),
                       valid_traffic_transfers.front());

        remaining_transfers.erase(it);
    }

    return iter_state;
}

map<string, set<int>> GreedySplit::doIter(const AlgorithmDSManager &DSManager, const int64_t iter_traffic, double iter_margin,
                             const map<string, double> &lb_sizes, vector<std::shared_ptr<transfer_t>> &remaining_transfers,
                             const vector<string> &workload_paths, const map<string, set<int>> &iter_start_state,
                             TransferSort sort_order) {
    static const bool VALIDATE_RESULTS = false;
    static const bool DONT_USE_CACHE = false;
    static const bool LOAD_BALANCED = true;
    static const std::string CHANGE_FILE_NOT_RELEVANT_IF_NOT_CACHE = "not_relevant_change_file";

    vector<std::shared_ptr<transfer_t>> iter_chosen_transfers;
    auto iter_state = iter_start_state;
    auto iter_state_without_removes = iter_start_state;
    static const map<string, set<int>> NO_CHANGES = {};
    int64_t iter_traffic_tmp = iter_traffic;
    while (iter_traffic_tmp > 0 && !remaining_transfers.empty()) // iter
    {
        vector<std::shared_ptr<transfer_t>> valid_traffic_transfers;
        valid_traffic_transfers.reserve(remaining_transfers.size());

        for (auto &transfer: remaining_transfers) {
            const map<int, int> &block_to_size_mapping = DSManager.getBlockToSizeMap();

            map<string, set<int>> final_state_without_removes = iter_state_without_removes;
            map<string, set<int>> final_state = iter_state;

            if(DSManager.isFileRemoved(transfer->file_index)){
                std::cout <<"Give up on moving file since it is removed: "<< transfer->file_index << std::endl;
                continue;
            }

            final_state[workload_paths[transfer->src_vol_index]].erase(transfer->file_index);
            final_state[workload_paths[transfer->dst_vol_index]].emplace(transfer->file_index);
            final_state_without_removes[workload_paths[transfer->dst_vol_index]].emplace(transfer->file_index);

            // use the "don't validate result" so all iteration's params are not needed
            // In order to calc traffic we will compare between the current state of the iteration with GC
            // with the end of the iteration (with GC)
            transfer->middle_iter_cost = Calculator::getClusteringCost(
                    false,
                    DONT_USE_CACHE,
                    DSManager.getAppearancesMatrix(),
                    block_to_size_mapping,
                    iter_state_without_removes,
                    final_state_without_removes,
                    NO_CHANGES,
                    NO_CHANGES,
                    CHANGE_FILE_NOT_RELEVANT_IF_NOT_CACHE,
                    iter_traffic_tmp,
                    VALIDATE_RESULTS,
                    iter_margin,
                    LOAD_BALANCED,
                    lb_sizes,
                    VolumeCalcInfo::MEMORY_CACHE_PATH);

            if(!transfer->middle_iter_cost->is_traffic_valid){
                continue;
            }

            // In order to calc deletion we will compare between the start state of the iteration to the current
            // State of the iteration (without GC)
            transfer->end_iter_cost = Calculator::getClusteringCost(
                    false,
                    DONT_USE_CACHE,
                    DSManager.getAppearancesMatrix(),
                    block_to_size_mapping,
                    iter_state,
                    final_state,
                    NO_CHANGES,
                    NO_CHANGES,
                    CHANGE_FILE_NOT_RELEVANT_IF_NOT_CACHE,
                    iter_traffic_tmp,
                    VALIDATE_RESULTS, //should not pay attention to traffic validation since it's calculated in the middle_iter_cost
                    iter_margin,
                    LOAD_BALANCED,
                    lb_sizes,
                    VolumeCalcInfo::MEMORY_CACHE_PATH);

            valid_traffic_transfers.emplace_back(transfer);
        }

        if (valid_traffic_transfers.empty()) {
            break;
        }

        sort_transfers(valid_traffic_transfers, sort_order);

        iter_chosen_transfers.emplace_back(valid_traffic_transfers.front());

        iter_state_without_removes[workload_paths[valid_traffic_transfers.front()->dst_vol_index]].emplace(
                valid_traffic_transfers.front()->file_index);

        iter_state[workload_paths[valid_traffic_transfers.front()->src_vol_index]].erase(
                valid_traffic_transfers.front()->file_index);
        iter_state[workload_paths[valid_traffic_transfers.front()->dst_vol_index]].emplace(
                valid_traffic_transfers.front()->file_index);

        iter_traffic_tmp -= valid_traffic_transfers.front()->middle_iter_cost->traffic_bytes;

        auto it = find(remaining_transfers.begin(), remaining_transfers.end(),
                       valid_traffic_transfers.front());

        remaining_transfers.erase(it);
    }

    return iter_state;
}

vector<std::shared_ptr<transfer_t>> GreedySplit::getTransfers(map<std::string, int> &workload_path_to_index,
                                                 const vector<string> &workload_paths,
                                                 map<string, set<int>> initial_state,
                                                 map<string, set<int>> final_state) {
    vector<std::shared_ptr<transfer_t>> transfers;


    map<string, set<int>> new_files;
    map<string, set<int>> removed_files;
    for (auto &vol_name: workload_paths) {
        set<int> init_set = initial_state.at(vol_name);
        set<int> final_set = final_state.at(vol_name);
        std::set<int> tmp_left_files;
        std::set<int> tmp_new_files;

        std::set_difference(init_set.begin(), init_set.end(),
                            final_set.begin(), final_set.end(),
                            std::inserter(tmp_left_files, tmp_left_files.begin()));
        std::set_difference(final_set.begin(), final_set.end(),
                            init_set.begin(), init_set.end(),
                            std::inserter(tmp_new_files, tmp_new_files.begin()));

        new_files[vol_name] = tmp_new_files;
        removed_files[vol_name] = tmp_left_files;
    }

    for (const auto &pair: new_files) {
        const string &workload = pair.first;
        const set<int> new_files_set = pair.second;
        for (auto new_file_index: new_files_set) {
            for (const auto &remove_pair: removed_files) {
                const string &removed_workload = remove_pair.first;
                const set<int> removed_files_set = remove_pair.second;
                if (removed_workload == workload) {
                    continue;
                }

                if (removed_files_set.find(new_file_index) != removed_files_set.cend()) {
                    transfer_t transfer = {0};
                    transfer.replicated = -1;
                    transfer.file_index = new_file_index;
                    transfer.dst_vol_index = workload_path_to_index[workload];
                    transfer.src_vol_index = workload_path_to_index[removed_workload];
                    transfers.emplace_back(std::make_shared<transfer_t>(transfer));
                    break;
                }
            }
        }
    }

    return transfers;
}

vector<std::shared_ptr<transfer_t>> GreedySplit::getTransfers(shared_ptr<AlgorithmDSManager> &DSManager,
                                                 map<std::string, int> &workload_path_to_index,
                                                 const vector<string> &workload_paths,
                                                 const std::string &migration_plan_path,
                                                 std::shared_ptr<MigrationPlan> &migration_plan) {
    migration_plan = make_shared<MigrationPlan>(DSManager, migration_plan_path, false,  true);
    auto initial_state = migration_plan->getVolInitState();
    auto final_state = migration_plan->getVolFinalState();

    return getTransfers(workload_path_to_index, workload_paths, initial_state, final_state);
}