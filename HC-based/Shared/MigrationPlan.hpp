#pragma once

#include "AlgorithmDSManager.hpp"
#include "Calculator.hpp"

#include <string>
#include <fstream>
#include <memory>

class MigrationPlan {
public:
    explicit MigrationPlan(std::shared_ptr<AlgorithmDSManager>& ds, const std::string& migration_file_path, const bool use_cache, std::string cache_path ="cache.db");
    explicit MigrationPlan(std::shared_ptr<AlgorithmDSManager>& ds, const std::string& migration_file_path, const bool load_summed, const bool use_cache, std::string cache_path ="cache.db");
    ~MigrationPlan();

public:
    static std::vector<std::string> getWorkloadPaths(const std::string& migration_file_path);

public:
    shared_ptr<Calculator::CostResult> getCurrentIterCost() const;
    uint64_t getCurrentIterMaxAllowedTraffic() const;
    float getCurrentIterDesiredMarginPercentages() const;
    void loadNextIter();
    bool isDone() const;
    map<string, set<int>> getVolInitState() const;
    map<string, set<int>> getVolFinalState() const;
    int getTotalWtElapsedTime() const;
    int getTotalElapsedTime() const;

private:
    std::set<int> getVolInitStateByLine(const std::vector<std::string> & splitted_line) const;
    std::set<int> getVolFinalStateByLine(const std::vector<std::string> & splitted_line) const;
    void loadIterMetadata();
    void loadSummedResults();

private:
    std::shared_ptr<AlgorithmDSManager> m_ds;
    std::ifstream m_migration_file;
    bool m_use_cache;
    std::string m_cache_path;
    bool m_is_done;
    bool m_is_last;
    bool m_total_elapsed_time_sec;
    bool m_total_w_t_elapsed_time_sec;
    map<string, set<int>> m_current_initial_system_clustering;
    map<string, set<int>> m_current_final_system_clustering;
    uint64_t m_current_iteration_max_traffic_bytes;
    float m_current_iteration_desired_margin;
};