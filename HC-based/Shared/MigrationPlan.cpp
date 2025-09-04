#include "MigrationPlan.hpp"
#include <stdexcept>
#include <iostream>

MigrationPlan::MigrationPlan(std::shared_ptr<AlgorithmDSManager> &ds,
                             const std::string &migration_file_path, const bool use_cache,  std::string cache_path) :
        m_ds(ds),
        m_migration_file(migration_file_path),
        m_is_done(false),
        m_use_cache(use_cache),
        m_is_last(false),
        m_cache_path(std::move(cache_path)),
        m_current_iteration_desired_margin(0),
        m_current_iteration_max_traffic_bytes(0)
{
    loadNextIter();
    loadSummedResults();
}

MigrationPlan::MigrationPlan(std::shared_ptr<AlgorithmDSManager> &ds,
                             const std::string &migration_file_path, const bool load_summed, const bool use_cache,  std::string cache_path) :
        m_ds(ds),
        m_migration_file(migration_file_path),
        m_is_done(false),
        m_use_cache(use_cache),
        m_is_last(false),
        m_cache_path(std::move(cache_path)),
        m_current_iteration_desired_margin(0),
        m_current_iteration_max_traffic_bytes(0)
{
    loadNextIter();
    if(load_summed)
        loadSummedResults();
}

void MigrationPlan::loadSummedResults() {
    try {
        // peek to check whether it's the last iteration
        std::streamoff last_position = m_migration_file.tellg();
        std::string line_content;
        if(!std::getline(m_migration_file, line_content) || line_content.find(',') != std::string::npos){
            throw runtime_error("should be an empty line");
        };

        if(!std::getline(m_migration_file, line_content) || line_content.find("Summed results:") == std::string::npos){
            throw runtime_error("Line should contain \"Summed results:\"");
        };

        if(!std::getline(m_migration_file, line_content)){
            throw runtime_error("Line should be header line");
        };

        if(!std::getline(m_migration_file, line_content)) {
            throw runtime_error("Line should contained summed values");
        }
        const std::vector<std::string> splitted_line_content = Utility::splitString(line_content, ',');

        static int TOTAL_ELAPSED_TIME_INDEX = 4;
        static int TOTAL_ELAPSED_W_T_TIME_INDEX = 5;
        m_total_elapsed_time_sec = stoi(splitted_line_content[TOTAL_ELAPSED_TIME_INDEX]);
        m_total_w_t_elapsed_time_sec = stoi(splitted_line_content[TOTAL_ELAPSED_W_T_TIME_INDEX]);

        m_migration_file.clear();
        m_migration_file.seekg(last_position);
    }catch (...){
        throw runtime_error("MigrationPlan::loadSummedResults: got exception when tried to load summed results");
    }
}

std::set<int> MigrationPlan::getVolInitStateByLine(const vector<std::string> &splitted_line) const{
    static const int VOL_INIT_STATE_INDEX = 1;
    const std::vector<std::string> init_state_sn = Utility::splitString(splitted_line[VOL_INIT_STATE_INDEX], '-');

    std::set<int> init_state;
    for(const std::string& sn_as_string: init_state_sn){
        try{
            init_state.insert(m_ds->getFileIndex(stoi(sn_as_string)));

        }catch (...){
            std::cout << "Erro with sn: " <<sn_as_string << " " << std::endl;
            throw;
        }
    }

    return std::move(init_state);
}

std::set<int> MigrationPlan::getVolFinalStateByLine(const vector<std::string> &splitted_line) const{
    static const int VOL_FINAL_STATE_INDEX = 2;
    const std::vector<std::string> final_state_sn = Utility::splitString(splitted_line[VOL_FINAL_STATE_INDEX], '-');

    std::set<int> final_state;
    for(const std::string& sn_as_string: final_state_sn){
        try{
            final_state.insert(m_ds->getFileIndex(stoi(sn_as_string)));

        }catch (...){
            std::cout << "Erro with sn: " <<sn_as_string << " skipping it" << std::endl;
            //throw;
        }
    }

    return std::move(final_state);
}

map<string, set<int>> MigrationPlan::getVolInitState() const{
    return m_current_initial_system_clustering;
}

int MigrationPlan::getTotalWtElapsedTime() const{
    return m_total_w_t_elapsed_time_sec;
}

int MigrationPlan::getTotalElapsedTime() const{
    return m_total_elapsed_time_sec;
}

map<string, set<int>> MigrationPlan::getVolFinalState() const{
    return m_current_final_system_clustering;
}

bool MigrationPlan::isDone() const {
    return m_is_done;
}

void MigrationPlan::loadIterMetadata() {
    std::string line_content;
    std::getline(m_migration_file, line_content); // just headers
    std::getline(m_migration_file, line_content); // the concrete values

    const std::vector<std::string> values_list = Utility::splitString(line_content, ',');

    constexpr uint32_t ITER_MAX_TRAFFIC_INDEX = 2;
    constexpr uint32_t ITER_DESIRED_MARGIN_PERC_INDEX = 7;

    m_current_iteration_max_traffic_bytes = std::stoull(values_list[ITER_MAX_TRAFFIC_INDEX]);
    m_current_iteration_desired_margin = std::stof(values_list[ITER_DESIRED_MARGIN_PERC_INDEX]);

    std::getline(m_migration_file, line_content); // just space line
}

void MigrationPlan::loadNextIter() {
    m_is_done = m_is_last;
    if (isDone())
        return;

    m_current_initial_system_clustering = {};
    m_current_final_system_clustering = {};

    std::string line_content;

    try {
        loadIterMetadata();
        std::getline(m_migration_file, line_content); // just headers line
        std::cout << "header :" << line_content<< std::endl;

        for(int i=0; i < m_ds->getNumOfWorkloads(); ++i)
        {
            std::getline(m_migration_file, line_content);
            const std::vector<std::string> splitted_line_content = Utility::splitString(line_content, ',');

            constexpr int VOL_NAME_INDEX = 0;
            const std::string vol_path = splitted_line_content[VOL_NAME_INDEX];
            std::cout << "before m_current_initial_system_clustering: "<< line_content << std::endl;

            m_current_initial_system_clustering[vol_path] = getVolInitStateByLine(splitted_line_content);
            std::cout << "after m_current_initial_system_clustering: "<< line_content << std::endl;

            m_current_final_system_clustering[vol_path] = getVolFinalStateByLine(splitted_line_content);
            std::cout << "after m_current_final_system_clustering: "<< line_content << std::endl;

        }

        std::getline(m_migration_file, line_content); // just summed result line
        std::getline(m_migration_file, line_content); // just space line

        // peek to check whether it's the last iteration
        std::streamoff last_position = m_migration_file.tellg();
        std::getline(m_migration_file, line_content);
        m_is_last = line_content.find(',') == std::string::npos;
        m_migration_file.clear();
        m_migration_file.seekg(last_position);
    }catch (...){
        std::cout << "Bad line content: "<< line_content << std::endl;
        throw runtime_error("MigrationPlan::loadNextIter: got exception when tried to load next iter");
    }
}

shared_ptr<Calculator::CostResult> MigrationPlan::getCurrentIterCost() const{
    if (isDone())
        throw runtime_error("MigrationPlan::get_current_iter_cost: Out of bound");

    static const bool dont_validate_results = true;
    const map<int, int>& block_to_size_mapping = m_ds->getBlockToSizeMap();

    // use the "don't validate result" so all iteration's params are not needed
    return Calculator::getClusteringCost(false,
                                         m_use_cache,
                                         m_ds->getAppearancesMatrix(),
                                         block_to_size_mapping,
                                         m_current_initial_system_clustering,
                                         m_current_final_system_clustering,
                                         {},
                                         {},
                                         "input_file",
                                         0,
                                         dont_validate_results,
                                         0,
                                         false,
                                         {},
                                         m_cache_path);
}

uint64_t MigrationPlan::getCurrentIterMaxAllowedTraffic() const{
    if (isDone())
        throw runtime_error("MigrationPlan::getCurrentIterMaxAllowedTraffic: Out of bound");

    return m_current_iteration_max_traffic_bytes;
}

float MigrationPlan::getCurrentIterDesiredMarginPercentages() const{
    if (isDone())
        throw runtime_error("MigrationPlan::getCurrentIterDesiredMarginPercentages: Out of bound");

    return m_current_iteration_desired_margin;
}

MigrationPlan::~MigrationPlan() {
    try {
        m_migration_file.close();
    } catch (...) {

    }
}

std::vector<std::string> MigrationPlan::getWorkloadPaths(const string &migration_file_path) {
    std::vector<std::string> res_vec;
    std::string line_content;
    std::ifstream file_stream(migration_file_path);
    constexpr uint32_t LINES_TO_FIRST_WORKLOAD = 5;
    for(int i=0; i< LINES_TO_FIRST_WORKLOAD; ++i){
        std::getline(file_stream, line_content); // just headers line
    }

    std::vector<std::string> splitted_line_content = Utility::splitString(line_content, ',');

    while (!splitted_line_content.front().empty()){
        res_vec.emplace_back(splitted_line_content.front());
        std::getline(file_stream, line_content); // just headers line
        splitted_line_content = Utility::splitString(line_content, ',');
    }

    return res_vec;
}


