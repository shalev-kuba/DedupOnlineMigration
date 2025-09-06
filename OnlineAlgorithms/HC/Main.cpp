#include "HierarchicalClustering.hpp"
#include "Shared/GreedySplit.hpp"
#include "Shared/CommandLineParser.hpp"

#include <iostream>
#include <algorithm>
#include <memory>

using namespace std;

static void setUpAndValidateParser(CommandLineParser& parser){
    parser.addConstraint("-workloads", CommandLineParser::ArgumentType::STRING, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, false,
                         " workloads to load, list of strings");

    parser.addConstraint("-fps", CommandLineParser::ArgumentType::STRING, 1, false,
                         "number of min hash fingerprints (input 'all' for all fps) - int or 'all'");

    parser.addConstraint("-traffic", CommandLineParser::ArgumentType::INT, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, false,
                         "traffic - list of int");

    parser.addConstraint("-wt_list", CommandLineParser::ArgumentType::INT, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES,true,
                         "list of W_T to try - list of int, default is 0, 20, 40, 60, 100");

    parser.addConstraint("-lb", CommandLineParser::ArgumentType::BOOL,0, true,
                         "use load balancing (optional, default false)");

    parser.addConstraint("-converge_margin", CommandLineParser::ArgumentType::BOOL,0, true,
                         "use converging margin (optional, default false)");

    parser.addConstraint("-use_new_dist_metric", CommandLineParser::ArgumentType::BOOL,0, true,
                         "use the new dist metric (optional, default false)");

    parser.addConstraint("-carry_traffic", CommandLineParser::ArgumentType::BOOL,0, true,
                         "carry traffic across epochs (optional, default false)");

    parser.addConstraint("-seed", CommandLineParser::ArgumentType::INT, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, false,
                         "seeds for the algorithm, list of int");

    parser.addConstraint("-gap", CommandLineParser::ArgumentType::DOUBLE, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, false,
                         "pick random results value from result values in this gap - list of double");

    parser.addConstraint("-lb_sizes", CommandLineParser::ArgumentType::DOUBLE, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, true,
                         "a list of clusters' requested sizes - list of int, must sum to 100 (optional, default is even distribution)");

    parser.addConstraint("-eps", CommandLineParser::ArgumentType::INT, 1, true,
                         "% to add to system's initial size at every iteration - int (mandatory if -lb is used, default is 5)");

    parser.addConstraint("-output_path_prefix", CommandLineParser::ArgumentType::STRING, CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, true,
                         "prefix of output path (optional, default is results/result in the working directory)");

    parser.addConstraint("-result_sort_order", CommandLineParser::ArgumentType::STRING,  CommandLineParser::VARIABLE_NUM_OF_OCCURRENCES, true,
                         "sort order for the best result selection. use the following literals: "
                         "(traffic_valid, lb_valid, deletion, lb_score, traffic). The default sort order is "
                         "'traffic_valid lb_valid deletion lb_score traffic'");

    parser.addConstraint("-no_cache", CommandLineParser::ArgumentType::BOOL, 0, true,
                         "flag for not using cache to calculate cost");

    parser.addConstraint("-cache_path", CommandLineParser::ArgumentType::STRING, 1, true,
                         "path to cost's cache (default is cache.db)");

    parser.addConstraint("-num_iterations", CommandLineParser::ArgumentType::INT, 1, true,
                         "num of epochs in a migration window (default=1 epoch)");

    parser.addConstraint("-num_changes_iterations", CommandLineParser::ArgumentType::INT, 1, true,
                         "num of epochs with system changes in a migration window, default is 0");

    parser.addConstraint("-changes_input_file", CommandLineParser::ArgumentType::STRING, 1, true,
                         "changes input file, default is \"\"");

    parser.addConstraint("-change_pos", CommandLineParser::ArgumentType::STRING, 1, false,
                         "Online algorithm to simulate. (previously called Change pos). that should be one of: migration_after_changes (post),"
                         "migration_before_changes (pre), migration_with_continuous_changes (multiple), "
                         "naive_split (space-split), lb_split (balance-split), smart_split (slide), or only_changes");

    parser.addConstraint("-changes_seed", CommandLineParser::ArgumentType::INT, 1, true,
                         "changes seed, default is 22");

    parser.addConstraint("-changes_perc", CommandLineParser::ArgumentType::INT, 1, true,
                         "num of changes as perc from num files in system, default is 0%");

    parser.addConstraint("-num_runs", CommandLineParser::ArgumentType::INT, 1, true,
                         "num of running the experiment one after another");

    parser.addConstraint("-files_index_path", CommandLineParser::ArgumentType::STRING, 1, true,
                         "path to files index (output file of volumes_split_creator.py with the flag of --output_filter_snaps_path)");

    parser.addConstraint("-changes_insert_type", CommandLineParser::ArgumentType::STRING, 1, true,
                         "changes insert type, default is random. options are random/backup");

    parser.addConstraint("-split_sort_order", CommandLineParser::ArgumentType::STRING, 1, true,
                         "split transfer sort order, default is hard_deletion. options are hard_deletion, soft_deletion,"
                         " hard_lb and soft_lb. hard_lb is the option used for Slide and Balance split");

    try {
        parser.validateConstraintsHold();
    }catch (const exception& e){
        parser.printUsageAndDescription();
        throw;
    }
}

static HierarchicalClustering::resultCompareFunc getCompareFuncFromStr(const string & str){
    if(str == "traffic_valid")
        return HierarchicalClustering::ClusteringResult::compareTrafficVaild;

    if(str == "lb_valid")
        return HierarchicalClustering::ClusteringResult::compareLbVaild;

    if(str == "deletion")
        return HierarchicalClustering::ClusteringResult::compareDeletion;

    if(str == "lb_score")
        return HierarchicalClustering::ClusteringResult::compareLbScore;

    if(str == "traffic")
        return HierarchicalClustering::ClusteringResult::compareTraffic;

    throw invalid_argument("The given sort order is not valid. str=" + str + " is not known");
}

static void validateAndFillSortOrder(const CommandLineParser& parser){

    static const vector<HierarchicalClustering::resultCompareFunc> DEFAULT_SORT_ORDER = {
            HierarchicalClustering::ClusteringResult::compareTrafficVaild,
            HierarchicalClustering::ClusteringResult::compareLbVaild,
            HierarchicalClustering::ClusteringResult::compareDeletion,
            HierarchicalClustering::ClusteringResult::compareLbScore,
            HierarchicalClustering::ClusteringResult::compareTraffic
    };

    if(!parser.isTagExist("-result_sort_order")){
        HierarchicalClustering::ClusteringResult::sort_order = DEFAULT_SORT_ORDER;
        return;
    }

    HierarchicalClustering::ClusteringResult::sort_order = vector<HierarchicalClustering::resultCompareFunc>();
    for(const string& sort_name : parser.getTag("-result_sort_order"))
        HierarchicalClustering::ClusteringResult::sort_order.emplace_back(getCompareFuncFromStr(sort_name));
}

static int validateAndGetNumOfIterations(const CommandLineParser& parser){
    static constexpr int DEFAULT_NUM_ITERATIONS = 1;
    if(!parser.isTagExist("-num_iterations"))
        return DEFAULT_NUM_ITERATIONS;

    const vector<string> current_arg = parser.getTag("-num_iterations");
    return stoi(current_arg.front());
}

static int validateAndGetNumOfChangesIterations(const CommandLineParser& parser){
    static constexpr int DEFAULT_NUM_ITERATIONS = 0;
    if(!parser.isTagExist("-num_changes_iterations"))
        return DEFAULT_NUM_ITERATIONS;

    const vector<string> current_arg = parser.getTag("-num_changes_iterations");
    return stoi(current_arg.front());
}

static int validateAndGetChangesSeed(const CommandLineParser& parser){
    static constexpr int DEFAULT_SEED = 22;
    if(!parser.isTagExist("-changes_seed"))
        return DEFAULT_SEED;

    const vector<string> current_arg = parser.getTag("-changes_seed");
    return stoi(current_arg.front());
}


static int validateAndGetChangesPerc(const CommandLineParser& parser){
    static constexpr int DEFAULT_CHANGE = 0;
    if(!parser.isTagExist("-changes_perc"))
        return DEFAULT_CHANGE;

    const vector<string> current_arg = parser.getTag("-changes_perc");
    return stoi(current_arg.front());
}

static int validateAndGetNumRuns(const CommandLineParser& parser){
    static constexpr int DEFAULT_NUM_RUNS = 1;
    if(!parser.isTagExist("-num_runs"))
        return DEFAULT_NUM_RUNS;

    const vector<string> current_arg = parser.getTag("-num_runs");
    return stoi(current_arg.front());
}


static std::string validateAndGetFilesIndexFile(const CommandLineParser& parser){
    static const std::string DEFAULT_INDEX_PATH = "<a default path to files index in a json format>";
    if(!parser.isTagExist("-files_index_path"))
        return DEFAULT_INDEX_PATH;

    const vector<string> current_arg = parser.getTag("-files_index_path");
    return current_arg.front();
}

static std::string validateAndGetChangesInputFile(const CommandLineParser& parser){
    if(!parser.isTagExist("-changes_input_file"))
        return "";

    const vector<string> current_arg = parser.getTag("-changes_input_file");
    return current_arg.front();
}

static AlgorithmDSManager::ChangeType validateAndGetChangesType(const CommandLineParser& parser){
    if(!parser.isTagExist("-changes_insert_type"))
        return AlgorithmDSManager::ChangeType::RANDOM_INSERT;

    const std::string current_arg = parser.getTag("-changes_insert_type").front();
    if(current_arg == "random")
        return AlgorithmDSManager::ChangeType::RANDOM_INSERT;
    else if(current_arg == "backup")
        return AlgorithmDSManager::ChangeType::BACKUP_INSERT;
    else
        throw std::invalid_argument("Not supported change type: " + current_arg);
}

static std::string validateAndGetChangePos(const CommandLineParser& parser){
    const vector<string> current_arg = parser.getTag("-change_pos");
    std::string res = current_arg.front();
    if(res!="migration_after_changes" && res != "migration_before_changes" &&
        res != "migration_with_continuous_changes" && res != "only_changes" && res != "smart_split"&& res != "naive_split" && res != "lb_split"){
        throw std::invalid_argument("change pos is not valid! it should be one of those: migration_after_changes,"
                                    "migration_before_changes, migration_with_continuous_changes, naive_split, lb_split or only_changes");
    }

    return res;
}

static GreedySplit::TransferSort validateAndSplitSortOrder(const CommandLineParser& parser){
    if(!parser.isTagExist("-split_sort_order"))
        return GreedySplit::TransferSort::SOFT_LB;
    const vector<string> current_arg = parser.getTag("-split_sort_order");
    if(current_arg.front()=="hard_deletion"){
        return GreedySplit::HARD_DELETION;
    }

    if(current_arg.front()=="soft_deletion"){
        return GreedySplit::SOFT_DELETION;
    }

    if(current_arg.front()=="soft_lb"){
        return GreedySplit::SOFT_LB;
    }

    if(current_arg.front()=="hard_lb"){
        return GreedySplit::HARD_LB;
    }

    throw std::invalid_argument("change type is not valid! it should be one of those: hard_deletion,"
                                    "soft_deletion, hard_lb or soft_lb");
}

static vector<string> validateAndGetSortedWorkloadsPaths(const CommandLineParser& parser){
    vector<string> workloads_paths = parser.getTag("-workloads");

    //sort in lexicographic order for convenience
    sort(workloads_paths.begin(), workloads_paths.end(), [](string &a, string &b) {
        return Utility::splitString(a, '/').back() < Utility::splitString(b, '/').back();
    });

    //check files exist
    for(const string& path : workloads_paths){
        if (!Utility::isFileExists(path))
            throw invalid_argument("The given workload's file path=" + path+ " doesn't exist");
    }

    return workloads_paths;
}

static int validateAndGetRequestedNumOfFingerprints(const CommandLineParser& parser){

    const vector<string> current_arg = parser.getTag("-fps");
    try {
        return stoi(current_arg.front());
    } catch (invalid_argument &e) {
        if (current_arg.front() != "all")
            throw invalid_argument("Please enter a valid fps_size");

        //default is set to 'all'
        static constexpr int ALL_FINGERPRINTS = -1;
        return ALL_FINGERPRINTS;
    }
}

static vector<double> validateAndGetWTs(const CommandLineParser& parser){

    vector<double> wts;
    if(!parser.isTagExist("-wt_list"))
        return {20, 40, 60, 100};
    for(const string& wt: parser.getTag("-wt_list")){
        try {
            const long long int converted_wt = stoll(wt);
            if(converted_wt < 0 || converted_wt > 100)
                throw invalid_argument("wt value should be between 0 to 100");

            wts.emplace_back(converted_wt);
        } catch (invalid_argument &e) {
            throw invalid_argument("wt value should be between 0 to 100");
        }
    }

    return wts;
}

static vector<double> validateAndGetTraffics(const CommandLineParser& parser){

    vector<double> traffics;

    for(const string& traffic: parser.getTag("-traffic")){
        try {
            const long long int converted_traffic = stoll(traffic);
            if(converted_traffic < 0)
                throw invalid_argument("Traffic value should be higher than 0");

            traffics.emplace_back(converted_traffic);
        } catch (invalid_argument &e) {
            throw invalid_argument("Traffic value should be between 0 to 100");
        }
    }

    if(traffics.size() != 1){
        throw invalid_argument("only one traffic per process is currently supported");
    }

    return traffics;
}

static int validateAndGetEps(const CommandLineParser& parser){
    if (parser.isTagExist("-eps"))
        return stoi(parser.getTag("-eps").front());

    static constexpr int EPS_DEFAULT = 5;
    return EPS_DEFAULT;
}

static int validateAndGetMargin(const CommandLineParser& parser){
    if (parser.isTagExist("-margin"))
        return stoi(parser.getTag("-margin").front());

    static constexpr int MARGIN_DEFAULT = 5;
    return MARGIN_DEFAULT;
}

static vector<int> validateAndGetSeeds(const CommandLineParser& parser){
    vector<int> seeds;
    for(const string& seed: parser.getTag("-seed"))
        seeds.emplace_back(stoi(seed));

    return seeds;
}

static vector<double> validateAndGetGaps(const CommandLineParser& parser){
    vector<double> gaps;
    for(const string& gap: parser.getTag("-gap"))
        gaps.emplace_back(stod(gap));

    return gaps;
}

static vector<double> validateAndGetSortedLbSizes(const CommandLineParser& parser, const int number_of_clusters){
    if (!parser.isTagExist("-lb_sizes"))
        return vector<double>(number_of_clusters, 100.0/number_of_clusters);

    vector<double> lb_sizes;
    for(const string& lb_size : parser.getTag("-lb_sizes")){
        try {
            const double converted_lb_size = stod(lb_size);
            if(converted_lb_size < 0 || converted_lb_size > 100)
                throw invalid_argument("lb size values should be between 0 to 100");

            lb_sizes.emplace_back(converted_lb_size);
        } catch (invalid_argument &e) {
            throw invalid_argument("lb size values should be between 0 to 100");
        }
    }

    sort(lb_sizes.begin(), lb_sizes.end(), greater<double>());

    return lb_sizes;
}

static void createDirsInPrefix(const vector<string>& splitted_prefix){
    string accumulated_prefix;

    for(int i=0; i < splitted_prefix.size() -1; ++i){
        if(splitted_prefix[i].size() ==0)
            continue;

        accumulated_prefix += splitted_prefix[i] + "/";
        Utility::createDir(accumulated_prefix);
    }
}

static string validateAndGetCachePath(const CommandLineParser& parser){
    static const string DEFAULT_OUTPUT_PATH_PREFIX = "results/cache.db";
    string output_prefix = DEFAULT_OUTPUT_PATH_PREFIX;
    if(parser.isTagExist("-cache_path"))
        output_prefix = parser.getTag("-cache_path").front();

    createDirsInPrefix(Utility::splitString(output_prefix, '/'));

    return std::move(output_prefix);
}

static string validateAndGetOutputPath(const CommandLineParser& parser){
    static const string DEFAULT_OUTPUT_PATH_PREFIX = "results/result";
    string output_prefix = DEFAULT_OUTPUT_PATH_PREFIX;
    if(parser.isTagExist("-output_path_prefix"))
        output_prefix = parser.getTag("-output_path_prefix").front();

    createDirsInPrefix(Utility::splitString(output_prefix, '/'));

    return std::move(output_prefix);
}

/**
 * 1. -workloads: workloads to load, list of strings
 * 2. -fps: number of min hash fingerprints (input 'all' for all fps) - int or 'all'
 * 3. -traffic: traffic - list of int
 * 4. -wt_list: list of W_T to try - list of int, default is 0, 20, 40, 60, 100
 * 5. -lb: use load balancing (optional, default false)
 * 6. -seed: seeds for the algorithm, list of int
 * 7. -gap: pick random value from values in this gap - list of double
 * 8. -lb_sizes: a list of clusters' requested sizes - list of int, must sum to 100 (optional, default is even distribution)
 * 9. -eps: % to add to system's initial size at every iteration - int (mandatory if -lb is used, default is 5)
 * 10. -output_path_prefix: output path prefix (optional, default is results/result in the working directory)
 * 11. -no_cache: flag for not using case to calculate cost
 * 12. -num_iterations: num of incremental clustering iterations, default is 1
 * 13. -cache_path: path to cost's cache (default is cache.db)
 * 14. -result_sort_order: "sort order for the best result. use the following literals:
 *                          (traffic_valid, lb_valid, deletion, lb_score, traffic). The default sort order is
 *                          'traffic_valid lb_valid deletion lb_score traffic'"
 */
int main(int argc, char **argv) {
    try {
        CommandLineParser parser(argc, argv);
        setUpAndValidateParser(parser);

        //parsing arguments
        const bool load_balance = parser.isTagExist("-lb");
        const bool use_cache = !parser.isTagExist("-no_cache");
        const int num_iterations = validateAndGetNumOfIterations(parser);
        const vector<string> workloads_paths = validateAndGetSortedWorkloadsPaths(parser);
        const int requested_number_of_fingerprints = validateAndGetRequestedNumOfFingerprints(parser);
        const int eps = validateAndGetEps(parser);
        const int margin = validateAndGetMargin(parser);
        const string output_path_prefix = validateAndGetOutputPath(parser);
        const string cache_path = validateAndGetCachePath(parser);
        const double traffic = validateAndGetTraffics(parser).front();
        const vector<double> wts = validateAndGetWTs(parser);
        const vector<int> seeds = validateAndGetSeeds(parser);
        const vector<double> gaps = validateAndGetGaps(parser);
        const vector<double> lb_sizes = validateAndGetSortedLbSizes(parser, workloads_paths.size());

        const int num_changes_iterations = validateAndGetNumOfChangesIterations(parser);
        const std::string change_pos = validateAndGetChangePos(parser);
        const int change_seed = validateAndGetChangesSeed(parser);
        const int changes_perc = validateAndGetChangesPerc(parser);
        const std::string changes_input_file = validateAndGetChangesInputFile(parser);
        const std::string files_index_path = validateAndGetFilesIndexFile(parser);
        const AlgorithmDSManager::ChangeType change_type = validateAndGetChangesType(parser);
        const int num_runs = validateAndGetNumRuns(parser);
        const GreedySplit::TransferSort split_sort_order = validateAndSplitSortOrder(parser);
        const bool is_converge_margin = parser.isTagExist("-converge_margin");
        const bool use_new_dist_metric = parser.isTagExist("-use_new_dist_metric");
        const bool carry_traffic = parser.isTagExist("-carry_traffic");

        validateAndFillSortOrder(parser);

        //init matrices
        unique_ptr<AlgorithmDSManager> DSManager = make_unique<AlgorithmDSManager>(
                workloads_paths, requested_number_of_fingerprints, num_changes_iterations, change_seed, changes_perc,
                changes_input_file, files_index_path, load_balance, change_type, num_runs);

        //run HC
        HierarchicalClustering HC(DSManager, lb_sizes);
        HC.run(workloads_paths, change_pos, num_changes_iterations, load_balance, use_cache, cache_path, margin, eps,
               traffic, wts, seeds, gaps, num_iterations, output_path_prefix, num_runs,
               is_converge_margin, use_new_dist_metric, split_sort_order, carry_traffic);

        return EXIT_SUCCESS;
    }catch (const exception& e){
        cerr << "Got exception: "<< e.what()<< endl;
        exit(EXIT_FAILURE);
    }
}
