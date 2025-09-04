#include "Cache.hpp"
#include "ScopeGuard.hpp"

#include <stdexcept>
#include <limits.h>

void sqlite3_close_deleter(sqlite3* ptr) {
    sqlite3_close(ptr);
}

Cache::Cache(const string& cache_path) : m_lock(cache_path + ".lock"), m_db(createDB(cache_path, m_lock))
{
    createTables(m_db);
}

unique_ptr<sqlite3, function<void (sqlite3*)>> Cache::createDB(const string& cache_path, Lock& lock){
    if(!lock.lock())
        throw runtime_error("could not lock database");

    const ScopeGuard lock_guard([&](){
        lock.unlock();
    });

    sqlite3* db_ptr = nullptr;

    const int rc = sqlite3_open(cache_path.c_str(), &db_ptr);
    unique_ptr<sqlite3, function<void (sqlite3*)>> db(db_ptr, sqlite3_close_deleter);

    if(rc != SQLITE_OK)
        throw runtime_error(string("Can't open database: ") + sqlite3_errmsg(db.get()));

    return db;
}

void Cache::createTables(const unique_ptr<sqlite3, function<void (sqlite3*)>>& db){

    string sql_query = "CREATE TABLE IF NOT EXISTS calc_cache(volumes_hash text, init_files_hash text, "
                       "final_files_hash text, Init_sys_size text ,";

    sql_query += createVolumesSpecificHeaders(true);
    sql_query +=  ", PRIMARY KEY(volumes_hash, init_files_hash, final_files_hash))";

    runQuery(sql_query, nullptr, nullptr);

    sql_query = "CREATE TABLE IF NOT EXISTS vol_calc_cache(volume_hash text, init_files_hash text, "
                      "final_files_hash text, ";

    sql_query += createVolumeSpecificHeaders(true);
    sql_query +=  ", PRIMARY KEY(volume_hash, init_files_hash, final_files_hash))";

    runQuery(sql_query, nullptr, nullptr);
}

string Cache::createVolumeSpecificHeaders(const bool is_with_types, const unsigned int index){
    string result = "";
    result+= string("init_vol_size") + to_string(index) + ( is_with_types? " text" : "") +
           ", vol_traffic" + to_string(index) + (is_with_types? " text" : "") +
           ", vol_deletion"+ to_string(index) + (is_with_types? " text" : "") +
           ", vol_received_traffic"+ to_string(index) + (is_with_types? " text" : "") +
           ", vol_overlap_traffic"+ to_string(index) + (is_with_types? " text" : "") +
           ", vol_block_reuse"+ to_string(index) + (is_with_types? " text" : "") +
           ", vol_aborted_traffic"+ to_string(index) + (is_with_types? " text" : "");
    return result;
}

string Cache::createVolumesSpecificHeaders(const bool is_with_types){
    string result = "";
    static constexpr int NUM_OF_VOLUMES = 20;
    for (int i = 1; i <= NUM_OF_VOLUMES; i++) {
        result += createVolumeSpecificHeaders(is_with_types, i) + ", ";
    }

    return result.substr(0, result.length() - 2);
}

void Cache::runQuery(const string& sql_query, CallbackFunc callback, void* callback_params){
    runQuery(*this, sql_query, callback, callback_params);
}

void Cache::runQuery(Cache& cache, const string& sql_query,
                     CallbackFunc callback, void* callback_params){

    if(!cache.m_lock.lock())
        throw runtime_error("could not lock database");

    const ScopeGuard lock_guard([&](){
        cache.m_lock.unlock();
    });

    char *errormsg_ptr = nullptr;

    /* Execute SQL statement */
    const int rc = sqlite3_exec(cache.m_db.get(), sql_query.c_str(), callback, callback_params, &errormsg_ptr);

    unique_ptr<char, function<void (char *)>> errormsg(errormsg_ptr, sqlite3_free);

    if(rc != SQLITE_OK)
        throw runtime_error(string( "SQL error: ") + errormsg.get());
}

int Cache::system_callback(void *data, int argc, char **argv, char **) {
    auto cache_line = reinterpret_cast<SystemCacheLine*>(data);
    cache_line->initial_system_size = stoll(string(argv[3]));

    static constexpr int NUM_OF_VOLUMES = 20;
    if(argc <  4 + 7 * NUM_OF_VOLUMES)
        throw runtime_error( "SQL error: not enough fields in table row of calc_cache");

    for (int i = 4; i < 4 + 7 * NUM_OF_VOLUMES; i += 7) {
        if ((argv[i] && argv[i+1] && argv[i+2]&& argv[i+3]
        && argv[i+4]&& argv[i+5]&& argv[i+6])){
            long long int traffic_vol = stoll(string(argv[i+1]));
            long long int deletion_vol = stoll(string(argv[i+2]));
            long long int receive_bytes = stoll(string(argv[i+3]));
            long long int overlap_traffic = stoll(string(argv[i+4]));
            long long int block_reuse = stoll(string(argv[i+5]));
            long long int aborted_traffic = stoll(string(argv[i+6]));

            cache_line->initial_volumes_sizes.emplace_back(stoll(string(argv[i])));
            cache_line->volumes_traffic.emplace_back(traffic_vol);
            cache_line->volumes_deletion.emplace_back(deletion_vol);
            cache_line->volumes_receive_bytes.emplace_back(receive_bytes);
            cache_line->volumes_overlap_traffic.emplace_back(overlap_traffic);
            cache_line->volumes_block_reuse.emplace_back(block_reuse);
            cache_line->volumes_aborted_traffic.emplace_back(aborted_traffic);
            cache_line->num_bytes_deleted += deletion_vol;
            cache_line->traffic_bytes += traffic_vol;
            cache_line->receive_bytes += receive_bytes;
            cache_line->overlap_traffic += overlap_traffic;
            cache_line->block_reuse += block_reuse;
            cache_line->aborted_traffic += aborted_traffic;
        }
    }

    return EXIT_SUCCESS;
}


int Cache::volume_callback(void *data, int argc, char **argv, char **) {
    auto cache_line = reinterpret_cast<VolCacheLine*>(data);

    constexpr int REQUIRED_FIELDS = 6;
    if(argc < REQUIRED_FIELDS)
        throw runtime_error( "SQL error: not enough fields in table row of vol_calc_cache");

    constexpr int INIT_VOL_SIZE_INDEX = 3;
    constexpr int RECEIVED_BYTES_INDEX = 4;
    constexpr int DELETION_INDEX = 5;

    cache_line->initial_vol_size = stoll(string(argv[INIT_VOL_SIZE_INDEX]));
    cache_line->vol_num_bytes_received = stoll(string(argv[RECEIVED_BYTES_INDEX]));
    cache_line->vol_num_bytes_deleted = stoll(string(argv[DELETION_INDEX]));

    return EXIT_SUCCESS;
}

void Cache::insertSystemResult(const string& volumes, const string& init, const string& final, long long int init_sys_size,
                               const vector<long long int> &init_vol_sizes, const vector<long long int> &vol_traffics,
                               const vector<long long int> &vol_deletions,
                               const vector<long long int> &vol_receive_bytes,
                               const vector<long long int> &vol_overlap_traffic,
                               const vector<long long int> &vol_block_reuse,
                               const vector<long long int> &vol_aborted_traffic){

    string sql_query =  "INSERT OR REPLACE INTO calc_cache(volumes_hash, init_files_hash, final_files_hash,"
                        " Init_sys_size, ";
    sql_query += createVolumesSpecificHeaders();

    sql_query += ") VALUES('" + volumes + "','" + init + "','" + final + "'," + to_string(init_sys_size);

    sql_query += getSpecificVolumesValues(init_vol_sizes, vol_traffics, vol_deletions, vol_receive_bytes,
                                          vol_overlap_traffic, vol_block_reuse, vol_aborted_traffic) + ");";

    runQuery(sql_query, nullptr, nullptr);
}

void Cache::insertVolumeResult(const string& volume, const string& init, const string& final,
                               long long int init_vol_size, long long int vol_traffic, long long int vol_deletion){

    string sql_query =  "INSERT OR REPLACE INTO vol_calc_cache(volume_hash, init_files_hash, final_files_hash, ";
    sql_query += createVolumeSpecificHeaders();

    sql_query += ") VALUES('" + volume + "','" + init + "','" + final + "'";

    sql_query += getSpecificVolumeValues(init_vol_size, vol_traffic, vol_deletion) + ");";

    runQuery(sql_query, nullptr, nullptr);
}

string Cache::getSpecificVolumeValues(long long int init_vol_size, long long int vol_traffic, long long int vol_deletion){
    string result="";

    result += "," + to_string(init_vol_size) + "," +
                  to_string(vol_traffic) + "," +
                  to_string(vol_deletion);

    return std::move(result);
}

string Cache::getSpecificVolumesValues(const vector<long long int> &init_vol_sizes,
                                       const vector<long long int> &vol_traffics,
                                       const vector<long long int> &vol_deletions,
                                       const vector<long long int> &vol_receive_bytes,
                                       const vector<long long int> &vol_overlap_traffic,
                                       const vector<long long int> &vol_block_reuse,
                                       const vector<long long int> &vol_aborted_traffic){
    string result="";
    static constexpr int NUM_OF_VOLUMES = 20;

    for (int i = 0; i < NUM_OF_VOLUMES; i++) {
        if (i >= init_vol_sizes.size())
            result += ",NULL,NULL,NULL,NULL,NULL,NULL,NULL";
        else {
            result += "," + to_string(init_vol_sizes[i ]) + "," +
                    to_string(vol_traffics[i]) + "," +
                    to_string(vol_deletions[i]) + "," +
                    to_string(vol_receive_bytes[i]) + "," +
                    to_string(vol_overlap_traffic[i]) + "," +
                    to_string(vol_block_reuse[i]) + "," +
                    to_string(vol_aborted_traffic[i]);
        }
    }

    return std::move(result);
}

void Cache::insertSystemResult(const string& cache_path, const string& volumes, const string& init, const string& final,
                               long long int init_sys_size, const vector<long long int> &init_vol_sizes,
                               const vector<long long int> &vol_traffics,
                               const vector<long long int> &vol_deletions,
                               const vector<long long int> &vol_receive_bytes,
                               const vector<long long int> &vol_overlap_traffic,
                               const vector<long long int> &vol_block_reuse,
                               const vector<long long int> &vol_aborted_traffic){
    Cache cache(cache_path);
    cache.insertSystemResult(volumes, init, final, init_sys_size, init_vol_sizes, vol_traffics, vol_deletions,
                             vol_receive_bytes,vol_overlap_traffic,vol_block_reuse,vol_aborted_traffic);
}

void Cache::insertVolumeResult(const string& cache_path, const string& volume, const string& init, const string& final,
                               long long int init_vol_size, long long int vol_traffic, long long int vol_deletion){
    Cache cache(cache_path);
    cache.insertVolumeResult(volume, init, final, init_vol_size, vol_traffic, vol_deletion);
}

Cache::SystemCacheLine Cache::getSystemResult(const string& volumes, const string& init, const string& final){
    SystemCacheLine cache_line;

    const string sql_query = "SELECT * FROM calc_cache WHERE volumes_hash='" + volumes + "' AND init_files_hash='" +
            init + "' AND final_files_hash='" + final + "'";

    try {
        runQuery(sql_query, system_callback, static_cast<void*>(&cache_line));
    }catch (const std::runtime_error&){
    }

    return std::move(cache_line);
}

Cache::SystemCacheLine Cache::getSystemResult(const string& cache_path, const string& volumes, const string& init, const string& final){
    Cache cache(cache_path);
    return cache.getSystemResult(volumes, init, final);
}

Cache::VolCacheLine Cache::getVolumeResult(const string& cache_path, const string& volume, const string& init, const string& final){
    Cache cache(cache_path);
    return cache.getVolumeResult(volume, init, final);
}

Cache::VolCacheLine Cache::getVolumeResult(const string& volume, const string& init, const string& final){
    VolCacheLine cache_line;

    const string sql_query = "SELECT * FROM vol_calc_cache WHERE volume_hash='" + volume + "' AND init_files_hash='" +
                             init + "' AND final_files_hash='" + final + "'";

    try {
        runQuery(sql_query, volume_callback, static_cast<void*>(&cache_line));
    }catch (const std::runtime_error&){
    }

    return std::move(cache_line);
}