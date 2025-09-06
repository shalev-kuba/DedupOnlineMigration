import os
import csv
import re

BASE_PATH = '.'
WORKLOADS = {
    'ubc150_5vols_by_user': {
        'volumeNumberStart': 5,
        'volumeNumberEnd': 5,
    },
    # add more workloads here
}

MASKS = ['mask_k13']
MAX_NUM_VOLS = 10
DESIRED_TRAFFICS = [20, 40, 60, 80, 100]
NUM_INCREMENTAL_ITERATIONS = [1, 4]
LOAD_BALANCING = [True]
FILE_NAMES_SHOULD_CONTAIN = ['migration_plan']
REAL_CALC_FILE_NAMES_SHOULD_END_WITH = '.cost'
BASE_PATH = '.'
COST_BASE_PATH = 'to_calc'
TRAFFIC_PATTERN = '_T(.*?)_'
SEED_PATTERN = '_S(.*?)_CT'
CHANGE_LIST_SEED_TYPE_PATTERN = '_CLS(.*?)_NR'
NUM_RUNS_PATTERN = '_NR(.*?)_migration'
CHANGE_ORDER_PATTERN = '_CP(.*?)_CPR'
CHANGE_PER_PATTERN = '_CPR(.*?)_S'
CHANGE_TYPE_PATTERN = '_CT_(.*?)_CLS'
METHOD = 'greedy'
result = {}


HEADERS = ['method',
           'workload',
           'server',
           'volumeNumberStart',
           'volumeNumberEnd',
           'k',
           'num_runs',  # added
           'change_seed',  # added
           'change_list_seed',  # new added
           'change_order',  # added
           'change_perc',  # added
           'change_type',  # added
           'num iterations',
           'num migration iterations',
           'num changes iterations',
           'num iteration',
           'num migration iteration',
           'num changes iteration',
           'is final iteration',
           'max traffic',
           'margin',
           'isLB',
           'elapsedTime(sec)',
           'chosenElapsedTime(sec)',
           'total traffic filter %',
           'total overlapped traffic filter %',
           'total aborted traffic filter %',
           'total block reuse filter %',
           'total deletion filter %',
           'max volume size filter %',  # added
           'max final volume size filter %',  # added
           'max system size filter %',  # added
           'max inbound link traffic filter %',  # added
           'system size filter %',  # added
           'increasing system size %',  # added
           'lb score filter',
           'max traffic peak',  # new added
           'min lb score filter',  # new added
           'is valid traffic',
           'isValidLb',
           'iter system size filter %',  # new added
           'iter max volume size filter %',  # new added
           'iter initial max volume size filter %',  # new added
           'iter final max volume size filter %',  # new added
           'iter max inbound link traffic filter %',  # new added
           'iter is lb valid',  # new added
           'iter lb score filter',  # new added
           'iter traffic % filter',
           'iter del % filter',
           ] + [f'vol{x} size Filter %' for x in range(1, MAX_NUM_VOLS + 1)]

def iteration_to_str(iter_num):
    return f'{iter_num}_iterations'


def is_lb_to_str(is_lb):
    return 'lb' if is_lb else 'no_lb'


def is_valid_name(file_name):
    return all(map(lambda x: x in file_name, FILE_NAMES_SHOULD_CONTAIN))


def get_all_valid_files(dir_path):
    if os.path.isdir(dir_path):
        return [os.path.join(dir_path, file_name) for file_name in list(filter(is_valid_name, os.listdir(dir_path)))]
    else:
        print(f'WARNING: {dir_path} is missing')
        return []


def get_all_cost_valid_files(dir_path):
    if os.path.isdir(dir_path):
        return [os.path.join(dir_path, file_name) for file_name in list(filter(is_valid_name, os.listdir(dir_path))) if
                file_name.endswith(REAL_CALC_FILE_NAMES_SHOULD_END_WITH)]
    else:
        # print(f'WARNING: {dir_path} is missing')
        return []


def get_traffic_splitted_file(files_list):
    traffic_to_file = {'real_cost': {}, 'masked_cost': {}}
    for file_name in files_list:
        traffic = int(float(re.search(TRAFFIC_PATTERN, file_name).group(1)))
        if traffic not in DESIRED_TRAFFICS:
            continue
        if file_name.endswith(REAL_CALC_FILE_NAMES_SHOULD_END_WITH):
            current_list = traffic_to_file['real_cost'].get(re.search(TRAFFIC_PATTERN, file_name).group(1), [])
            current_list.append(file_name)
            traffic_to_file['real_cost'][re.search(TRAFFIC_PATTERN, file_name).group(1)] = current_list
        else:
            current_list = traffic_to_file['masked_cost'].get(re.search(TRAFFIC_PATTERN, file_name).group(1), [])
            current_list.append(file_name)
            traffic_to_file['masked_cost'][re.search(TRAFFIC_PATTERN, file_name).group(1)] = current_list

    return traffic_to_file


def get_cost_info_per_iter(csv_lines, num_of_end_vols):
    results = dict(iters_lb_score=[], iters_deletions=[], iters_traffics=[], iters_vols_res=[], iter_params=[],
                   summary_results=[], iters_is_lb_valid=[])

    iter_res_line_index = 0
    PARAMS_HEADER_TO_VALUE_OFFSET = 1
    ITER_TOTAL_CONSTANT_HEADER_OFFSET = 6
    ITER_CONSTANT_HEADER_OFFSET_TO_RES = 4
    ITER_RES_TRAFFIC_BYTES_INDEX = 13
    ITER_RES_DELETION_BYTES_INDEX = 15
    ITER_RES_LB_SCORE_INDEX = 17
    ITER_RES_IS_LB_VALID = 19
    initial_size_bytes = calc_filter_initial_size(csv_lines[ITER_CONSTANT_HEADER_OFFSET_TO_RES:
                                                            ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols])

    while "Summed results:" not in csv_lines[iter_res_line_index]:
        if len(csv_lines[iter_res_line_index]) == 0:
            iter_res_line_index += 1
            continue
        results['iters_vols_res'].append(csv_lines[iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES:
                                                   iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols])
        results['iter_params'].append(csv_lines[iter_res_line_index + PARAMS_HEADER_TO_VALUE_OFFSET])

        results['iters_traffics'].append(float(
            csv_lines[iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols][
                ITER_RES_TRAFFIC_BYTES_INDEX]) / initial_size_bytes * 100)
        results['iters_deletions'].append(float(
            csv_lines[iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols][
                ITER_RES_DELETION_BYTES_INDEX]) / initial_size_bytes * 100)

        results['iters_lb_score'].append(float(
            csv_lines[iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols][
                ITER_RES_LB_SCORE_INDEX]))
        results['iters_is_lb_valid'].append(bool(int(
            csv_lines[iter_res_line_index + ITER_CONSTANT_HEADER_OFFSET_TO_RES + num_of_end_vols][
                ITER_RES_IS_LB_VALID])))
        iter_res_line_index += ITER_TOTAL_CONSTANT_HEADER_OFFSET + num_of_end_vols

    OFFSET_TO_SUMMARY_VAULE = 2
    results['summary_results'] = csv_lines[iter_res_line_index + OFFSET_TO_SUMMARY_VAULE]
    return results


def get_cost_result_from_file(workload, file_path):
    csv_lines = []
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file)
        csv_lines = list(csv_reader)

    num_of_end_vols = WORKLOADS[workload]['volumeNumberEnd']
    return get_cost_info_per_iter(csv_lines, num_of_end_vols)


def get_files_for_benchmark(benchmark_name):
    benchmark_dict = {}
    benchmark_bash_dir = os.path.join(BASE_PATH, benchmark_name)
    cost_bash_dir = os.path.join(COST_BASE_PATH, benchmark_name)

    for mask in MASKS:
        benchmark_dict[mask] = benchmark_dict.get(mask, {})
        for iteration in NUM_INCREMENTAL_ITERATIONS:
            iteration_str = iteration_to_str(iteration)
            benchmark_dict[mask][iteration_str] = benchmark_dict[mask].get(iteration_str, {})
            for is_lb in LOAD_BALANCING:
                is_lb_str = is_lb_to_str(is_lb)
                specific_path = os.path.join(benchmark_bash_dir, mask, iteration_str, is_lb_str)
                cost_specific_path = os.path.join(cost_bash_dir, mask, iteration_str, is_lb_str)
                benchmark_dict[mask][iteration_str][is_lb_str] = get_traffic_splitted_file(
                    get_all_valid_files(specific_path) + get_all_cost_valid_files(cost_specific_path))

    return benchmark_dict


def get_vol_sizes(cost_result, max_num_vol, num_iter):
    return ['-' if i >= len(cost_result['iters_vols_res'][num_iter]) else cost_result['iters_vols_res'][num_iter][i][12] for i in
            range(max_num_vol)]


def get_real_vol_sizes(cost_result, max_num_vol, num_iter):
    return ['-' if 'iters_vols_res' in cost_result and i >= len(cost_result['iters_vols_res'][num_iter]) else (cost_result['iters_vols_res'][num_iter][i][6] if 'iters_vols_res' in cost_result else '-') for i in
            range(max_num_vol)]


def is_last_iter(cost_result, iteration):
    ITERATION_INDEX = 1
    return iteration == int(cost_result['iter_params'][-1][ITERATION_INDEX].split('_')[0])

def get_filter_max_system_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_system_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_initial_size = calc_filter_initial_size(cost_result['iters_vols_res'][iter])
        iter_final_size = calc_filter_final_size(cost_result['iters_vols_res'][iter])
        max_system_size = max(max_system_size, iter_initial_size, iter_final_size)

    return float(max_system_size) / float(initial_size_bytes) * 100.0

def get_filter_system_size_perc(cost_result, iter):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])

    return float(calc_filter_final_size(cost_result['iters_vols_res'][iter])) / float(initial_size_bytes) * 100.0

def get_filter_max_volume_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_volume_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_initial_max_vol_size = calc_filter_initial_max_vol_size(cost_result['iters_vols_res'][iter])
        iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter])
        max_volume_size = max(max_volume_size, iter_initial_max_vol_size, iter_final_max_vol_size)

    return float(max_volume_size) / float(initial_size_bytes) * 100.0

def get_filter_final_max_volume_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_volume_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter])
        max_volume_size = max(max_volume_size, iter_final_max_vol_size)

    return float(max_volume_size) / float(initial_size_bytes) * 100.0

def get_migration_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_m')[1].split('_')[0])

def get_change_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_c')[1])

def get_total_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_')[0])

def get_filter_min_lb_sore(cost_result):
    min_lb_score = 1.0
    for iter_num in range(len(cost_result['iters_lb_score'])):
        min_lb_score = min(min_lb_score, cost_result['iters_lb_score'][iter_num])

    return min_lb_score

def get_filter_iter_final_system_size_perc(cost_result, num_iter):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_final_size = calc_filter_final_size(cost_result['iters_vols_res'][num_iter])

    return float(iter_final_size) / float(initial_size_bytes) * 100.0

def get_filter_final_max_volume_size_perc_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter_num])
    return float(iter_final_max_vol_size) / float(initial_size_bytes) * 100.0


def get_filter_init_max_volume_size_perc_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_init_max_vol_size = calc_filter_initial_max_vol_size(cost_result['iters_vols_res'][iter_num])
    return float(iter_init_max_vol_size) / float(initial_size_bytes) * 100.0

def get_filter_max_volume_size_perc_in_iter(cost_result, iter_num):
    return max(get_filter_final_max_volume_size_perc_in_iter(cost_result, iter_num),
               get_filter_init_max_volume_size_perc_in_iter(cost_result, iter_num))

def get_filter_max_traffic_peak(cost_result):
    max_iter_traffic = 0
    for iter_num in range(len(cost_result['iters_traffics'])):
        max_iter_traffic = max(max_iter_traffic, cost_result['iters_traffics'][iter_num])

    return max_iter_traffic

def get_filter_max_inbound_traffic_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_inbound_traffic = calc_filter_iter_max_inbound_traffic(cost_result['iters_vols_res'][iter_num])
    return float(max_inbound_traffic) / float(initial_size_bytes) * 100.0

def calc_filter_initial_traffic_bytes(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += float(line[7])*1024

    return initial_size_bytes

def get_filter_max_inbound_traffic_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_inbound_traffic = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_max_traffic = calc_filter_iter_max_inbound_traffic(cost_result['iters_vols_res'][iter])
        max_inbound_traffic = max(max_inbound_traffic, iter_max_traffic)

    return float(max_inbound_traffic) / float(initial_size_bytes) * 100.0


def calc_filter_iter_max_inbound_traffic(relevant_csv_lines):
    initial_inbound_traffic= 0

    for line in relevant_csv_lines:
        initial_inbound_traffic = max(initial_inbound_traffic, int(float(line[7])*1024))

    return initial_inbound_traffic

def get_filter_max_traffic_peak_fix(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])

    max_iter_traffic = 0
    for iter_num in range(len(cost_result['iters_vols_res'])):
        iter_traffic = calc_filter_initial_traffic_bytes(cost_result['iters_vols_res'][iter_num])
        max_iter_traffic = max(max_iter_traffic, iter_traffic)

    return float(max_iter_traffic) / float(initial_size_bytes) * 100.0

def calc_filter_overlap_traffic(relevant_csv_lines):
    overlap_traffic = 0

    for line in relevant_csv_lines:
        overlap_traffic += int(float(line[8])*1024)

    return overlap_traffic


def calc_filter_block_reuse(relevant_csv_lines):
    block_reuse = 0

    for line in relevant_csv_lines:
        block_reuse += int(float(line[9])*1024)

    return block_reuse


def calc_filter_aborted_traffic(relevant_csv_lines):
    aborted_traffic = 0

    for line in relevant_csv_lines:
        aborted_traffic += int(float(line[10])*1024)

    return aborted_traffic

def get_filter_total_overlap_traffic(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    overlap_traffic = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        overlap_traffic += calc_filter_overlap_traffic(cost_result['iters_vols_res'][iter])

    return float(overlap_traffic) / float(initial_size_bytes) * 100.0


def get_filter_total_block_reuse(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    block_reuse = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        block_reuse += calc_filter_block_reuse(cost_result['iters_vols_res'][iter])

    return float(block_reuse) / float(initial_size_bytes) * 100.0


def get_filter_total_aborted_traffic(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    aborted_traffic = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        aborted_traffic += calc_filter_aborted_traffic(cost_result['iters_vols_res'][iter])

    return float(aborted_traffic) / float(initial_size_bytes) * 100.0



def get_result_lines(workload, is_lb, max_traffic, cost_result, real_cost_result, file_path):
    MASK = 13
    PARAMS_SERVER_INDEX = 0
    PARAMS_MARGIN_INDEX = 6
    SUMMARY_RES_TOTAL_ELAPSED_TIME_INDEX = 4
    SUMMARY_RES_CHOSEN_ELAPSED_TIME_INDEX = 5
    SUMMARY_RES_TOTAL_TRAFFIC_INDEX = 1
    SUMMARY_REAL_RES_TOTAL_TRAFFIC_INDEX = 1
    SUMMARY_RES_TOTAL_DELETION_INDEX = 3
    SUMMARY_REAL_RES_TOTAL_DELETION_INDEX = 3
    SUMMARY_RES_LB_SCORE_INDEX = 6
    SUMMARY_RES_IS_VAILD_TRAFFIC_INDEX = 7
    SUMMARY_RES_IS_VAILD_LB_INDEX = 8


    file_name = os.path.basename(file_path)
    change_seed = re.search(SEED_PATTERN, file_name).group(1)
    change_list_seed = re.search(CHANGE_LIST_SEED_TYPE_PATTERN, file_name).group(1)
    num_runs = re.search(NUM_RUNS_PATTERN, file_name).group(1)
    change_order = re.search(CHANGE_ORDER_PATTERN, file_name).group(1)
    change_perc = re.search(CHANGE_PER_PATTERN, file_name).group(1)
    change_type = re.search(CHANGE_TYPE_PATTERN, file_name).group(1)
    dummy_list = ['-' for _ in range(20)]

    lines = [[METHOD if change_order != 'only_changes' else 'None',
              workload,
              cost_result['iter_params'][iteration][PARAMS_SERVER_INDEX],
              WORKLOADS[workload]['volumeNumberStart'],
              WORKLOADS[workload]['volumeNumberEnd'],
              MASK,
              f'num_runs_{num_runs}',  # added
              f'change_seed_{change_seed}',  # added
              f'change_list_seed_{change_list_seed}',  # new added
              change_order,  # added
              f'{change_perc}_changes_perc',  # added
              change_type,  # added
              get_total_iter_from_complex_iter(cost_result['iter_params'][-1][1]),  # num iterations
              get_migration_iter_from_complex_iter(cost_result['iter_params'][-1][1]),  # added
              get_change_iter_from_complex_iter(cost_result['iter_params'][-1][1]),  # added
              iteration + 1,
              get_migration_iter_from_complex_iter(cost_result['iter_params'][iteration][1]),  # added
              get_change_iter_from_complex_iter(cost_result['iter_params'][iteration][1]),  # added
              is_last_iter(cost_result, iteration + 1),  # is_last_iter(cost_result, iteration + 1),
              max_traffic,
              cost_result['iter_params'][iteration][PARAMS_MARGIN_INDEX],
              is_lb,
              cost_result['summary_results'][SUMMARY_RES_TOTAL_ELAPSED_TIME_INDEX],
              cost_result['summary_results'][SUMMARY_RES_CHOSEN_ELAPSED_TIME_INDEX],
              cost_result['summary_results'][SUMMARY_RES_TOTAL_TRAFFIC_INDEX],
              get_filter_total_overlap_traffic(cost_result),
              get_filter_total_aborted_traffic(cost_result),
              get_filter_total_block_reuse(cost_result),
              cost_result['summary_results'][SUMMARY_RES_TOTAL_DELETION_INDEX],
              get_filter_max_volume_size_perc(cost_result),  # 'max volume size filter %', #added
              get_filter_final_max_volume_size_perc(cost_result),  # 'max volume size filter %', #added
              get_filter_max_system_size_perc(cost_result),  # 'max system size filter %', #added
              get_filter_max_inbound_traffic_perc(cost_result),  # 'max inbound link traffic filter %', #added
              get_filter_system_size_perc(cost_result, iteration),  # 'system size filter %', #added
              get_filter_system_size_perc(cost_result, iteration) - 100.0,  # 'system size filter %', #added
              cost_result['summary_results'][SUMMARY_RES_LB_SCORE_INDEX],
              get_filter_max_traffic_peak_fix(cost_result),  # new add 'max traffic peak fix'
              get_filter_min_lb_sore(cost_result),  # new add 'min lb_score'
              cost_result['summary_results'][SUMMARY_RES_IS_VAILD_TRAFFIC_INDEX],
              cost_result['summary_results'][SUMMARY_RES_IS_VAILD_LB_INDEX],
              get_filter_iter_final_system_size_perc(cost_result, iteration),  # iter system size filter % #new added
              get_filter_max_volume_size_perc_in_iter(cost_result, iteration),
              # iter max volume size filter % #new added
              get_filter_init_max_volume_size_perc_in_iter(cost_result, iteration),
              get_filter_final_max_volume_size_perc_in_iter(cost_result, iteration),
              get_filter_max_inbound_traffic_in_iter(cost_result, iteration),
              cost_result['iters_is_lb_valid'][iteration],  # new add 'iter lb score filter'
              cost_result['iters_lb_score'][iteration],  # new add 'iter lb score filter'
              cost_result['iters_traffics'][iteration],
              cost_result['iters_deletions'][iteration]] +
             get_vol_sizes(cost_result, MAX_NUM_VOLS, iteration)
             for iteration in range(len(cost_result['iters_traffics']))]

    return lines


def calc_initial_size(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(float(line[1])*1024)

    return initial_size_bytes


def calc_filter_initial_size(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(float(line[3])*1024)

    return initial_size_bytes

def calc_filter_initial_max_vol_size(relevant_csv_lines):
    initial_max_vol_size = 0

    for line in relevant_csv_lines:
        initial_max_vol_size = max(initial_max_vol_size, int(float(line[3])*1024))

    return initial_max_vol_size

def calc_filter_final_max_vol_size(relevant_csv_lines):
    final_max_vol_size = 0

    for line in relevant_csv_lines:
        final_max_vol_size = max(final_max_vol_size, int(float(line[11])*1024))

    return final_max_vol_size

def calc_filter_final_size(relevant_csv_lines):
    final_size_bytes = 0

    for line in relevant_csv_lines:
        final_size_bytes += int(float(line[11])*1024)

    return final_size_bytes

def sum_deletion_bytes(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(float(line[4])*1024)

    return initial_size_bytes


def sum_traffic_bytes(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(float(line[3])*1024)

    return initial_size_bytes


def get_real_cost_info_per_iter(csv_lines, num_of_end_vols):
    results = dict(iters_deletions=[], iters_traffics=[], iters_vols_res=[], summary_results=[])

    relevant_csv_lines = csv_lines[4:]
    OFFSET_TO_NEXT_ITER = 5
    ITER_RES_TRAFFIC_INDEX = 7
    ITER_RES_DELETION_INDEX = 9

    initial_size_bytes = calc_initial_size(relevant_csv_lines[0:num_of_end_vols])
    summed_traffic = 0
    summed_deletion = 0
    true_line_index = 0
    for line_index in range(len(relevant_csv_lines)):
        if true_line_index > line_index:
            continue

        iter_lines = relevant_csv_lines[line_index:line_index + num_of_end_vols]

        summed_traffic += sum_traffic_bytes(iter_lines)
        summed_deletion += sum_deletion_bytes(iter_lines) - sum_traffic_bytes(iter_lines)

        results['iters_vols_res'].append(iter_lines)
        results['iters_traffics'].append(
            float(relevant_csv_lines[line_index + num_of_end_vols][ITER_RES_TRAFFIC_INDEX]) / initial_size_bytes * 100)
        results['iters_deletions'].append(
            float(relevant_csv_lines[line_index + num_of_end_vols][ITER_RES_DELETION_INDEX]) / initial_size_bytes * 100)
        true_line_index += num_of_end_vols + OFFSET_TO_NEXT_ITER

    results['summary_results'] = [summed_traffic, float(summed_traffic) / initial_size_bytes * 100.0,
                                  summed_deletion, float(summed_deletion) / initial_size_bytes * 100.0]
    return results


def get_real_cost_result_from_file(workload, file_path):
    try:
        csv_lines = []
        with open(file_path) as csv_file:
            csv_reader = csv.reader(csv_file)
            csv_lines = list(csv_reader)

        num_of_end_vols = WORKLOADS[workload]['volumeNumberEnd']
        return get_real_cost_info_per_iter(csv_lines, num_of_end_vols)
    except:
        return {}


def get_lines_for_workload(workload_name):
    workload_files_dict = get_files_for_benchmark(workload_name)
    lines_for_workload = []
    for mask, mask_info in workload_files_dict.items():
        for num_iterations_str, num_iterations_info in mask_info.items():
            for lb_str, lb_info in num_iterations_info.items():
                for max_traffic_str, file_pathes in lb_info['masked_cost'].items():
                    for file_path in file_pathes:
                        try:
                            cost_result_per_iter = get_cost_result_from_file(workload_name, file_path)

                            real_cost_result_per_iter = get_real_cost_result_from_file(workload_name,
                                                                                       workload_files_dict[mask][
                                                                                           num_iterations_str][lb_str][
                                                                                           'real_cost'].get(max_traffic_str))

                            lines_for_workload += get_result_lines(workload_name, lb_str == 'lb', max_traffic_str,
                                                                   cost_result_per_iter, real_cost_result_per_iter, file_path)
                        except Exception as e:
                            print(f"Warning: failed to handle {file_path} error: {e}")
    return lines_for_workload


def main_with_no_max_between_traffic():
    print('Started...')

    all_result = []
    for workload_name in WORKLOADS.keys():
        all_result += get_lines_for_workload(workload_name)

    with open('summarized_results_with_iterations.csv', 'w', newline='') as output_file:
        csv_result = csv.writer(output_file)
        csv_result.writerow(HEADERS)
        csv_result.writerows(all_result)

    print('Finished!')


if __name__ == "__main__":
    main_with_no_max_between_traffic()
