import math
import os
import csv
import re

WORKLOADS = {
    'ubc150_5vols_by_user': {
        'volumeNumberStart': 5,
        'volumeNumberEnd': 5,
    },
    # Add more workloads here
}

MASKS = ['mask_k13']
NUM_RUNS = [1, 2, 4]
MAX_NUM_VOLS = 10
NUM_INCREMENTAL_ITERATIONS = [1, 2, 4, 8]
CHANGE_TYPE = ['filter_backup', 'filter_add', 'filter_add_rem']
CHANGE_SEED = [22, 80, 443, 8080]
CHANGE_LIST_SEEDS = [1010, 999, 7192, 123, 0 , 7979, 2222]
DESIRED_TRAFFICS = [20, 40, 60, 80, 100]
DEFAULT_CHANGE_LIST_SEED = CHANGE_LIST_SEEDS[0]
IS_CHANGE_LIST_SEED_EXISTS = True
PRINT_REAL_COST_MISSING = False
CHANGE_POS = ['migration_with_continuous_changes', 'migration_before_changes', 'migration_after_changes',
              'only_changes', 'smart_split', 'naive_split', 'lb_split']
CHANGES_PERC = [5, 10, 15, 20]
NUM_CHANGES_ITERATIONS = [2, 4, 8]
LOAD_BALANCING = [True]
FILE_NAMES_SHOULD_CONTAIN = ['migration_plan']
REAL_CALC_FILE_NAMES_SHOULD_END_WITH = '.cost'
BASE_PATH = '.'
COST_BASE_PATH = 'to_real_calc'
TRAFFIC_PATTERN = '_T(.*?)_'
METHOD = 'hc'

result = {}

HEADERS = ['method',
           'workload',
           'server',
           'volumeNumberStart',
           'volumeNumberEnd',
           'k',
           'num_runs',
           'change_seed',
           'change_list_seed',
           'change_order',
           'change_perc',
           'change_type',
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
           'max volume size filter %',
           'max final volume size filter %',
           'max system size filter %',
           'max inbound link traffic filter %',
           'system size filter %',
           'increasing system size %',
           'lb score filter',
           'max traffic peak',
           'min lb score filter',
           'is valid traffic',
           'isValidLb',
           'iter system size filter %',
           'iter max volume size filter %',
           'iter initial max volume size filter %',
           'iter final max volume size filter %',
           'iter max inbound link traffic filter %',
           'iter is lb valid',
           'iter lb score filter',
           'iter traffic % filter',
           'iter del % filter',
           ] + [f'vol{x} size Filter %' for x in range(1, MAX_NUM_VOLS + 1)]


def iteration_to_str(iter_num):
    return f'{iter_num}_iterations'


def change_iteration_to_str(iter_num):
    return f'{iter_num}_changes_iterations'


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


def get_traffic_splitted_file(files_list):
    traffic_to_file = {'real_cost': {}, 'masked_cost': {}}
    for file_name in files_list:
        traffic = int(float(re.search(TRAFFIC_PATTERN, file_name).group(1)))
        if traffic not in DESIRED_TRAFFICS:
            continue
        if file_name.endswith(REAL_CALC_FILE_NAMES_SHOULD_END_WITH):
            traffic_to_file['real_cost'][re.search(TRAFFIC_PATTERN, file_name).group(1)] = file_name
        else:
            traffic_to_file['masked_cost'][re.search(TRAFFIC_PATTERN, file_name).group(1)] = file_name

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

    try:
        num_of_end_vols = WORKLOADS[workload]['volumeNumberEnd']
        return get_cost_info_per_iter(csv_lines, num_of_end_vols)
    except Exception as e:
        print(f"Handling of: {file_path} failed wie error: {repr(e)}")
        raise


def get_files_for_benchmark(benchmark_name):
    benchmark_dict = {}
    benchmark_bash_dir = os.path.join(BASE_PATH, benchmark_name)
    cost_bash_dir = os.path.join(COST_BASE_PATH, benchmark_name)

    for mask in MASKS:
        benchmark_dict[mask] = benchmark_dict.get(mask, {})
        for mask in MASKS:
            for num_run in NUM_RUNS:
                num_runs_str = f"num_runs_{num_run}"
                benchmark_dict[num_runs_str] = benchmark_dict.get(num_runs_str, {})
                benchmark_dict[num_runs_str][mask] = benchmark_dict[num_runs_str].get(mask, {})
                for iteration in NUM_INCREMENTAL_ITERATIONS:
                    iteration_str = iteration_to_str(iteration)
                    benchmark_dict[num_runs_str][mask][iteration_str] = benchmark_dict[num_runs_str][mask].get(
                        iteration_str, {})
                    for change_iter in NUM_CHANGES_ITERATIONS:
                        change_iter_str = change_iteration_to_str(change_iter)
                        benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str] = \
                            benchmark_dict[num_runs_str][mask][iteration_str].get(
                                change_iter_str, {})
                        for change_perc in CHANGES_PERC:
                            change_perc_str = f'{change_perc}_changes_perc'
                            benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][change_perc_str] = \
                                benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str].get(change_perc_str,
                                                                                                       {})
                            for change_pos in CHANGE_POS:
                                benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][change_perc_str][
                                    change_pos] = \
                                    benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                        change_perc_str].get(
                                        change_pos, {})
                                for change_type in CHANGE_TYPE:
                                    benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][change_perc_str][
                                        change_pos][
                                        change_type] = \
                                        benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                            change_perc_str][
                                            change_pos].get(
                                            change_type, {})
                                    for change_seed in CHANGE_SEED:
                                        change_seed_str = f'change_seed_{change_seed}'
                                        benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                            change_perc_str][
                                            change_pos][
                                            change_type][change_seed_str] = \
                                            benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                change_perc_str][
                                                change_pos][
                                                change_type].get(change_seed_str, {})

                                        for is_lb in LOAD_BALANCING:
                                            is_lb_str = is_lb_to_str(is_lb)
                                            if not IS_CHANGE_LIST_SEED_EXISTS:
                                                change_list_seed_str = f'change_list_seed_{DEFAULT_CHANGE_LIST_SEED}'
                                                benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                    change_perc_str][
                                                    change_pos][
                                                    change_type][change_seed_str][change_list_seed_str] = \
                                                    benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                        change_perc_str][
                                                        change_pos][
                                                        change_type][change_seed_str].get(change_list_seed_str, {})
                                                specific_path = os.path.join(benchmark_bash_dir, mask, num_runs_str,
                                                                             iteration_str,
                                                                             change_iter_str, change_perc_str,
                                                                             is_lb_str,
                                                                             change_pos, change_type, change_seed_str)
                                                cost_specific_path = os.path.join(cost_bash_dir, mask, num_runs_str,
                                                                                  iteration_str,
                                                                                  change_iter_str, change_perc_str,
                                                                                  is_lb_str,
                                                                                  change_pos, change_type,
                                                                                  change_seed_str)
                                                benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                    change_perc_str][
                                                    change_pos][
                                                    change_type][change_seed_str][change_list_seed_str][
                                                    is_lb_str] = get_traffic_splitted_file(
                                                    get_all_valid_files(specific_path))
                                            else:
                                                for change_list_seed in CHANGE_LIST_SEEDS:
                                                    change_list_seed_str = f'change_list_seed_{change_list_seed}'
                                                    benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                        change_perc_str][
                                                        change_pos][
                                                        change_type][change_seed_str][change_list_seed_str] = \
                                                        benchmark_dict[num_runs_str][mask][iteration_str][
                                                            change_iter_str][
                                                            change_perc_str][
                                                            change_pos][
                                                            change_type][change_seed_str].get(change_list_seed_str, {})
                                                    specific_path = os.path.join(benchmark_bash_dir, mask,
                                                                                 num_runs_str,
                                                                                 iteration_str,
                                                                                 change_iter_str, change_perc_str,
                                                                                 is_lb_str,
                                                                                 change_pos, change_type,
                                                                                 change_seed_str,
                                                                                 change_list_seed_str)
                                                    cost_specific_path = os.path.join(cost_bash_dir, mask,
                                                                                      num_runs_str,
                                                                                      iteration_str,
                                                                                      change_iter_str, change_perc_str,
                                                                                      is_lb_str,
                                                                                      change_pos, change_type,
                                                                                      change_seed_str,
                                                                                      change_list_seed_str)

                                                    benchmark_dict[num_runs_str][mask][iteration_str][change_iter_str][
                                                        change_perc_str][
                                                        change_pos][
                                                        change_type][change_seed_str][change_list_seed_str][
                                                        is_lb_str] = get_traffic_splitted_file(
                                                        get_all_valid_files(specific_path))

    return benchmark_dict


def get_vol_sizes(cost_result, max_num_vol, num_iter):
    return ['-' if i >= len(cost_result['iters_vols_res'][num_iter]) else cost_result['iters_vols_res'][num_iter][i][12]
            for i in
            range(max_num_vol)]


def is_last_iter(cost_result, iteration):
    ITERATION_INDEX = 1
    return iteration == int(cost_result['iter_params'][-1][ITERATION_INDEX].split('_')[0])


def get_migration_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_m')[1].split('_')[0])


def get_change_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_c')[1])


def get_total_iter_from_complex_iter(complex_iter):
    return int(complex_iter.split('_')[0])


def calc_filter_final_size(relevant_csv_lines):
    final_size_bytes = 0

    for line in relevant_csv_lines:
        final_size_bytes += int(float(line[11]))

    return final_size_bytes


def get_filter_system_size_perc(cost_result, iter):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])

    return float(calc_filter_final_size(cost_result['iters_vols_res'][iter])) / float(initial_size_bytes) * 100.0


def calc_filter_initial_max_vol_size(relevant_csv_lines):
    initial_max_vol_size = 0

    for line in relevant_csv_lines:
        initial_max_vol_size = max(initial_max_vol_size, int(float(line[3])))

    return initial_max_vol_size


def calc_filter_iter_max_inbound_traffic(relevant_csv_lines):
    initial_inbound_traffic= 0

    for line in relevant_csv_lines:
        initial_inbound_traffic = max(initial_inbound_traffic, int(float(line[7])))

    return initial_inbound_traffic

def calc_filter_final_max_vol_size(relevant_csv_lines):
    final_max_vol_size = 0

    for line in relevant_csv_lines:
        final_max_vol_size = max(final_max_vol_size, int(float(line[11])))

    return final_max_vol_size


def get_filter_max_volume_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_volume_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_initial_max_vol_size = calc_filter_initial_max_vol_size(cost_result['iters_vols_res'][iter])
        iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter])
        max_volume_size = max(max_volume_size, iter_initial_max_vol_size, iter_final_max_vol_size)

    return float(max_volume_size) / float(initial_size_bytes) * 100.0

def get_filter_max_inbound_traffic_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_inbound_traffic = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_max_traffic = calc_filter_iter_max_inbound_traffic(cost_result['iters_vols_res'][iter])
        max_inbound_traffic = max(max_inbound_traffic, iter_max_traffic)

    return float(max_inbound_traffic) / float(initial_size_bytes) * 100.0

def calc_filter_overlap_traffic(relevant_csv_lines):
    overlap_traffic = 0

    for line in relevant_csv_lines:
        overlap_traffic += int(float(line[8]))

    return overlap_traffic


def calc_filter_block_reuse(relevant_csv_lines):
    block_reuse = 0

    for line in relevant_csv_lines:
        block_reuse += int(float(line[9]))

    return block_reuse


def calc_filter_aborted_traffic(relevant_csv_lines):
    aborted_traffic = 0

    for line in relevant_csv_lines:
        aborted_traffic += int(float(line[10]))

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


def get_filter_final_max_volume_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_volume_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter])
        max_volume_size = max(max_volume_size, iter_final_max_vol_size)

    return float(max_volume_size) / float(initial_size_bytes) * 100.0


def get_filter_final_max_volume_size_perc_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_final_max_vol_size = calc_filter_final_max_vol_size(cost_result['iters_vols_res'][iter_num])
    return float(iter_final_max_vol_size) / float(initial_size_bytes) * 100.0

def get_filter_max_inbound_traffic_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_inbound_traffic = calc_filter_iter_max_inbound_traffic(cost_result['iters_vols_res'][iter_num])
    return float(max_inbound_traffic) / float(initial_size_bytes) * 100.0


def get_filter_init_max_volume_size_perc_in_iter(cost_result, iter_num):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_init_max_vol_size = calc_filter_initial_max_vol_size(cost_result['iters_vols_res'][iter_num])
    return float(iter_init_max_vol_size) / float(initial_size_bytes) * 100.0


def get_filter_max_volume_size_perc_in_iter(cost_result, iter_num):
    return max(get_filter_final_max_volume_size_perc_in_iter(cost_result, iter_num),
               get_filter_init_max_volume_size_perc_in_iter(cost_result, iter_num))


def get_filter_max_system_size_perc(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    max_system_size = 0
    for iter in range(len(cost_result['iters_vols_res'])):
        iter_initial_size = calc_filter_initial_size(cost_result['iters_vols_res'][iter])
        iter_final_size = calc_filter_final_size(cost_result['iters_vols_res'][iter])
        max_system_size = max(max_system_size, iter_initial_size, iter_final_size)

    return float(max_system_size) / float(initial_size_bytes) * 100.0


def get_filter_min_lb_sore(cost_result):
    min_lb_score = 1.0
    for iter_num in range(len(cost_result['iters_lb_score'])):
        min_lb_score = min(min_lb_score, cost_result['iters_lb_score'][iter_num])

    return min_lb_score


def get_filter_max_traffic_peak(cost_result):
    max_iter_traffic = 0
    for iter_num in range(len(cost_result['iters_traffics'])):
        max_iter_traffic = max(max_iter_traffic, cost_result['iters_traffics'][iter_num])

    return max_iter_traffic


def get_filter_max_traffic_peak_fix(cost_result):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])

    max_iter_traffic = 0
    for iter_num in range(len(cost_result['iters_vols_res'])):
        iter_traffic = calc_filter_initial_traffic_bytes(cost_result['iters_vols_res'][iter_num])
        max_iter_traffic = max(max_iter_traffic, iter_traffic)

    return float(max_iter_traffic) / float(initial_size_bytes) * 100.0


def get_filter_iter_initial_system_size_perc(cost_result, num_iter):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_initial_size = calc_filter_initial_size(cost_result['iters_vols_res'][num_iter])

    return float(iter_initial_size) / float(initial_size_bytes) * 100.0


def get_filter_iter_final_system_size_perc(cost_result, num_iter):
    initial_size_bytes = calc_filter_initial_size(cost_result['iters_vols_res'][0])
    iter_final_size = calc_filter_final_size(cost_result['iters_vols_res'][num_iter])

    return float(iter_final_size) / float(initial_size_bytes) * 100.0


def get_result_lines(workload, num_run, is_lb, max_traffic, cost_result,
                     change_seed, change_list_seed, change_order, change_perc,
                     change_type):
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

    dummy_list = ['-' for _ in range(20)]
    lines = [[METHOD if change_order != 'only_changes' else 'None',
              workload,
              cost_result['iter_params'][iteration][PARAMS_SERVER_INDEX],
              WORKLOADS[workload]['volumeNumberStart'],
              WORKLOADS[workload]['volumeNumberEnd'],
              MASK,
              num_run,
              change_seed,
              change_list_seed,
              change_order,
              change_perc,
              change_type,
              get_total_iter_from_complex_iter(cost_result['iter_params'][-1][1]),
              get_migration_iter_from_complex_iter(cost_result['iter_params'][-1][1]),
              get_change_iter_from_complex_iter(cost_result['iter_params'][-1][1]),
              iteration + 1,
              get_migration_iter_from_complex_iter(cost_result['iter_params'][iteration][1]),
              get_change_iter_from_complex_iter(cost_result['iter_params'][iteration][1]),
              is_last_iter(cost_result, iteration + 1),
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
              get_filter_max_volume_size_perc(cost_result),
              get_filter_final_max_volume_size_perc(cost_result),
              get_filter_max_system_size_perc(cost_result),
              get_filter_max_inbound_traffic_perc(cost_result),
              get_filter_system_size_perc(cost_result, iteration),
              get_filter_system_size_perc(cost_result, iteration) - 100.0,
              cost_result['summary_results'][SUMMARY_RES_LB_SCORE_INDEX],
              get_filter_max_traffic_peak_fix(cost_result),
              get_filter_min_lb_sore(cost_result),
              cost_result['summary_results'][SUMMARY_RES_IS_VAILD_TRAFFIC_INDEX],
              cost_result['summary_results'][SUMMARY_RES_IS_VAILD_LB_INDEX],
              get_filter_iter_final_system_size_perc(cost_result, iteration),
              get_filter_max_volume_size_perc_in_iter(cost_result, iteration),
              get_filter_init_max_volume_size_perc_in_iter(cost_result, iteration),
              get_filter_final_max_volume_size_perc_in_iter(cost_result, iteration),
              get_filter_max_inbound_traffic_in_iter(cost_result, iteration),
              cost_result['iters_is_lb_valid'][iteration],
              cost_result['iters_lb_score'][iteration],
              cost_result['iters_traffics'][iteration],
              cost_result['iters_deletions'][iteration]] +
             get_vol_sizes(cost_result, MAX_NUM_VOLS, iteration)
             for iteration in range(len(cost_result['iters_traffics']))]

    return lines


def calc_initial_size(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(line[1])

    return initial_size_bytes


def calc_filter_initial_size(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(line[3])

    return initial_size_bytes


def calc_filter_initial_traffic_bytes(relevant_csv_lines):
    initial_size_bytes = 0

    for line in relevant_csv_lines:
        initial_size_bytes += int(line[7])

    return initial_size_bytes


def get_lines_for_workload(workload_name):
    workload_files_dict = get_files_for_benchmark(workload_name)
    lines_for_workload = []
    for num_run, num_run_info in workload_files_dict.items():
        for mask, mask_info in num_run_info.items():
            for num_iterations_str, num_iterations_info in mask_info.items():
                for num_changes_iterations_str, num_changes_iterations_info in num_iterations_info.items():
                    for change_perc, change_perc_info in num_changes_iterations_info.items():
                        for change_pos, change_pos_info in change_perc_info.items():
                            for change_type, change_type_info in change_pos_info.items():
                                for change_seed, change_seed_info in change_type_info.items():
                                    for change_list_seed, change_list_seed_info in change_seed_info.items():
                                        for lb_str, lb_info in change_list_seed_info.items():
                                            for max_traffic_str, file_path in lb_info['masked_cost'].items():
                                                cost_result_per_iter = get_cost_result_from_file(workload_name,
                                                                                                 file_path)

                                                lines_for_workload += get_result_lines(workload_name,
                                                                                       num_run,
                                                                                       lb_str == 'lb',
                                                                                       max_traffic_str,
                                                                                       cost_result_per_iter,
                                                                                       change_seed,
                                                                                       change_list_seed,
                                                                                       change_pos, change_perc,
                                                                                       change_type)

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
