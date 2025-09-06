import subprocess
import sys
import argparse
import json
import os
from itertools import product
from multiprocessing import Pool

DEFAULT_PERC_NUM_HOST_IN_BACKUP = 10
DEFAULT_NUM_CHANGES_STEPS = 3
DEFAULT_TRAFFICS = [20, 40, 60, 80, 100]
DEFAULT_GAPS = [0.5, 1.0, 3.0]
DEFAULT_SEEDS = [0, 37, 41, 58, 99, 111, 185, 199, 523, 666]
DEFAULT_CHANGE_SEEDS = [22, 80, 443, 8080]
DEFAULT_CHANGE_LIST_SEEDS = [1010, 999, 7192]
CHANGE_TYPE_OPTIONS = ['filter_add', 'filter_add_rem'' filter_backup']
CHANGE_POS_OPTIONS = ['only_changes', 'migration_after_changes', 'migration_before_changes',
                      'migration_with_continuous_changes', 'smart_split', 'naive_split', 'lb_split']
DEFAULT_INDEX_PATH = "filtered_indexed_input_files.json"
DEFAULT_SPLIT_SORT_ORDER = "soft_lb"
workload_to_conf_map = {
    "ubc150_5vols_by_user": "configs/conf_exp_ubc150_5vols_by_user.json",
    "ubc150_5vols_by_random": "configs/conf_exp_ubc150_5vols_by_random.json",
    "ubc150_5vols_by_date": "configs/conf_exp_ubc150_5vols_by_date.json",
}


def get_workload_conf_file(workload_conf):
    with open(workload_to_conf_map[workload_conf], 'r') as conf_file:
        return json.load(conf_file)


def get_hc_command_line(workload_conf, experiment_args, traffic, change_pos, change_seed, change_type,
                        change_list_seed):
    prefix_dir = experiment_args.workload + f"/mask_{experiment_args.mask}/num_runs_{experiment_args.num_of_runs}/" \
                                            f"{experiment_args.num_iterations}_iterations/" \
                                            f"{experiment_args.num_changes_iterations}_changes_iterations/" \
                                            f"{experiment_args.changes_perc}_changes_perc/" \
                                            f"{'no_' if not experiment_args.lb else ''}lb/{change_pos}/" \
                                            f"{change_type}/change_seed_{change_seed}/" \
                                            f"change_list_seed_{change_list_seed}/"

    users_prefix = f"_{experiment_args.changes_perc}_perc_users" \
        if experiment_args.changes_perc != DEFAULT_PERC_NUM_HOST_IN_BACKUP else ""
    num_epochs = f"_{experiment_args.num_changes_iterations * experiment_args.num_of_runs}_change_epochs" \
        if experiment_args.num_changes_iterations * experiment_args.num_of_runs != DEFAULT_NUM_CHANGES_STEPS else ""

    changes_path = os.path.join(
        os.path.dirname(os.path.dirname(workload_conf["mask_to_volumes_names"][experiment_args.mask][0])),
        f'changes.seed_{change_list_seed}{users_prefix}{num_epochs}.{experiment_args.change_type}.in')
    changes_path = changes_path if not experiment_args.is_filtered_changes else f'{changes_path}.filtered'

    return f'{experiment_args.hc_path} ' \
           f'-workloads {" ".join(workload_conf["mask_to_volumes_names"][experiment_args.mask])} ' \
           f'-fps {workload_conf["fps_size"]} ' \
           f'-traffic {traffic} ' \
           f'-eps {experiment_args.eps} ' \
           f'-margin {experiment_args.margin} ' \
           f'-gap {" ".join([str(g) for g in experiment_args.gaps])} ' \
           f'-seed {" ".join([str(s) for s in experiment_args.seeds])} ' \
           f'{"-lb" if experiment_args.lb else ""} ' \
           f'-num_iterations {experiment_args.num_iterations} ' \
           f'{"-lb sizes" + " ".join([str(x) for x in experiment_args.lb_sizes]) if experiment_args.lb_sizes else ""} ' \
           f'{"-no_cache" if not experiment_args.use_cache else ""} ' \
           f'{f"-result_sort_order {experiment_args.result_sort_order}" if experiment_args.result_sort_order else ""} ' \
           f'-cache_path {prefix_dir + "cache.db"} ' \
           f'-output_path_prefix {prefix_dir + experiment_args.workload} ' \
           f'-num_changes_iterations {experiment_args.num_changes_iterations} ' \
           f'-changes_seed {change_seed} ' \
           f'-changes_perc {experiment_args.changes_perc} ' \
           f'-changes_input_file {changes_path} ' \
           f'-changes_insert_type {experiment_args.changes_insert_type} ' \
           f'-files_index_path {experiment_args.files_index_path} ' \
           f'-change_pos {change_pos} ' \
           f'-num_runs {experiment_args.num_of_runs} ' \
           f'{"-converge_margin" if experiment_args.converge_margin else ""} ' \
           f'{"-use_new_dist_metric" if experiment_args.new_dist_metric else ""} '\
           f'{"-carry_traffic" if experiment_args.carry_traffic else ""} '\
           f'-split_sort_order {experiment_args.split_sort_order}'


def run_in_parallel(workload_conf, experiment_args, traffic, change_seed, change_list_seed):
    command_line = get_hc_command_line(workload_conf, experiment_args, traffic, experiment_args.change_pos,
                                       change_seed, experiment_args.change_type, change_list_seed)

    print(f'PID={os.getpid()} Running {command_line}')

    process = subprocess.Popen(command_line.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    print(f'PID={os.getpid()} Done running {command_line}')


def get_setup_args():
    parser = argparse.ArgumentParser(description='Runs hc\'s experiments in parallel')
    parser.add_argument('--load_balance', dest='lb', action='store_true',
                        help='Used to enable load balancing feature')
    parser.add_argument('--dont_cache', dest='use_cache', action='store_false',
                        help='whether or not to use cache')

    parser.add_argument('--mask', dest='mask', type=str, help='which mask to use, for example "k13". default=no_mask',
                        default="no_mask")

    parser.add_argument('--hc_path', dest='hc_path', type=str, default='./hc',
                        help='hc location. default is binary named hc in the current working directory')

    parser.add_argument('--lb_sizes', dest='lb_sizes', nargs='+', type=float, default=[],
                        help='lb sizes')

    parser.add_argument('--traffics', dest='traffics', nargs='+', type=int, default=DEFAULT_TRAFFICS,
                        help='traffics for hc. default is' + ' '.join([str(x) for x in DEFAULT_TRAFFICS]))

    parser.add_argument('--gaps', dest='gaps', nargs='+', type=float, default=DEFAULT_GAPS,
                        help='gaps for hc. default is' + ' '.join([str(x) for x in DEFAULT_GAPS]))

    parser.add_argument('--seeds', dest='seeds', nargs='+', type=int, default=DEFAULT_SEEDS,
                        help='seeds for hc. default is' + ' '.join([str(x) for x in DEFAULT_SEEDS]))

    parser.add_argument('--result_sort_order', dest='result_sort_order', type=str, default='',
                        help='sort order for the best result. use the following literals (space seperated): '
                             '(traffic_valid, lb_valid, deletion, lb_score, traffic).')

    parser.add_argument('--eps', dest='eps', type=int, default=30, help='eps for hc, default is 30')
    parser.add_argument('--margin', dest='margin', type=int, default=5, help='desired margin for hc, default is 5')
    parser.add_argument('--num_iterations', dest='num_iterations', type=int, default=1,
                        help='num of epochs in a window, default is 1')
    parser.add_argument('--workload', dest='workload', type=str,
                        help='a workload name to run hc on. one from the following - ' + ', '.join(
                            workload_to_conf_map.keys()))

    parser.add_argument('--changes_seeds', dest='changes_seeds', nargs='+', type=int, default=DEFAULT_CHANGE_SEEDS,
                        help='seeds for changes steps. default is' + ' '.join([str(x) for x in DEFAULT_CHANGE_SEEDS]))

    parser.add_argument('--change_type', dest='change_type', type=str, default=CHANGE_TYPE_OPTIONS[0],
                        help=f'change type for migration. Options: {"/".join(CHANGE_TYPE_OPTIONS)}\n'
                             f'default is {CHANGE_TYPE_OPTIONS[0]}')

    parser.add_argument('--change_pos', dest='change_pos', type=str, default=CHANGE_POS_OPTIONS[0],
                        help=f'Online migration algorithm to use (previosuly called change position for migration). '
                             f'Options: {"/".join(CHANGE_POS_OPTIONS)}\n'
                             f'default is {CHANGE_POS_OPTIONS[0]}')

    parser.add_argument('--num_changes_iterations', dest='num_changes_iterations', type=int, default=4,
                        help='num of epochs contains changes, default is 4')

    parser.add_argument('--changes_perc', dest='changes_perc', type=int, default=10,
                        help='num of changes as perc from system size, default is 10 perc')

    parser.add_argument('--is_filtered_changes', dest='is_filtered_changes', type=bool, default=True,
                        help='is changes are taken from filtered parsed traces')

    parser.add_argument('--changes_insert_type', dest='changes_insert_type', type=str, default="random",
                        help='insert type random/backup')

    parser.add_argument('--num_of_runs', dest='num_of_runs', type=int, default=1,
                        help='num of times to continue run the experiment one after another. '
                             'The final state of experiment becomes the initial state for the next run')

    parser.add_argument('--changes_list_seeds', dest='changes_list_seeds', nargs='+', type=int,
                        default=DEFAULT_CHANGE_LIST_SEEDS,
                        help='seeds for changes list. default is' + ' '.join(
                            [str(x) for x in DEFAULT_CHANGE_LIST_SEEDS]))

    parser.add_argument('--files_index_path', dest='files_index_path', type=str, default=DEFAULT_INDEX_PATH,
                        help=f'path to index json file. default is {DEFAULT_INDEX_PATH}. '
                             f'(output file of volumes_split_creator.py with the flag of --output_filter_snaps_path)')
    parser.add_argument('--split_sort_order', dest='split_sort_order', type=str, default=DEFAULT_SPLIT_SORT_ORDER,
                        help=f'split sort order: hard_deletion, soft_deletion, soft_lb, hard_lb. '
                             f'default is {DEFAULT_SPLIT_SORT_ORDER} (used for Slide and Balance-split)')
    parser.add_argument('--converge_margin', dest='converge_margin', action='store_true',
                        help='Used to enable load balancing converging')
    parser.add_argument('--new_dist_metric', dest='new_dist_metric', action='store_true',
                        help='Used to enable new dist metric feature')
    parser.add_argument('--carry_traffic', dest='carry_traffic', action='store_true',
                        help='Used to enabl carry traffic feature')
    parser.set_defaults(load_balance=True, use_cache=True, is_filtered_changes=True,
                        converge_margin=False, new_dist_metric=False, carry_traffic=False)

    return parser.parse_args()


def run_experiment():
    experiment_args = get_setup_args()

    workload_conf = get_workload_conf_file(experiment_args.workload)
    combinations = list(product(
        [workload_conf],
        [experiment_args],
        experiment_args.traffics,
        experiment_args.changes_seeds,
        experiment_args.changes_list_seeds
    ))
    pool = Pool(len(combinations))
    pool.starmap(run_in_parallel, combinations)
    pool.close()
    pool.join()


if __name__ == "__main__":
    run_experiment()
