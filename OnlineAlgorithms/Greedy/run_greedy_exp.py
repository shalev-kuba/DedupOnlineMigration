import subprocess
import sys
import argparse
import json
import os
from itertools import product

from multiprocessing import Pool

DEFAULT_PERC_NUM_HOST_IN_BACKUP = 10
DEFAULT_NUM_CHANGES_STEPS = 3
DEFAULT_TRAFFICS = [20.00, 40.00, 60.00, 80.00, 100.00]
DEFAULT_TIME_LIMIT = 100000
INDEX_FILE = './filtered_indexed_input_files.json'

workload_to_conf_map = {
    "ubc150_5vols_by_user": "configs/conf_exp_ubc150_5vols_by_user.json",
    #add more systems here
}


def get_workload_conf_file(workload_conf):
    with open(workload_to_conf_map[workload_conf], 'r') as conf_file:
        return json.load(conf_file)


def get_greedy_command_line(prefix_dir, experiment_args, traffic, seed, pos, change_list_seed):
    """
    arguments format is: {volumelist} {output} {conclusionFile} {timelimit} {Traffic} {margin} {num_iters} {seed} {changes_file} {file_index} {change_pos} {total_perc_changes}
    """


    users_prefix = f"_{experiment_args.change_perc}_perc_users" \
        if experiment_args.change_perc != DEFAULT_PERC_NUM_HOST_IN_BACKUP else ""
    num_epochs = f"_{experiment_args.num_changes_iterations * experiment_args.num_of_runs}_change_epochs" \
        if experiment_args.num_changes_iterations * experiment_args.num_of_runs != DEFAULT_NUM_CHANGES_STEPS else ""

    out_prefix = f'{prefix_dir}{experiment_args.workload}_T{traffic}_CP{pos}_CPR{experiment_args.change_perc}_S{seed}_CT_{experiment_args.change_prefix}_CLS{change_list_seed}_NR{experiment_args.num_of_runs}'
    return f'{experiment_args.greedy_path} ' \
           f'{prefix_dir}{experiment_args.workload}_{experiment_args.mask}_volumeList ' \
           f'{out_prefix} ' \
           f'{out_prefix}conclusion ' \
           f'{experiment_args.time_limit} ' \
           f'{traffic} ' \
           f'{experiment_args.margin} ' \
           f'{experiment_args.num_iterations} ' \
           f'{experiment_args.num_changes_iterations} ' \
           f'{seed} ' \
           f'{prefix_dir}changes.seed_{change_list_seed}{users_prefix}{num_epochs}.{experiment_args.change_prefix}.in{"" if not experiment_args.is_filtered_changes else ".filtered"} ' \
           f'{INDEX_FILE} ' \
           f'{pos} ' \
           f'{experiment_args.change_perc} ' \
           f'{experiment_args.change_insert_type} ' \
           f'{experiment_args.num_of_runs}'


def create_volume_list_and_dirs(prefix_dir, workload_conf, experiment_args):
    for changes_list_seed in experiment_args.changes_list_seeds:
        os.makedirs(prefix_dir, mode=0o777, exist_ok=True)
        volume_list = workload_conf['mask_to_volumes_names'][f'{experiment_args.mask}']

        users_prefix = f"_{experiment_args.change_perc}_perc_users" \
            if experiment_args.change_perc != DEFAULT_PERC_NUM_HOST_IN_BACKUP else ""
        num_epochs = f"_{experiment_args.num_changes_iterations * experiment_args.num_of_runs}_change_epochs" \
            if experiment_args.num_changes_iterations * experiment_args.num_of_runs != DEFAULT_NUM_CHANGES_STEPS else ""

        changes_path = os.path.join(
            os.path.dirname(os.path.dirname(workload_conf["mask_to_volumes_names"][experiment_args.mask][0])),
            f'changes.seed_{changes_list_seed}{users_prefix}{num_epochs}.{experiment_args.change_prefix}.in')
        changes_path = changes_path if not experiment_args.is_filtered_changes else f'{changes_path}.filtered'

        os.system(f'cp {changes_path} {prefix_dir}changes.seed_{changes_list_seed}{users_prefix}{num_epochs}.{experiment_args.change_prefix}.in'
                  f'{"" if not experiment_args.is_filtered_changes else ".filtered"}')

    with open(f'{prefix_dir}{experiment_args.workload}_{experiment_args.mask}_volumeList', 'w') as vol_file:
        for vol_path in volume_list:
            vol_file.write(f'{vol_path}, a, {1 / len(volume_list)}\n')


def run_in_parallel(workload_conf, experiment_args, traffic, seed, pos, change_list_seed):
    prefix_dir = experiment_args.workload + \
                 f"/mask_{experiment_args.mask}/{experiment_args.num_iterations}_iterations/lb/"
    command_line = get_greedy_command_line(prefix_dir, experiment_args, traffic, seed, pos, change_list_seed)
    print(f'PID={os.getpid()} Running {command_line}')

    process = subprocess.Popen(command_line.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    print(f'PID={os.getpid()} Done running {command_line}')


def get_setup_args():
    parser = argparse.ArgumentParser(description='Runs greedy\'s experiments in parallel')

    parser.add_argument('--mask', dest='mask', type=str, help='which mask to use, for example "k13". default=no_mask',
                        default="no_mask")

    parser.add_argument('--greedy_path', dest='greedy_path', type=str, default='./GreedyLoadBalancerUnited',
                        help='greedy binary location. default is binary named GreedyLoadBalancerUnited in '
                             'the current working directory')

    parser.add_argument('--traffics', dest='traffics', nargs='+', type=float, default=DEFAULT_TRAFFICS,
                        help='traffics for hc. default is' + ' '.join([str(x) for x in DEFAULT_TRAFFICS]))
    parser.add_argument('--time_limit', dest='time_limit', type=int, default=DEFAULT_TIME_LIMIT,
                        help=f'time limit forgreedy. default is {DEFAULT_TIME_LIMIT}')
    parser.add_argument('--margin', dest='margin', type=float, default=0.05,
                        help='desired margin for greedy, default is 0.05')
    parser.add_argument('--num_iterations', dest='num_iterations', type=int, default=1,
                        help='num of incremental iterations, default is 1')
    parser.add_argument('--num_changes_iterations', dest='num_changes_iterations', type=int, default=1,
                        help='num of incremental iterations of changes, default is 1')
    parser.add_argument('--workload', dest='workload', type=str,
                        help='a workload name to run greedy on. one from the following - ' + ', '.join(
                            workload_to_conf_map.keys()))
    parser.add_argument('--seed_list', dest='seed_list', nargs='+', type=int,
                        help='list of seeds')
    parser.add_argument('--changes_list_seeds', dest='changes_list_seeds', nargs='+', type=int,
                        help='list of change list seeds')
    parser.add_argument('--change_pos_list', dest='change_pos_list', nargs='+', type=str,
                        help='where to do changes? only_changes/migration_after_changes/migration_with_continuous_changes/migration_before_changes/(other means no changes at all) '
                             'List means different jobs')
    parser.add_argument('--change_perc', dest='change_perc', type=int,
                        help='num of changes to do as percentages from number of files in system')
    parser.add_argument('--num_of_runs', dest='num_of_runs', type=int, default=1,
                        help='num of times to run experiment one after the other')
    parser.add_argument('--is_filtered_changes', dest='is_filtered_changes', type=bool, default=True,
                        help='is changes are taken from filtered parsed traces')
    parser.add_argument('--change_prefix', dest='change_prefix', type=str,
                        help='prefix of change that represent the change type file to take '
                             'filter_backup/filter_add/filter_add_rem/add/add_rem')
    parser.add_argument('--change_insert_type', dest='change_insert_type', type=str,
                        help='change insert type random/backup')
    return parser.parse_args()


def run_experiment():
    experiment_args = get_setup_args()
    workload_conf = get_workload_conf_file(experiment_args.workload)
    prefix_dir = experiment_args.workload + \
                 f"/mask_{experiment_args.mask}/{experiment_args.num_iterations}_iterations/lb/"
    create_volume_list_and_dirs(prefix_dir, workload_conf, experiment_args)

    combinations = list(product(
        [workload_conf],
        [experiment_args],
        experiment_args.traffics,
        experiment_args.seed_list,
        experiment_args.change_pos_list,
        experiment_args.changes_list_seeds
    ))
    pool = Pool(len(combinations))
    pool.starmap(run_in_parallel, combinations)
    pool.close()
    pool.join()


if __name__ == "__main__":
    run_experiment()
