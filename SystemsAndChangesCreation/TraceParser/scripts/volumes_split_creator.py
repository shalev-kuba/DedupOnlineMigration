import argparse
import datetime
import math
import os
import json
import random

from volumes_spliters.general import split_by_num_files_per_volume
from snaps_utils.snaps_filters import filter_one_snap_per_user_per_day, filter_less_than_num_snaps
from snaps_utils.snaps_loader import get_snaps_dict, get_info_by_file_name
from volumes_spliters.by_users_adv import volume_index_by_user_adv
from volumes_spliters.by_users import volume_index_by_user
from volumes_spliters.by_os import volume_index_by_os
from volumes_spliters.by_date import volume_index_by_date

DATE_FORMAT = "%m/%d/%Y %H:%M"
DAYS_THRESHOLD = 3
MIN_NUM_SNAPS = 4
FILTERED_PARSED_TRACES_PATH = './filtered_parsed_traces/'
DEFAULT_SEED = 1010
GEN_DEPTH_LIMIT_DEFAULT = 3
DEFAULT_PERC_NUM_HOST_IN_BACKUP = 10
DEFAULT_NUM_CHANGES_STEPS = 4

SPLIT_METHOD_FUNCTIONS = {
    'user_adv': volume_index_by_user_adv,
    'user': volume_index_by_user,
    'os': volume_index_by_os,
    'date': volume_index_by_date,
}

ORIG_FILES_DEL_ENDED_MARK = dict(added='', deleted='')


def filter_snaps(snaps_dict):
    filter_one_snap_per_user_per_day(snaps_dict, DAYS_THRESHOLD)
    filter_less_than_num_snaps(snaps_dict, MIN_NUM_SNAPS)


def output_volumes(volumes, output_dir):
    for volume in volumes:
        full_path = os.path.join(output_dir, f'vol_{volume.index}.in')
        with open(full_path, 'w') as output_file:
            output_file.write('\n'.join(volume.files_paths))

    print(f'Done write volumes')


def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return repr(obj)


def make_snaps_printable(snaps_dict) -> None:
    for user in snaps_dict.keys():
        for host in snaps_dict[user].keys():
            res = []
            for val in snaps_dict[user][host]:
                res.append(val.asDict())

            snaps_dict[user][host] = res


def run():
    args = get_setup_args()
    snaps_dict, generated_snaps_dict = get_snaps_dict(args.input_dir)
    print("Finish get_snaps_dict")
    filter_snaps(snaps_dict)

    if args.out_filtered_snaps != "":
        make_snaps_printable(snaps_dict)
        with open(args.out_filtered_snaps, "w+") as out_dict:
            print(snaps_dict)
            out_dict.write(json.dumps(snaps_dict, indent=4))
        return

    volumes = split_by_num_files_per_volume(snaps_dict, args.num_volumes, args.num_files_per_volume,
                                            SPLIT_METHOD_FUNCTIONS[args.divide_method],
                                            args.noise_percentages / 100.0, args.balance_system)
    os.system(f'mkdir -p {args.out_dir}')
    output_volumes(volumes, args.out_dir)
    changes = []
    changes_file_name = ''

    num_epochs = f"_{args.bak_num_changes_iters * args.bak_num_runs}_change_epochs" \
        if args.bak_num_changes_iters * args.bak_num_runs != DEFAULT_NUM_CHANGES_STEPS else ""

    if args.changes_type.startswith('random'):
        changes_filtered_hosts = False
        changes_only_add = False
        if args.changes_type == 'random_filter_add_rem':
            changes_file_name = f'changes.seed_{args.seed}{num_epochs}.filter_add_rem.in'
            changes_filtered_hosts = True
        elif args.changes_type == 'random_filter_add':
            changes_file_name = f'changes.seed_{args.seed}{num_epochs}.filter_add.in'
            changes_filtered_hosts = True
            changes_only_add = True
        else:
            raise Exception(f'unsupported type: {args.changes_type}')

        changes = create_dynamic_changes(volumes, generated_snaps_dict, args.seed,
                                         args.gen_depth_limit,
                                         filter_by_hosts=changes_filtered_hosts,
                                         only_addition=changes_only_add)
    elif args.changes_type == 'backup':
        users_prefix = f"_{args.bak_num_hosts_perc}_perc_users" \
            if args.bak_num_hosts_perc != DEFAULT_PERC_NUM_HOST_IN_BACKUP else ""

        changes_file_name = f'changes.seed_{args.seed}' \
                            f'{users_prefix}{num_epochs}.filter_backup.in'
        changes = create_dynamic_backup_changes(volumes, generated_snaps_dict, args.seed, args.gen_depth_limit,
                                                args.bak_num_hosts_perc, args.bak_num_changes_iters)
    else:
        raise Exception(f'unsupported type: {args.changes_type}')

    output_changes(changes, os.path.join(args.changes_output_dir, changes_file_name), args.filtered_parsed_traces)


def get_all_snaps_in_volumes(volumes):
    files_in_volumes = []
    for volume in volumes:
        files_in_volumes += volume.snaps

    return files_in_volumes


def get_all_generation_files(generated_snaps_dict, gen_depth_limit, filter_hosts=None):
    generation_dict = {}

    for user in generated_snaps_dict.keys():
        for host in generated_snaps_dict[user].keys():
            if filter_hosts is not None and host not in filter_hosts:
                continue

            for snap in generated_snaps_dict[user][host]:
                if snap.generation not in generation_dict:
                    generation_dict[snap.generation] = {}

                if snap.user not in generation_dict[snap.generation]:
                    generation_dict[snap.generation][snap.user] = {}

                assert snap.host not in generation_dict[snap.generation][snap.user]

                if snap.generation > gen_depth_limit:
                    continue

                generation_dict[snap.generation][snap.user][snap.host] = snap

    return generation_dict


def get_first_level_gen_files(generation_dict):
    first_level_snaps = []
    FIRST_LEVEL_GEN = 1
    for user in generation_dict[FIRST_LEVEL_GEN].keys():
        for host in generation_dict[FIRST_LEVEL_GEN][user].keys():
            first_level_snaps.append(generation_dict[FIRST_LEVEL_GEN][user][host])

    return first_level_snaps


def do_step(available_snaps_to_add, current_volumes_snaps, generations_dict, only_addition=False):
    is_add = only_addition or (True if len(current_volumes_snaps) == 0 else bool(random.getrandbits(1)))
    add_snap = None
    del_snap = None
    if is_add:
        add_snap = random.choice(available_snaps_to_add)
        available_snaps_to_add.remove(add_snap)
        new_available = generations_dict.get(add_snap.generation + 1, {}).get(
            add_snap.user, {}).get(add_snap.host, None)
        if new_available is not None:
            available_snaps_to_add.append(new_available)
    else:  # remove
        del_snap = random.choice(current_volumes_snaps)
        current_volumes_snaps.remove(del_snap)

    return add_snap, del_snap


def output_changes(changes, changes_output_path, filtered_traces_path):
    with open(changes_output_path, 'w+') as fp:
        for change in changes:
            if change == ORIG_FILES_DEL_ENDED_MARK:
                fp.write("# NO MORE ORIG FILES TO REMOVE\n")
            else:
                fp.write(f'A:{change["added"]},D:{change["deleted"]}\n')

    # output filtered changes
    with open(f'{changes_output_path}.filtered', 'w+') as fp:
        for change in changes:
            if change == ORIG_FILES_DEL_ENDED_MARK:
                fp.write("# NO MORE ORIG FILES TO REMOVE\n")
            else:
                add_path = "" if change["added"] == "" else os.path.join(filtered_traces_path, os.path.basename(
                    os.path.dirname(change["added"])),
                                                                         os.path.basename(change["added"]))
                del_path = "" if change["deleted"] == "" else os.path.join(filtered_traces_path, os.path.basename(
                    os.path.dirname(change["deleted"])),
                                                                           os.path.basename(change["deleted"]))
                fp.write(f'A:{add_path},D:{del_path}\n')


def create_dynamic_changes(volumes, generated_snaps_dict, seed, gen_depth_limit, filter_by_hosts=True,
                           only_addition=False):
    # currently will random changes
    random.seed(seed)
    current_volumes_snaps = get_all_snaps_in_volumes(volumes)
    filter_hosts = {snap.host for snap in current_volumes_snaps}
    formatted_gen_dict = get_all_generation_files(generated_snaps_dict,
                                                  gen_depth_limit,
                                                  filter_hosts if filter_by_hosts else None)
    available_snaps_to_add = get_first_level_gen_files(formatted_gen_dict)

    mark_add_changes_ended = False
    changes = []
    while len(available_snaps_to_add) > 0:
        add_snap, del_snap = do_step(available_snaps_to_add,
                                     current_volumes_snaps,
                                     formatted_gen_dict, only_addition=only_addition)
        changes.append(dict(added=add_snap.full_path if add_snap else '',
                            deleted=del_snap.full_path if del_snap else ''))
        if not mark_add_changes_ended and len(current_volumes_snaps) == 0:
            print("ORIG_FILES_DEL_ENDED_MARK")
            changes.append(ORIG_FILES_DEL_ENDED_MARK)
            mark_add_changes_ended = True

    return changes


def find_oldest_backup(host, current_volumes_snaps):
    host_files = [snap for snap in current_volumes_snaps if snap.host == int(host)]
    host_files.sort(key=lambda snap: snap.date)
    return host_files[0]


def create_dynamic_backup_changes(volumes, generated_snaps_dict, seed, gen_depth_limit, bak_num_hosts_perc,
                                  bak_num_changes_iters):
    random.seed(seed)
    current_volumes_snaps = get_all_snaps_in_volumes(volumes)

    num_hosts_in_backup_stream = int(math.ceil(((len(current_volumes_snaps) * (bak_num_hosts_perc / 100.0))
                                                / bak_num_changes_iters)))

    MINIMUM_NUM_OF_SNAPS = 3

    # Black listed hosts is relevant only to reproduce results, synthetic filed for File 61 take a lot of time to be
    # generated, so in our specific case we discard it as we were short of time. If that's not the case, you can leave
    # this list empty
    BLACK_LISTED_HOSTS_FOR_BACKUP = [61]
    filter_hosts = {snap.host for snap in current_volumes_snaps
                   if len([s for s in current_volumes_snaps if s.host == snap.host and s.generation == 0])
                    > MINIMUM_NUM_OF_SNAPS}

    selected_hosts = random.sample(filter_hosts, num_hosts_in_backup_stream + len(BLACK_LISTED_HOSTS_FOR_BACKUP))

    for black_listed in BLACK_LISTED_HOSTS_FOR_BACKUP:
        if black_listed in selected_hosts:
            selected_hosts.remove(black_listed)

    selected_hosts = selected_hosts[:num_hosts_in_backup_stream]

    formatted_gen_dict = get_all_generation_files(generated_snaps_dict, gen_depth_limit, selected_hosts)

    changes = []
    for gen in sorted(formatted_gen_dict.keys()):
        if gen > gen_depth_limit:
            break
        for user in formatted_gen_dict[gen].keys():
            for host in formatted_gen_dict[gen][user].keys():
                new_snap = formatted_gen_dict[gen][user][host]
                old_snap = find_oldest_backup(host, current_volumes_snaps)
                changes.append(dict(added=new_snap.full_path, deleted=old_snap.full_path))
                current_volumes_snaps.remove(old_snap)
                # Currently we don't start removing generated files for now, that's why the next line is commented
                current_volumes_snaps.append(new_snap)
    print("ORIG_FILES_DEL_ENDED_MARK")
    print(f"num changes= {len(changes)}")

    changes.append(ORIG_FILES_DEL_ENDED_MARK)

    return changes


def get_setup_args():
    parser = argparse.ArgumentParser(description='Create an input files for UnionFsFiles, each file represent volume')
    parser.add_argument('--input_dir', dest='input_dir', type=str, default='./',
                        help='Path to the root directory of the parsed traces')

    parser.add_argument('--filtered_parsed_traces', dest='filtered_parsed_traces', type=str,
                        default=FILTERED_PARSED_TRACES_PATH,
                        help='Path to the root directory of the filtered parsed traces')

    parser.add_argument('--out_dir', dest='out_dir', type=str, default='./',
                        help='Path to the root directory where the output will be written to')

    parser.add_argument('--divide_method', dest='divide_method', type=str, default='user',
                        help='The volumes dividing method')

    parser.add_argument('--num_volumes', dest='num_volumes', type=int, default=5,
                        help='num volumes to divide to')

    parser.add_argument('--balance_system', dest='balance_system', type=bool, default=False,
                        help='balance volumes across system, default is False')

    parser.add_argument('--num_files_per_volume', dest='num_files_per_volume', type=int, default=100,
                        help='num files per volume')

    parser.add_argument('--noise_percentages', dest='noise_percentages', type=float, default=0.0,
                        help='num of percentages of random files in each volumes (adds "noise")')

    parser.add_argument('--output_filter_snaps_path', dest='out_filtered_snaps', default="", type=str,
                        help='If given, the script will just output the filtered snaps dict without creating a system (as called filtered_indexed_input_files.json)')

    parser.add_argument('--changes_output_dir', dest='changes_output_dir', default="./", type=str,
                        help='path to the dir where output the changes')

    parser.add_argument('--changes_type', dest='changes_type', default='random_filter_add_rem', type=str,
                        help='changes type, currently support random_filter_add_rem/random_filter_add/backup')

    parser.add_argument('--seed', dest='seed', default=DEFAULT_SEED, type=int,
                        help='seed for the random')

    parser.add_argument('--gen_depth_limit', dest='gen_depth_limit', default=GEN_DEPTH_LIMIT_DEFAULT, type=int,
                        help='depth limit for gen files')

    parser.add_argument('--bak_num_hosts_perc', dest='bak_num_hosts_perc', default=DEFAULT_PERC_NUM_HOST_IN_BACKUP, type=int,
                        help='perc of num hosts to by participated in backup routine')

    parser.add_argument('--bak_num_changes_iters', dest='bak_num_changes_iters', default=DEFAULT_NUM_CHANGES_STEPS, type=int,
                        help='num of backup iters')
    parser.add_argument('--bak_num_runs', dest='bak_num_runs', default=1, type=int,
                        help='num of runs')
    return parser.parse_args()


if __name__ == "__main__":
    run()
