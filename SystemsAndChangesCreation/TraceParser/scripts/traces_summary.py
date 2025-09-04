import argparse
import csv

from snaps_utils.snaps_filters import filter_one_snap_per_user_per_day, filter_less_than_num_snaps
from snaps_utils.snaps_loader import get_snaps_dict

HEADERS = ['File name', 'User', 'Host', 'Input File', 'Snapshot Time (US format)', 'User Num Snapshot',
           'User Num Hosts', 'User All Hosts', 'Num Logical Files', 'Num Logical Blocks', 'Num Physical Blocks',
           'Operating System', 'Logical Size']

DATE_FORMAT = "%m/%d/%Y %H:%M"
DAYS_THRESHOLD = 3
MIN_NUM_SNAPS = 4


def get_user_lines(snaps_dict, user):
    user_lines = []
    hosts = set({})
    num_total_snaps = 0
    for host in snaps_dict[user]:
        hosts.add(host)
        num_total_snaps += len(snaps_dict[user][host])

    for host in snaps_dict[user]:
        for snap in snaps_dict[user][host]:
            if snap.user != user:
                print(f"Warning: the file {snap.full_path} seems to belong to {snap.user} "
                      f"but located inside {user} dir")
            user_line = [
                snap.snap_name,
                snap.user,
                snap.host,
                snap.input_file,
                snap.date.strftime(DATE_FORMAT),
                num_total_snaps,
                len(hosts),
                ', '.join(list([str(host) for host in hosts]))
            ]

            if snap.additional_info:
                user_line += [
                    snap.additional_info.num_logical_files,
                    snap.additional_info.num_logical_blocks,
                    snap.additional_info.num_physical_blocks,
                    snap.additional_info.operating_system,
                    snap.additional_info.logical_size,
                ]

            user_lines.append(user_line)

    return user_lines


def filter_snaps(snaps_dict):
    filter_one_snap_per_user_per_day(snaps_dict, DAYS_THRESHOLD)
    filter_less_than_num_snaps(snaps_dict, MIN_NUM_SNAPS)


def run():
    args = get_setup_args()
    snaps_dict = get_snaps_dict(args.input_dir)
    if args.filter_results:
        filter_snaps(snaps_dict)

    lines = []
    for user in sorted(snaps_dict.keys()):
        lines += get_user_lines(snaps_dict, user)

    with open(args.out_file, 'w', newline='') as output_file:
        csv_result = csv.writer(output_file)
        csv_result.writerow(HEADERS)
        csv_result.writerows(lines)

    print(f'Output summary csv file has been written')


def get_setup_args():
    parser = argparse.ArgumentParser(description='Summary parsed traces results')
    parser.add_argument('--input_dir', dest='input_dir', type=str, default='./',
                        help='Path to the root directory of the parsed traces')

    parser.add_argument('--out_file', dest='out_file', type=str, default='./summarized_traces.csv',
                        help='Path to where the output csv will be written to')

    parser.add_argument('--filter_results', dest='filter_results', type=bool, default=False,
                        help='whether or not to filter the results')

    return parser.parse_args()


if __name__ == "__main__":
    run()
