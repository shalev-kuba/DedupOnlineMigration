import argparse
import os
import re


def get_files_by_user_dict(input_dir):
    arranged_dict = {}
    for user in os.listdir(input_dir):
        user_dir = os.path.join(input_dir, user)
        user_files = list(os.listdir(user_dir))
        arranged_dict[int(user)] = user_files

    return arranged_dict


def get_info_by_file_name(file_name):
    FILE_NAME_PATTERN = r'U(.*)_H(.*)_IF(.*)_TS(.*)_(.*)_(.*)_(.*)_(.*)'
    FILE_NAME_INDEX = 0
    USER_INDEX = 1
    HOST_INDEX = 2

    regex_groups = re.search(FILE_NAME_PATTERN, file_name)

    return {
        'file': regex_groups[FILE_NAME_INDEX],
        'user': regex_groups[USER_INDEX],
        'host': regex_groups[HOST_INDEX],
    }


def get_user_file_info(files_by_user_dict, user):
    files_info = []
    for file in files_by_user_dict[user]:
        files_info.append(get_info_by_file_name(file))

    return files_info


def run():
    args = get_setup_args()
    files_infos_list = []
    hosts_info = {}
    files_by_user_dict = get_files_by_user_dict(args.input_dir)
    for user in files_by_user_dict.keys():
        files_infos_list += get_user_file_info(files_by_user_dict, user)

    for file_info in files_infos_list:
        if int(file_info['host']) not in hosts_info:
            hosts_info[int(file_info['host'])] = {int(file_info['user'])}
        else:
            hosts_info[int(file_info['host'])].add(int(file_info['user']))

    for host in hosts_info.keys():
        if len(hosts_info[host]) > 1:
            print(f"Found host (={host}) with more than 1 user: ({', '.join([str(x) for x in hosts_info[host]])})")

    print(f'host check has finished')


def get_setup_args():
    parser = argparse.ArgumentParser(description='Check whether there is different users with the same host')
    parser.add_argument('--input_dir', dest='input_dir', type=str, default='./',
                        help='Path to the root directory of the parsed traces')

    return parser.parse_args()


if __name__ == "__main__":
    run()
