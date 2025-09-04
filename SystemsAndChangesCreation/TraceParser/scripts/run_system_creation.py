import argparse
import os
import subprocess

from multiprocessing import Pool


def get_parser_command_line(input_file, output_file, binary_path, filter_on):
    return f'{binary_path} ' \
           f'{input_file} ' \
           f'{output_file} ' \
           f'{1 if filter_on else 0}'


def create_one_volume(args, input_volume_name):
    output_dir = args.input_dir
    if args.is_filter_on:
        output_dir = os.path.join(args.input_dir, 'filtered')
        os.system(f'mkdir -p {output_dir}')

    command_line = get_parser_command_line(os.path.join(args.input_dir, input_volume_name),
                                           os.path.join(output_dir, input_volume_name.replace('.in', '.out')),
                                           args.union_fs_files,
                                           args.is_filter_on)

    print(f'PID={os.getpid()} Running {command_line}')

    process = subprocess.Popen(command_line.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    print(f'PID={os.getpid()} Done running {command_line}')


def create_system_creation_input_file(input_dir):
    input_files = [file for file in os.listdir(input_dir) if file.endswith('.out')]
    input_files.sort()
    system_config_path = os.path.join(input_dir, 'system_config.in')

    with open(system_config_path, 'w') as system_creation_file:
        for input_volume_name in input_files:
            system_creation_file.write(f'{os.path.join(input_dir, input_volume_name)}\n')

    return system_config_path


def create_system(args):
    output_dir = args.input_dir
    if args.is_filter_on:
        output_dir = os.path.join(args.input_dir, 'filtered')

    system_config_path = create_system_creation_input_file(input_dir=output_dir)

    process = subprocess.Popen(f"{args.create_system_elf} {system_config_path}".split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    process.wait()


def run():
    args = get_setup_args()

    if not args.only_system_creation:
        pool = Pool(args.num_processes)

        input_files = [file for file in os.listdir(args.input_dir) if file.endswith('.in')]

        print(f'Input files: {", ".join(input_files)}')

        processes_results = [pool.apply_async(create_one_volume, args=(args, input_volume_name))
                             for input_volume_name in input_files]

        processes_results = [process_result.get() for process_result in processes_results]

        pool.close()
        pool.join()

    create_system(args=args)


def get_setup_args():
    parser = argparse.ArgumentParser(description='Runs trace parser')
    parser.add_argument('--union_fs_files', dest='union_fs_files', type=str, default='./union_fs_files',
                        help='Path to the union_fs_files binary')

    parser.add_argument('--create_system_elf', dest='create_system_elf', type=str, default='./create_system',
                        help='Path to the create_system binary')

    parser.add_argument('--input_dir', dest='input_dir', type=str,
                        help='Path to the dir where all the volumes input file are at')

    parser.add_argument('--num_process', dest='num_processes', type=int, default=1,
                        help='num of parallel processes')

    parser.add_argument('--filter_on', dest='is_filter_on', type=bool, default=True,
                        help='is filter enabled')

    parser.add_argument('--only_system_creation', dest='only_system_creation', type=bool, default=False,
                        help='is only system creation (Volume already exist)')

    return parser.parse_args()


if __name__ == "__main__":
    run()
