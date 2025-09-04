import argparse
import os
import subprocess

from multiprocessing import Pool


def get_extraction_command_line(compressed_file_path, extract_cmd):
    return f'{extract_cmd} {compressed_file_path}'


def extract_single_tar(args, input_file_name):
    command_line = get_extraction_command_line(os.path.join(args.input_dir, input_file_name), args.extract_cmd)
    print(f'PID={os.getpid()} Running {command_line}')

    process = subprocess.Popen(command_line.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    print(f'PID={os.getpid()} finished to extract {input_file_name}')


def run():
    args = get_setup_args()

    pool = Pool(args.num_processes)

    processes_results = [pool.apply_async(extract_single_tar, args=(args, trace_file_name))
                         for trace_file_name in os.listdir(args.input_dir)
                         if trace_file_name.endswith(args.files_extension)]

    processes_results = [process_result.get() for process_result in processes_results]

    pool.close()
    pool.join()

    print(f'Extraction of all given files has finished')


def get_setup_args():
    parser = argparse.ArgumentParser(description='Extract compressed files')
    parser.add_argument('--input_dir', dest='input_dir', type=str, default='./',
                        help='Path of the directory where the compressed files are at')

    parser.add_argument('--num_process', dest='num_processes', type=int, default=1,
                        help='num of parallel processes')

    parser.add_argument('--extract_cmd', dest='extract_cmd', type=str, default="tar -xvf",
                        help='The extraction command of a single file')

    parser.add_argument('--files_extension', dest='files_extension', type=str, default=".tar",
                        help='The expected extension of the file to extract')

    return parser.parse_args()


if __name__ == "__main__":
    run()
