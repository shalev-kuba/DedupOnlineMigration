import argparse
import os
import subprocess
from multiprocessing import Pool


def get_parser_command_line(input_file, args):
    return f'{args.trace_parser_path} ' \
           f'{input_file} ' \
           f'{args.out_traces_path}'


def run_specific_path(specific_input_file: str, args):
    command_line = get_parser_command_line(os.path.join(args.input_traces_path, specific_input_file), args)
    print(f'PID={os.getpid()} Running {command_line}')

    process = subprocess.Popen(command_line.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(f'PID={os.getpid()} error={error} output={output}')
    print(f'PID={os.getpid()} Done running {command_line}')


def run():
    args = get_setup_args()
    files = list(os.listdir(args.input_traces_path))
    pool = Pool(args.num_processes)
    processes_results = [pool.apply_async(run_specific_path, args=(input_file, args))
                         for input_file in files]

    processes_results = [process_result.get() for process_result in processes_results]

    pool.close()
    pool.join()


def get_setup_args():
    parser = argparse.ArgumentParser(description='Runs trace parser')
    parser.add_argument('--trace_parser_path', dest='trace_parser_path', type=str, default='./trace_parser',
                        help='trace_parser location. default is binary named trace_parser'
                             ' in the current working directory')

    parser.add_argument('--traces_dir_path', dest='input_traces_path', type=str, default='',
                        help='path to the dir where the input traces are located')

    parser.add_argument('--traces_out_dir_path', dest='out_traces_path', type=str, default='',
                        help='path to the dir where the outputed traces will be')

    parser.add_argument('--num_processes', dest='num_processes', type=int, default=15,
                        help='num processes')

    return parser.parse_args()


if __name__ == "__main__":
    run()
