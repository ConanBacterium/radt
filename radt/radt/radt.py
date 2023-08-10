import argparse
import sys
from pathlib import Path

from . import constants
from .run import start_run
from .schedule import start_schedule


def schedule_split_arguments():
    sysargs = sys.argv[1:]

    for i, arg in enumerate(sysargs):
        if arg.strip()[-3:] == ".py" or arg.strip()[-4:] == ".csv":
            return sysargs[:i], Path(arg), sysargs[i + 1 :]
    else:
        print("Please supply a python or csv file.")
        exit()


def schedule_parse_arguments(args):
    parser = argparse.ArgumentParser(description="Multi-Level DNN GPU Benchmark")
    parser.add_argument(
        "-e",
        "--experiment",
        type=int,
        dest="experiment",
        default=0,
        help="Experiment ID",
    )
    parser.add_argument(
        "-w", "--workload", type=int, dest="workload", default=0, help="Workload ID"
    )
    parser.add_argument(
        "-d",
        "--devices",
        type=str,
        dest="devices",
        default="0",
        help="Devices to run on separated by +, e.g. 0, 1+2",
    )
    parser.add_argument(
        "-c",
        "--collocation",
        type=str,
        dest="collocation",
        default="-",
        help="Method of collocation, either empty, mps, or a MIG profile string",
    )
    parser.add_argument(
        "-l",
        "--listeners",
        type=str,
        dest="listeners",
        default="smi+top+dcgmi",
        help=f"Metric collectors separated by +, available: {' '.join(constants.RUN_LISTENERS + list(constants.WORKLOAD_LISTENERS.keys()))}",
    )
    parser.add_argument(
        "-r",
        "--rerun",
        type=bool,
        dest="rerun",
        default=False,
        help="Whether to force rerun runs that have previously been completed",
    )
    parser.add_argument(
        "--conda",
        action="store_true",
        dest="useconda",
        default=True,
        help="Use conda.yaml to create a conda environment",
    )
    parser.add_argument(
        "--local",
        action="store_false",
        dest="useconda",
        default=True,
        help="Use the current active environment",
    )

    return parser.parse_args(args)


def run_parse_arguments(args):
    parser = argparse.ArgumentParser(description="Multi-Level DNN GPU Benchmark")

    parser.add_argument(
        "-l",
        "--listeners",
        metavar="LISTENERS",
        required=True,
        help=f"listeners, available: {' '.join(constants.RUN_LISTENERS)}",
    )
    parser.add_argument(
        "-c",
        "--command",
        type=str,
        metavar="COMMAND",
        required=True,
    )
    parser.add_argument("-p", "--params", type=str, metavar="PARAMS")

    return parser.parse_args(args)


def check_run_listeners(l):
    if len(l) == 1 and l[0] == "none":
        return
    for entry in l:
        if entry not in constants.RUN_LISTENERS:
            raise Exception(f"Unavailable listener: {entry}")


def cli_schedule():
    args, file, args_passthrough = schedule_split_arguments()
    parsed_args = schedule_parse_arguments(args)

    start_schedule(parsed_args, file, args_passthrough)


def cli_run():
    args = run_parse_arguments(sys.argv[2:])
    listeners = args.listeners.lower().split("+")
    check_run_listeners(listeners)
    start_run(args, listeners)


def cli():
    print(sys.argv)

    if sys.argv[1].strip() == "run":
        cli_run()
    else:
        cli_schedule()