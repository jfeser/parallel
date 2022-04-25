#!/usr/bin/env python

import argparse
from collections import namedtuple
import csv
import json
import logging
from multiprocessing.connection import Listener
import os
import shlex
import subprocess
from subprocess import run, DEVNULL
import sys
import tempfile


logging.basicConfig(level=logging.DEBUG)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SLICE = "bench.slice"


def cmd_to_string(cmd):
    return " ".join([shlex.quote(s) for s in cmd])


class Job:
    def __init__(self, name, cmd, time_limit=None, mem_limit=None, stdin=None):
        self.name = name
        self.cmd = cmd.split(" ")
        self.time_limit = time_limit
        self.mem_limit = mem_limit
        self.stdin = stdin

        self.state = "pending"
        self.core = None

    @staticmethod
    def from_json(x):
        return Job(x["name"], x["cmd"], x.get("time"), x.get("mem"), x.get("stdin"))

    def run(self, out_dir, tmp_dir, core):
        """Run a job with time and memory controls."""
        stdout = f"{out_dir}/{self.name}.out"
        stderr = f"{out_dir}/{self.name}.err"
        wrapper = f"{DIR_PATH}/job_wrapper.py"

        properties = [
            "MemoryAccounting=true",
            "CPUAccounting=true",
            f"StandardOutput=truncate:{stdout}",
            f"StandardError=truncate:{stderr}",
            f"ExecStartPost={wrapper} --mode pre --output {out_dir} --name {self.name}",
            f"ExecStop={wrapper} --mode post --output {out_dir} --name {self.name} --temp {tmp_dir}",
            f"AllowedCPUs={core.id_}",
            f"AllowedMemoryNodes={core.numa}",
        ]
        if self.time_limit:
            properties += [f"RuntimeMaxSec={self.time_limit}"]
        if self.mem_limit:
            properties += [f"MemoryMax={self.mem_limit}"]
        if self.stdin:
            properties += [f"StandardInput={self.stdin}"]

        property_args = []
        for p in properties:
            property_args += ["-p", p]

        run_args = [
            "--no-block",
            "--user",
            "--no-ask-password",
            "--quiet",
            "--same-dir",
            "--unit",
            self.name,
            "--collect",
            "--slice",
            SLICE,
        ] + property_args

        args = ["systemd-run"] + run_args + self.cmd

        logging.debug(f"Running command: '{cmd_to_string(args)}'")
        run(args, stdout=DEVNULL)

        self.core = core
        self.state = "running"


Core = namedtuple("Core", ["id_", "numa"])


def load_cores():
    with open("/sys/devices/system/cpu/online", "r") as f:
        cpu_range = f.read().strip()
    min_id, max_id = tuple(cpu_range.split("-"))
    min_id = int(min_id)
    max_id = int(max_id)

    cores = []
    for cpu_id in range(min_id, max_id + 1):
        numa = None
        for d in os.listdir(f"/sys/devices/system/cpu/cpu{cpu_id}"):
            if d.startswith("node"):
                numa = int(d[4:])
                break
        cores.append(Core(cpu_id, numa))
    return set(cores)


def load_jobs(f):
    jobs = {}
    for job_json in json.load(f):
        job = Job.from_json(job_json)
        if job.name in jobs:
            logging.error(f"Job name '{job.name}' is not unique.")
            exit(1)
        jobs[job.name] = job
    return jobs


def schedule(jobs, out_dir, tmp_dir, cores, mem_limit=None):
    # Compute remaining resources
    cores_remain = set(cores)
    for j in jobs:
        if j.state == "running":
            cores_remain.remove(j.core)

    mem_remain = None
    if mem_limit is not None:
        mem_remain = mem_limit
        for j in jobs:
            if j.state == "running":
                mem_used -= j.mem_limit

    # Start new jobs until no resources remain
    for j in jobs:
        if (
            j.state != "pending"
            or len(cores_remain) == 0
            or (mem_limit is not None and mem_remain < job.mem_limit)
        ):
            continue

        core = cores_remain.pop()
        j.run(out_dir, tmp_dir, core)
        if mem_limit is not None:
            mem_remain -= job.mem_limit


def shutdown(jobs):
    for job in jobs.values():
        run(["systemctl", "stop", f"{job.name}.service"])


def main():
    cores = load_cores()

    parser = argparse.ArgumentParser(
        description="Run jobs in parallel with time and memory limits."
    )
    parser.add_argument(
        "-m",
        "--memory",
        metavar="M",
        type=int,
        help="total memory allocated for all jobs (bytes)",
    )
    parser.add_argument(
        "-c",
        "--cpus",
        metavar="N",
        type=int,
        default=len(cores),
        help="number of cpus allocated for all jobs",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="D",
        default=os.getcwd(),
        help="directory for job output (default: cwd)",
    )
    args = parser.parse_args()

    if args.cpus <= 0:
        logging.error("Argument to --cpus should be positive.")
        exit(1)

    cores = set(list(cores)[: args.cpus])  # Take args.cpus cores

    jobs = load_jobs(sys.stdin)
    for job in jobs:
        if args.memory is not None and job.mem_limit is None:
            logging.warning(
                "Overall memory limit set, but job does not have a memory limit."
            )
            job.mem_limit = args.memory
        if (
            args.memory is not None
            and job.mem_limit is not None
            and job.mem_limit > args.memory
        ):
            logging.error(
                f"Job memory limit ({job.mem_limit}) > overall memory limit ({args.memory})."
            )
            exit(1)

    # Check that hyperthreading is disabled.
    with open("/sys/devices/system/cpu/smt/control", "r") as f:
        if f.read().strip() == "on":
            logging.warning(
                "Hyperthreading is enabled. This may affect benchmark timing."
            )

    # Set up the benchmark slice
    cpus_str = "".join(str(c.id_) for c in cores)
    properties = [f"AllowedCPUs={cpus_str}"]
    if args.memory is not None:
        properties.append(f"MemoryMax={args.memory}")
    run(["systemctl", "--user", "--runtime", "set-property", SLICE])

    # Create output directory
    run(["mkdir", "-p", args.output])

    tmp_dir = tempfile.mkdtemp()

    completed = 0
    with Listener(address=f"{tmp_dir}/control.sock", family="AF_UNIX") as listener:
        while completed < len(jobs):
            schedule(jobs.values(), args.output, tmp_dir, cores, mem_limit=args.memory)

            running = sum(1 if j.state == "running" else 0 for j in jobs.values())
            logging.info(f"Completed {completed}/Running {running}/Total {len(jobs)}")

            with listener.accept() as conn:
                completed_name = conn.recv()
                jobs[completed_name].state = "completed"
                completed += 1
    logging.info(f"Completed {completed}/Running {running}/Total {len(jobs)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        shutdown(jobs)
