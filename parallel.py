#!/usr/bin/env python

import argparse
import asyncio
import csv
import json
import logging
import os
import shlex
from subprocess import run
import sys


logging.basicConfig(level=logging.INFO)

TIME_FMT = "".join(
    """
{
  "command": "%C",
  "time_wall_sec": %e,
  "time_sys": %S,
  "time_user": %U,
  "max_rss_kb": %M,
  "context_switch_invol": %c,
  "context_switch_vol": %w,
  "exit_code": %x
}
""".split(
        "\n"
    )
)


def cmd_to_string(cmd):
    return " ".join([shlex.quote(s) for s in cmd])


class Job:
    def __init__(self, name, cmd, time_limit=None, mem_limit=None, stdin=None):
        self.name = name
        self.cmd = cmd.split(" ")
        self.time_limit = time_limit
        self.mem_limit = mem_limit
        self.stdin = stdin

    @staticmethod
    def from_json(x):
        return Job(x["name"], x["cmd"], x.get("time"), x.get("mem"), x.get("stdin"))

    async def run(self):
        """Run a job with time and memory controls.

        time_limit : job run time in seconds
        mem_limit : max memory use in bytes
        """
        stdout = f"/tmp/{self.name}.out"
        stderr = f"/tmp/{self.name}.err"
        properties = [
            "-p",
            f"StandardOutput=file:{stdout}",
            "-p",
            f"StandardError=file:{stderr}",
            "-p",
            f"ExecStopPost=cp {stdout} {stderr} {os.getcwd()}/",
        ]
        if self.time_limit:
            properties += ["--property", f"RuntimeMaxSec={self.time_limit}"]
        if self.mem_limit:
            properties += ["--property", f"MemoryHigh={self.mem_limit}"]
        if self.stdin:
            properties += ["--property", f"StandardInput={self.stdin}"]

        run_args = [
            "--quiet",
            "--wait",
            "--same-dir",
            "--unit",
            self.name,
            "--collect",
        ] + properties
        time_cmd = [
            "/usr/bin/time",
            "--output",
            f"{self.name}.time",
            "--format",
            TIME_FMT,
        ]
        args = run_args + time_cmd + self.cmd
        logging.debug(f"Running command: systemd-run {cmd_to_string(args)}.")
        proc = await asyncio.create_subprocess_exec("systemd-run", *args)
        await proc.wait()


def load_jobs(f):
    names = {}
    jobs_json = json.load(f)

    jobs = []
    for job_json in jobs_json:
        job = Job.from_json(job_json)
        if job.name in names:
            if names[job.name] == 0:
                logging.warn(f"Job name '{job.name}' is not unique.")
            names[job.name] += 1
            job.name = f"{job.name}-{names[job.name]}"
        else:
            names[job.name] = 0
        jobs.append(job)
    return jobs


def schedule(jobs, mem_used, mem_limit=None):
    to_run = []
    job_num = 0
    while job_num < len(jobs):
        job = jobs[job_num]
        can_run = True
        if mem_limit:
            can_run = can_run and mem_used + job.mem_limit <= mem_limit

        if can_run:
            to_run.append(job)
            del jobs[job_num]
        else:
            job_num += 1
    return to_run


async def main():
    parser = argparse.ArgumentParser(
        description="Run jobs in parallel with time and memory limits."
    )
    parser.add_argument(
        "-m", "--memory", metavar="M", help="total memory allocated for all jobs in kB"
    )
    args = parser.parse_args()

    jobs = load_jobs(sys.stdin)
    for job in jobs:
        if args.memory is not None and job.mem_limit is None:
            logging.warn(
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

    total = len(jobs)
    completed = 0
    running = 0

    mem_used = 0
    running = []
    while True:
        print(f"Completed {completed}/Running {len(running)}/Total {total}")
        to_run = schedule(jobs, mem_used, mem_limit=args.memory)
        running += set([job.run() for job in to_run])
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        completed += len(done)
        if len(pending) == 0:
            break


if __name__ == "__main__":
    asyncio.run(main())
