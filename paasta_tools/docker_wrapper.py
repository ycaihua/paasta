#!/usr/bin/env python
""" Meant to be used by mesos-slave instead of the /usr/bin/docker executable
directly This will parse the CLI arguments intended for docker, extract
environment variable settings related to the actual node hostname and mesos
task ID, and use those as an additional --hostname argument when calling the
underlying docker command.

If the environment variables are unspecified, or if --hostname is already
specified, this does not change any arguments and just directly calls docker
as-is.

Additionally this wrapper will look for the environment variable
NUMA_CPU_AFFFINITY which contains the physical CPU and memory to restrict the
container to. If the system is NUMA enabled, docker will be called with the
arguments cpuset-cpus and cpuset-mems.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import socket
import sys


ENV_MATCH_RE = re.compile('^(-\w*e\w*|--env)(=(\S.*))?$')
MAX_HOSTNAME_LENGTH = 63


def parse_env_args(args):
    result = {}
    in_env = False
    for arg in args:
        if not in_env:
            match = ENV_MATCH_RE.match(arg)
            if not match:
                continue
            arg = match.group(3) or ''
            if not arg:
                in_env = True
                continue

        in_env = False
        if '=' not in arg:
            continue

        k, _, v = arg.partition('=')
        result[k] = v

    return result


def already_has_hostname(args):
    for arg in args:
        if arg == '-h':
            return True
        if arg.startswith('--hostname'):
            return True
        if len(arg) > 1 and arg[0] == '-' and arg[1] != '-':
            # several short args
            arg = arg.partition('=')[0]
            if 'h' in arg:
                return True
    return False


def generate_hostname(fqdn, mesos_task_id):
    host_hostname = fqdn.partition('.')[0]
    task_id = mesos_task_id.rpartition('.')[2]

    hostname = host_hostname + '-' + task_id

    # hostnames can only contain alphanumerics and dashes and must be no more
    # than 63 characters
    hostname = re.sub('[^a-zA-Z0-9-]+', '-', hostname)[:MAX_HOSTNAME_LENGTH]
    return hostname


def add_argument(args, argument):
    # Add an argument immediately after 'run' command if it exists
    args = list(args)

    try:
        run_index = args.index('run')
    except ValueError:
        pass
    else:
        args.insert(run_index + 1, argument)

    return args


def get_core_list(cpuid):
    core = 0
    core_list = []

    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                m = re.match('physical\sid.*(\d)', line)
                if m:
                    if int(m.group(1)) == cpuid:
                        core_list.append(core)
                    core += 1
    except IOError:
        pass

    return core_list


def is_numa_enabled():
    return os.path.exists('/proc/1/numa_maps')


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    env_args = parse_env_args(argv)

    # Marathon sets MESOS_TASK_ID whereas Chronos sets mesos_task_id
    mesos_task_id = env_args.get('MESOS_TASK_ID') or env_args.get('mesos_task_id')

    # Invalid the variable if it has a bogus value
    try:
        numa_cpuid = int(env_args.get('PIN_TO_NUMA_NODE'))
    except (ValueError, TypeError):
        numa_cpuid = None

    if numa_cpuid and is_numa_enabled():
        core_list = get_core_list(numa_cpuid)
        if len(core_list) > 0:
            argv = add_argument(argv, '--cpuset-cpus=' + ','.join(str(c) for c in core_list))
            argv = add_argument(argv, '--cpuset-mems=' + str(numa_cpuid))

    if mesos_task_id and not already_has_hostname(argv):
        hostname = generate_hostname(socket.getfqdn(), mesos_task_id)
        argv = add_argument(argv, '--hostname=' + hostname)

    os.execlp('docker', 'docker', *argv[1:])
