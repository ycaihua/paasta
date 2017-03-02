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
PIN_TO_NUMA_NODE which contains the physical CPU and memory to restrict the
container to. If the system is NUMA enabled, docker will be called with the
arguments cpuset-cpus and cpuset-mems.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import socket
import sys

import binpacking

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


def prune_dead_pids(bins):
    # TODO: check pids
    return bins


def get_existing_bins():
    if True:  # file exists
        # TODO: Serialize
        existing_bins = [
            {101: 6, 102: 2.0}, {}
        ]
        existing_bins = prune_dead_pids(existing_bins)
        return existing_bins
    else:
        print("First time running, new bins")
        return []


def binpack(bins, value, max_bin_capacity, max_bins):
    # TODO: binpack for realz
    incoming_key = value[0]
    incoming_weight = value[1]
    print("Trying to pack %s:%s into the current bins: %s" % (incoming_key, incoming_weight, bins))
    print("with a max bin capacity of %s and not using more than %s bins" % (max_bin_capacity, max_bins))
    for bin in bins:
        bin_capacity = sum(bin.values())
        if bin_capacity + incoming_weight > max_bin_capacity:
            continue
        else:
            bin[incoming_key] = incoming_weight
            return bins
    # If we got here, maybe we can try starting a new bin
    if len(bins) < max_bins:
        return max_bins.append({incoming_key[incoming_weight]})
    else:
        raise ValueError("Couldn't find a place for %s:%s" % (incoming_key[incoming_weight]))


def is_numa_enabled():
    return os.path.exists('/proc/1/numa_maps')


def get_requested_cpu(env_args):
    # TODO: do better
    return float(env_args.get('MARATHON_APP_RESOURCE_CPUS', '10'))


def pick_best_numa_zone(requested_cpu):
    numa_zones = 2
    cores_per_zone = len(get_core_list(0))
    pid = os.getpid()
    existing_bins = get_existing_bins()

    try:
        new_bins = binpack(
            bins=existing_bins,
            value=(pid, requested_cpu),
            max_bin_capacity=cores_per_zone,
            max_bins=numa_zones,
        )
    except ValueError:
        return None

    print("After packing, here is what the new bins look like: %s" % new_bins)

    for bin in new_bins:
        if bin.get(pid, False):
            print("Guessing the best place to put this is on numa zone %d" % new_bins.index(bin))
            return new_bins.index(bin)

    print("Couldn't find a place to bin. Shouldn't get here!")
    return None


def get_numa_args(requested_cpu):
    numa_zone = pick_best_numa_zone(requested_cpu)
    if numa_zone == None:
        return ""
    else:
        core_list = get_core_list(numa_zone)
        return '--cpuset-cpus=%s --cpuset-mems=%s' % (','.join(str(c) for c in core_list), str(numa_zone))


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]

    env_args = parse_env_args(argv)

    # Marathon sets MESOS_TASK_ID whereas Chronos sets mesos_task_id
    mesos_task_id = env_args.get('MESOS_TASK_ID') or env_args.get('mesos_task_id')

    # Invalid the variable if it has a bogus value
    pin_to_numa = bool(env_args.get('PIN_TO_NUMA_NODE', False))
    pin_to_numa = True

    if pin_to_numa:
        if is_numa_enabled():
            requested_cpu = get_requested_cpu(env_args)
            argv = add_argument(argv, get_numa_args(requested_cpu=requested_cpu))
        else:
            print("Warning: asked for NUMA pinning but not on a NUMA-enabled machine!")

    if mesos_task_id and not already_has_hostname(argv):
        hostname = generate_hostname(socket.getfqdn(), mesos_task_id)
        argv = add_argument(argv, '--hostname=' + hostname)

    print(argv)
