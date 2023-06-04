################################################################################
#
# Pajamas
# PBS Assistant for Job Admission, Management And Statistics
#
# Copyright Aniruddha Deb 2023.
#
################################################################################

import sys
import subprocess
import argparse
import json
import os
from pajamas.tabulate import tabulate

# schema example:
# pbsnodes -a -F json <nodeid>
# (Omitting <nodeid> gives you all the nodes)
# {
#     "timestamp":1682438136,
#     "pbs_version":"2021.1.3.20220217134230",
#     "pbs_server":"hn1",
#     "nodes":{
#         "aice003":{
#             "Mom":"aice003.hpc.iitd.ac.in",
#             "ntype":"PBS",
#             "state":"free", ["free", "job-busy", "offline", "down", "state-unknown"]
#             "pcpus":64,
#             "jobs":[
#                 "3469111.pbshpc",
#                 "3470330.pbshpc",
#                 "3469111.pbshpc",
#                 "3470330.pbshpc",
#                 "3469111.pbshpc",
#                 "3470286.pbshpc",
#                 "3469111.pbshpc",
#                 "3470286.pbshpc",
#                 "3469111.pbshpc",
#                 "3469443.pbshpc",
#                 "3469267.pbshpc",
#                 "3469270.pbshpc",
#                 "3469442.pbshpc",
#                 "3469111.pbshpc",
#                 "3469442.pbshpc",
#                 "3471280.pbshpc",
#                 "3471598.pbshpc"
#             ],
#             "resources_available":{
#                 "arch":"linux",
#                 "centos":"icelake",
#                 "host":"aice003",
#                 "hpmem":"0b",
#                 "mem":"514260mb",
#                 "ncpus":64,
#                 "ngpus":2,
#                 "vmem":"579732mb",
#                 "vnode":"aice003"
#             },
#             "resources_assigned":{
#                 "ncpus":62,
#                 "ngpus":2
#             },
#             "queue":"test",
#             "resv_enable":"True",
#             "sharing":"default_shared",
#             "license":"l",
#             "last_state_change_time":1682379461,
#             "last_used_time":1682426236
#         }
#     }
# }
#
# qstat -f -F json <jobid>
# {
#     "timestamp":1682496699,
#     "pbs_version":"2021.1.3.20220217134230",
#     "pbs_server":"hn1",
#     "Jobs":{
#         "3472664.pbshpc":{
#             "Job_Name":"STDIN",
#             "Job_Owner":"cs1190329@ibklogin01",
#             "resources_used":{
#                 "cpupercent":0,
#                 "cput":"00:00:00",
#                 "mem":"5008kb",
#                 "ncpus":1,
#                 "vmem":"196804kb",
#                 "walltime":"00:01:17"
#             },
#             "job_state":"R",
#             "queue":"standard",
#             "server":"hn1",
#             "Checkpoint":"u",
#             "ctime":"Wed Apr 26 13:40:10 2023",
#             "Error_Path":"/dev/pts/2",
#             "exec_host":"khas024/4",
#             "exec_vnode":"(khas024:ncpus=1:ngpus=1)",
#             "Hold_Types":"n",
#             "interactive":"True",
#             "Join_Path":"n",
#             "Keep_Files":"oed",
#             "Mail_Points":"n",
#             "mtime":"Wed Apr 26 13:41:33 2023",
#             "Output_Path":"/dev/pts/2",
#             "Priority":0,
#             "qtime":"Wed Apr 26 13:40:10 2023",
#             "Rerunable":"False",
#             "Resource_List":{
#                 "ncpus":1,
#                 "ngpus":1,
#                 "nodect":1,
#                 "place":"free",
#                 "run_share":1200,
#                 "select":"1:ncpus=1:ngpus=1:centos=haswell",
#                 "site":"{\"job.q_prio\": 100.0, \"job.ncpus\": 1.0, \"job.walltime\": 1200, \"job.ngpus\": 1.0}",
#                 "walltime":"00:20:00"
#             },
#             "stime":"Wed Apr 26 13:40:15 2023",
#             "session_id":29218,
#             "sandbox":"private",
#             "jobdir":"/scratch/pbs/pbs.3472664.pbshpc.x8z",
#             "substate":42,
#             "Variable_List":{
#                 "PBS_O_HOME":"/home/cse/btech/cs1190329",
#                 "PBS_O_LANG":"en_US.UTF-8",
#                 "PBS_O_LOGNAME":"cs1190329",
#                 "PBS_O_PATH":"/home/cse/btech/cs1190329/.vscode-server/bin/704ed70d4fd1c6bd6342c436f1ede30d1cff4710/bin/remote-cli:/opt/am/bin/:/opt/am/sbin/:/opt/pbs/default/bin:/opt/pbs/default/sbin:/usr/share/Modules/4.4.1/bin:/opt/pbs/2021.1.0/bin:/opt/pbs/default/bin:/opt/am/bin:/usr/lib64/qt-3.3/bin:/opt/am/bin/:/opt/am/sbin/:/opt/pbs/default/bin:/opt/pbs/default/sbin:/usr/share/Modules/4.4.1/bin:/opt/pbs/2021.1.0/bin:/opt/pbs/default/bin:/opt/am/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/am/python/bin:/opt/ibutils/bin:/opt/pbs/2021.1.3/bin:/opt/am/python/bin:/opt/pbs/2021.1.3/bin",
#                 "PBS_O_MAIL":"/var/spool/mail/cs1190329",
#                 "PBS_O_SHELL":"/bin/bash",
#                 "PBS_O_WORKDIR":"/home/cse/btech/cs1190329/main",
#                 "PBS_O_SYSTEM":"Linux",
#                 "PBS_NCHUNKS":1,
#                 "PBS_NCPUS":1,
#                 "PBS_NTASKS":1,
#                 "Mail_Pt":"n",
#                 "PBS_O_QUEUE":"standard",
#                 "PBS_O_HOST":"ibklogin01"
#             },
#             "comment":"Job run at Wed Apr 26 at 13:40 on (khas024:ncpus=1:ngpus=1)",
#             "etime":"Wed Apr 26 13:40:10 2023",
#             "run_count":1,
#             "eligible_time":"00:00:00",
#             "Submit_arguments":"-I -P col380.cs1190329 -l select=1:ncpus=1:ngpus=1:centos=haswell -l walltime=00:20:00",
#             "project":"col380.cs1190329",
#             "Submit_Host":"ibklogin01"
#         }
#     }
# }

# usage:
node_stats = {}
job_stats = {}

def print_node_stats(node_list):
    table = {
        'nodes': list(node_list.keys()),
        'ncpus': [a['resources_available']['ncpus'] - a['resources_assigned'].get('ncpus', 0) for a in node_list.values()],
        'ngpus': [a['resources_available']['ngpus'] - a['resources_assigned'].get('ngpus', 0) if 'ngpus' in a['resources_available'] else ' ' for a in node_list.values()]
    }
    # TODO group/sort the table
    print(tabulate(table, headers='keys'))

def print_node_stats_verbose(node_list):
    print("ERROR: not implemented yet")
    pass

def parse_resources(str):
    return {a[0]: a[1] for a in [b.split('=') for b in str.split(':')]}

def parse_args():
    parser = argparse.ArgumentParser(prog='Pajamas', 
            description='PBS Assistant for Job Admission, Management And Statistics',
            epilog="""This script is a wrapper over the standard 
                    PBS utilities which offers better insight into what jobs 
                    are running on which particular nodes, how soon a node will
                    free up, etc.""")
    
    subparsers = parser.add_subparsers(dest='command')
    stat_parser = subparsers.add_parser('stat')
    admit_parser = subparsers.add_parser('admit')

    stat_parser.add_argument('-v', '--verbose', action='store_true',
                             help='Verbose output for each node')
    stat_parser.add_argument('-f', '--free', action='store_true',
                             help='Only get statistics of free nodes')
    stat_parser.add_argument('-e', '--exclude', nargs='*', type=str,
                             help='Specific node prefixes to exclude from output')
    stat_parser.add_argument('-i', '--include', nargs='*', type=str,
                             help='Specific node prefixes to include in output')

    admit_parser.add_argument('resource_list', type=parse_resources,
                              help='Resource list for target job to run')
    
    return parser.parse_args()

def get_stats_from_system():

    global node_stats, job_stats

    if os.path.exists('node_stats.json'):
        node_stats = json.load(open('node_stats.json', 'r'))['nodes']
    else:
        node_stats_raw = subprocess.run(['pbsnodes', '-a', '-F', 'json'], encoding='utf-8', stdout=subprocess.PIPE)
        node_stats = json.loads(node_stats_raw.stdout)['nodes']

    if os.path.exists('job_stats.json'):
        job_stats = json.load(open('job_stats.json', 'r'))['Jobs']
    else:
        job_stats_raw = subprocess.run(['qstat', '-f', '-F', 'json'], encoding='utf-8', stdout=subprocess.PIPE)
        job_stats = json.loads(job_stats_raw.stdout)['Jobs']

    return node_stats, job_stats

def check_admissibility(resource_list):
    free_nodes = {a:b for a,b in node_stats.items() if b['state'] == 'free'}
    # TODO select
    assert('ncpus' in resource_list)
    if 'centos' in resource_list:
        # filter based on OS
        free_nodes = {a:b for a,b in free_nodes.items() 
                      if b['resources_available']['centos'] == resource_list['centos']}

    if 'ngpus' in resource_list:

        free_nodes = {a:b for a,b in free_nodes.items()
                      if ('ngpus' in b['resources_available']) and 
                         (int(b['resources_available']['ngpus']) - b['resources_assigned'].get('ngpus', 0)) >= int(resource_list['ngpus'])
                     }

    free_nodes = {a:b for a,b in free_nodes.items()
                  if (int(b['resources_available']['ncpus']) - b['resources_assigned'].get('ncpus',0) >= int(resource_list['ncpus']))
                 }

    if free_nodes:
        print_node_stats(free_nodes)
    else:
        print('No Free Nodes available')
        # TODO smart resource tuning
        # TODO ETA to node free up

def print_stats(args):
    # filter nodes based on includes/excludes/verbose, then print
    free_nodes = node_stats
    if args.free:
        free_nodes = {a:b for a,b in node_stats.items() if b['state'] == 'free'}

    # list before, otherwise dictionary changes size during iteration
    for a in list(free_nodes.keys()):
        if args.include:
            for prefix in args.include:
                if not a.startswith(prefix):
                    del free_nodes[a]
        elif args.exclude:
            for prefix in args.exclude:
                if a.startswith(prefix):
                    del free_nodes[a]

    if args.verbose:
        print_node_stats_verbose(free_nodes)
    else:
        print_node_stats(free_nodes)

