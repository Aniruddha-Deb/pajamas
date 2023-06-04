from pajamas.pajamas import *

if __name__ == "__main__":
    # if node_stats.json or job_stats.json exists in the current directory, load
    # those instead of running these commands.

    get_stats_from_system()

    args = parse_args()
    print(args)
    if args.command == 'admit':
        check_admissibility(args.resource_list)
    elif args.command == 'stat':
        print_stats(args)
