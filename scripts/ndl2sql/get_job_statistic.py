#! /usr/local/bin/python3.6

import json
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--jobs-files', dest='jobs', required=True, help='--job-files <job_file_1> <job_file2>',
                        nargs='+', type=str)
    parser.add_argument('-o', '--otput-file', dest='output_file', required=True, help='--output-file <output_file>', type=str)
    args = parser.parse_args()

    # {'jobs': [
    #       {'tasks': {'canceling': 0, 'created': 0, 'scheduled': 0, 'finished': 175, 'failed': 0, 'deploying': 0, 'reconciling': 0, 'total': 175, 'running': 0, 'canceled': 0},
    #        'duration': 83802,
    #        'end-time': 1634588250541,
    #        'last-modification': 1634588250541,
    #        'name': 'p_10-q22-rew_test.dlp_par-10_ttl-5_p-p3',
    #        'start-time': 1634588166739,
    #        'state': 'FINISHED',
    #        'jid': 'a804c4e268645703270fc3a3b66ef870'}]}
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = 'stats.csv'

    with open(output_file, 'w') as csv:
        header_file = ['jid', 'name', 'duration', 'start-time', 'end-time', 'state', 'finished', 'failed', 'total', 'job-parallelism']
        print(','.join(str(c) for c in header_file), file=csv)
        for job_name in args.jobs:
            with open(job_name, "r") as read_file:
                json_data = json.load(read_file)

            json_job = json.loads(json_data["archive"][0]["json"])
            json_config = json.loads(json_data["archive"][1]["json"])
            job_keys = header_file[0:6] # ['jid', 'name', 'duration', 'start-time', 'end-time', 'state']
            jobs_stats = list(map(lambda key: json_job['jobs'][0][key], job_keys))
            tasks_keys = header_file[6:9] # ['finished', 'failed', 'total']
            tasks_stats = list(map(lambda key: json_job['jobs'][0]['tasks'][key], tasks_keys))
            config_keys = header_file[9:10]
            config_stats = list(map(lambda key: json_config['execution-config'][key], config_keys))
            stats = jobs_stats + tasks_stats + config_stats
            print(','.join(str(c) for c in stats), file=csv)

if __name__ == '__main__':
    main()

