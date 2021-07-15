#!/usr/bin/env python3

import os
import sys
import yaml
import json
import gzip
import string
import argparse
import subprocess
from glob import glob
from datetime import date


BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
JOB_DIR = os.path.join(BASE_DIR, 'jobs')
PAYLOAD_DIR = os.path.join(BASE_DIR, 'collab-song-payload-dumps')

JOB_PARAMS = {
    "_job_id": None,
    "study_id": None,
    "analysis_id": [],
    "max_retries": 3,
    "first_retry_wait_time": 60,
    "cpus": 1,
    "mem": 1,
    "local_dir": "/icgc-argo-scratch",
    "download_song_url": "https://song.cancercollaboratory.org",
    "download_score_url": "https://storage.cancercollaboratory.org",
    "download_cpus": 1,
    "download_mem": 2,
    "download_transport_mem": 2,
    "upload_song_url": "https://song.azure.icgc.overture.bio",
    "upload_score_url": "https://score.azure.icgc.overture.bio",
    "upload_cpus": 1,
    "upload_mem": 2,
    "upload_transport_mem": 2,
    "transport_mem": 2
}


def get_new_batch_id(batch_ids=list()):
    today = date.today().strftime('%Y-%m-%d')
    if not batch_ids:
        return today

    last_batch_id = batch_ids[-1]
    if last_batch_id == today:
        return f"{today}a"

    elif last_batch_id.startswith(today):
        suffix = last_batch_id.replace(today, '')
        if len(suffix) != 1 or suffix not in string.ascii_lowercase:
            sys.exit(f"Last batch ID not valid: {last_batch_id}. Batch ID must follow pattern: 'YYYY-MM-DD[a-z]{0,1}'")
        elif suffix == 'z':
            sys.exit("Can no longer generate new batch of jobs for today.")

        idx = string.ascii_lowercase.index(suffix)
        return f"{today}{string.ascii_lowercase[idx + 1]}"

    else:
        return today


def get_data_size(analysis):
    data_size = 0
    for f in analysis['file']:
        data_size += f['fileSize']

    return data_size


def create_new_job(study, analysis_ids):
    total_data_size = 0
    for a in analysis_ids:
        total_data_size += analysis_ids[a]

    down_up_cpus = 1
    if len(analysis_ids) == 1:  # single analysis job
        down_up_cpus = int(total_data_size / 5000000000)  # 5GB per CPU

    if down_up_cpus < 1:
        down_up_cpus = 1

    if down_up_cpus > 4:
        down_up_cpus = 4

    new_job = JOB_PARAMS.copy()
    new_job.update({
        'study_id': study,
        'analysis_id': sorted(list(analysis_ids.keys())),
        'download_cpus': down_up_cpus,
        'download_mem': down_up_cpus * 2,  # 2 GB mem per cpu
        'upload_cpus': down_up_cpus,
        'upload_mem': down_up_cpus * 2,  # 2 GB mem per cpu
        '_analysis_ids': analysis_ids,
        '_total_data_size': total_data_size
    })

    return new_job


def main(study, job_count, config):
    # locate the payload dump in `PAYLOAD_DIR`
    payload_dumps = sorted(glob(os.path.join(PAYLOAD_DIR, f"{study}.payloads.*.json.gz")))
    if not payload_dumps:  # no dump found
        sys.exit(f"Error: no SONG payload dump found under {PAYLOAD_DIR} for study {study}")
    else:
        payload_dump = payload_dumps[-1]  # use the latest one (if multiple exist)

    with gzip.open(payload_dump, 'r') as p:
        collab_song_analysis_objects = json.load(p)

    # get all previous batches for the specified study, collect analysis IDs that
    # have already been part of jobs previously generated
    analysis_ids_with_job_generated = []
    batch_summary_files = sorted(glob(os.path.join(JOB_DIR, study, '*', 'batch-summary.tsv')))

    batch_ids = []
    for f in batch_summary_files:
        batch_ids.append(os.path.basename(os.path.dirname(f)))
        with open(f, 'r') as s:
            for line in s:
                if line.startswith('#'):
                    continue
                analysis_ids_with_job_generated.append(line.split('\t')[3])

    # assign new batch ID
    new_batch_id = get_new_batch_id(batch_ids)

    # prepare transfer jobs
    new_jobs = []
    analysis_ids = dict()
    for analysis in collab_song_analysis_objects:
        if analysis['study'] != study:
            sys.exit(f"Error: different study '{analysis['study']}' in payload dump, specified study in command line: {study}")
        if analysis['analysisId'] in analysis_ids_with_job_generated:
            continue
        if analysis['analysisState'] != 'PUBLISHED':
            continue

        data_size = get_data_size(analysis)
        if data_size > config['max_analysis_data_size_to_be_pooled']:
            new_jobs.append(create_new_job(study, {analysis['analysisId']: data_size}))
            if len(new_jobs) == job_count:
                break
        else:
            if len(analysis_ids) >= config['max_analyses_per_job']:
                new_jobs.append(create_new_job(study, analysis_ids))
                analysis_ids = dict()
                if len(new_jobs) == job_count:
                    break

            analysis_ids[analysis['analysisId']] = data_size

    if analysis_ids and len(new_jobs) < job_count:
        new_jobs.append(create_new_job(study, analysis_ids))

    # print(json.dumps(new_jobs, indent=2))

    if not new_jobs:
        print(f'Nothing to do. All analysis objects from {payload_dump} have been included in previously generated jobs.')
        sys.exit()

    # create batch directories
    new_batch_dir = os.path.join(JOB_DIR, study, new_batch_id)
    dirs = ['backlog', 'completed', 'failed', 'queued']
    for d in dirs:
        fullpath = os.path.join(new_batch_dir, d)
        os.makedirs(fullpath)
        open(os.path.join(fullpath, '.gitkeep'), 'a').close()

    # generate batch-summary.tsv
    batch_summary_f = open(os.path.join(new_batch_dir, 'batch-summary.tsv'), 'w')
    batch_summary_f.write(f"# SONG analysis objects retrieved from payload dump: {os.path.basename(payload_dump)}\n")
    i = 0
    for job in new_jobs:
        i += 1
        job_id = 'job.' + (4 - len(str(i))) * '0' + str(i)

        job['_job_id'] = job_id
        total_data_size = job.pop('_total_data_size')
        analysis_ids = job.pop('_analysis_ids')
        for a in analysis_ids:
            fields = [job_id, str(len(analysis_ids)), str(total_data_size), a, str(analysis_ids[a])]
            batch_summary_f.write("\t".join(fields) + "\n")

        job_dir = os.path.join(new_batch_dir, 'backlog', job_id)
        os.makedirs(job_dir)
        with open(os.path.join(job_dir, 'params.json'), 'w') as j:
            j.write(json.dumps(job, indent=2))

    batch_summary_f.close()

    print(f"Generated {len(new_jobs)} jobs in {new_batch_dir}\n"
          "After verification of the new jobs, please remember git add, commit and push")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ICGC 25k data Azure transfer job generator')
    parser.add_argument('-s', '--study', type=str,
                        help='SONG study', required=True)
    parser.add_argument('-n', '--job-count', type=int,
                        help='Number of jobs to be generated', required=True)
    args = parser.parse_args()

    config_file = os.path.join(BASE_DIR, 'scripts', 'job-gen.conf')
    with open(config_file) as f:
        config = yaml.safe_load(f)

    if args.job_count > 9999:
        sys.exit(f"Job count can not be greater than 9999: {args.job_count} specified")

    if args.job_count > config['max_jobs_per_batch']:
        sys.exit(f"According to the supplied config, job count per batch can not be "
                 f"greater than {config['max_jobs_per_batch']}: {args.job_count} specified")

    # make sure to merge scheduler branch into main branch before generating any new jobs
    cmd = 'git checkout scheduler && git pull && git checkout main && git merge scheduler -m "merge scheduler"'
    proc = subprocess.Popen(
                cmd,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
    stdout, stderr = proc.communicate()
    if proc.returncode:
        sys.exit(f"Unable to merge latest scheduler branch into main. Failed command: '{cmd}'.\n"
                 f"STDOUT: {stdout.decode('utf-8')}\nSTDERR: {stderr.decode('utf-8')}")

    if args.job_count == 0:
        print("Job count was set to 0, stop here.")
        sys.exit()

    main(args.study, args.job_count, config)
