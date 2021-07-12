#!/usr/bin/env python3

import os
import sys
import json
import yaml
import time
import random
import requests
import argparse
import subprocess
import datetime
from glob import glob
from tenacity import retry, wait_random, stop_after_attempt
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
JOB_DIR = os.path.join(BASE_DIR, 'jobs')
CONFIG_FILE = os.path.join(BASE_DIR, 'scripts', 'job-scheduler.conf')
API_TOKEN = os.getenv("API_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


def run_cmd(cmd):
    proc = subprocess.Popen(
                cmd,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
    stdout, stderr = proc.communicate()

    if proc.returncode:
        error_msg = f"Error: failed to perform command: {cmd}"
        print(error_msg, file=sys.stderr)

    return (
        stdout.decode("utf-8").strip(),
        stderr.decode("utf-8").strip(),
        proc.returncode
    )


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(3))
def get_wes_token(env, config):
    token_url = config['compute_environments'][env]['token_url']
    client_id = os.environ.get(config['compute_environments'][env]['ENV']['client_id'])
    client_secret = os.environ.get(config['compute_environments'][env]['ENV']['client_secret'])

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(
                token_url=token_url,
                client_id=client_id,
                client_secret=client_secret,
                include_client_id=True
            )

    return token.get('access_token')


def pull_job_batches(config):
    # need to add a condition here: only when the last commit on the scheduler branch
    # appeared on the main branch, it's safe to proceed with merging main into scheduler branch
    cmd = "git checkout scheduler > /dev/null 2>&1 && git log --grep='^\\[scheduler\\]' --format='%at' -n1"
    stdout, stderr, rc = run_cmd(cmd)
    if rc:  # when error out, no need to continue
        print("Skip sync with main branch. Unable to retieve commit log for last job status update on the scheduler branch", file=sys.stderr)
        return
    scheduler_last_update_at = int(stdout) if stdout else 0

    # this is clunky, better alternative would be to make API calls against GitHub to get log info
    cmd = "git stash > /dev/null 2>&1 && git checkout main > /dev/null 2>&1 && " + \
          "git pull > /dev/null 2>&1 && git log --grep='^\\[scheduler\\]' --format='%at' -n1 && " + \
          "git checkout scheduler > /dev/null 2>&1 && ( git stash pop > /dev/null 2>&1 || true ) && git add . > /dev/null 2>&1"
    stdout, stderr, rc = run_cmd(cmd)
    if rc:  # when error out, no need to continue
        print("Skip sync with main branch. Unable to retieve commit log for last job status update on the main branch", file=sys.stderr)
        return
    main_last_update_at = int(stdout) if stdout else 0

    if main_last_update_at != scheduler_last_update_at:
        main_update_at_str = datetime.datetime.fromtimestamp(main_last_update_at).strftime('%Y-%m-%d %H:%M:%S')
        scheduler_update_at_str = datetime.datetime.fromtimestamp(scheduler_last_update_at).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Skip sync with main branch. The last scheduler status update at {scheduler_update_at_str} has not "
              f"been merged into main branch, whose last job status update was at {main_update_at_str}")
        return

    cmd = 'git checkout main && git pull && git checkout scheduler && git merge main'

    stdout, stderr, rc = run_cmd(cmd)
    if rc:
        error_msg = f"Failed to execute command: {cmd}\nStdout: {stdout}\nStderr: {stderr}\n"
        print(error_msg, file=sys.stderr)
        send_notification(error_msg, 'CRITICAL', config)
        sys.exit(1)
    else:
        print("Pulled and merged updates from main branch to scheduler branch.")


def get_job_status_summary():
    report_summary_lines = []

    cmd = "git status jobs |grep renamed |grep 'params.json' |awk -F'/' '{print $2\" \"$4\"->\"$9}' |sort |uniq -c |awk '{print $2\" \"$3\" \"$1}'"
    stdout, stderr, rc = run_cmd(cmd)
    job_status_changes = stdout.split('\n') if stdout else []

    report_summary_lines = job_status_changes
    changed_studies = set([])
    for change in job_status_changes:
        changed_studies.add(change.split(' ')[0])

    for s in changed_studies:
        cmd = "ls -d jobs/%s/*/*/job.* |awk -F'/' '{print $2\" \"$4}' |sort |uniq -c |awk '{print $2\" \"$3\" \"$1}'" % s
        stdout, stderr, rc = run_cmd(cmd)
        report_summary_lines += stdout.split('\n') if stdout else []

    return "```\n" + "\n".join(report_summary_lines) + "\n```"


def push_job_status(config):
    # detect whether new excessive failure flag file created
    new_excessive_run_failure = False
    cmd = "git add . && git status jobs | grep 'new file' | grep excessive_failure ||true"
    stdout, stderr, rc = run_cmd(cmd)
    if 'excessive_failure' in stdout:
        new_excessive_run_failure = True

    cmd = "git log --grep='^\\[scheduler\\]' --format='%at' -n1"
    stdout, stderr, rc = run_cmd(cmd)
    last_update_at = int(stdout) if stdout else 0
    epoch_time_now = int(time.time())

    # push job status when new excessive failure detected or push interval greater than configured threshold
    if not new_excessive_run_failure and \
       epoch_time_now - last_update_at < config['tracking_repo_push_interval'] * 60 * 60 + 300:  # allow 5 min offset
        return

    stdout, stderr, rc = run_cmd("git status")
    if 'working tree clean' in stdout:  # nothing to commit
        msg = 'No job status change.'
        print(msg)
        send_notification(msg, 'INFO', config)
        return

    status_summary = get_job_status_summary()

    cmd = "git add . && git commit -m '[scheduler] update job status' && git push"

    stdout, stderr, rc = run_cmd(cmd)
    if rc:
        error_msg = f"Failed to execute command: {cmd}\nStdout: {stdout}\nStderr: {stderr}\n"
        print(error_msg, file=sys.stderr)
        send_notification(error_msg, 'CRITICAL', config)
    else:
        print("Pushed latest job status on the scheduler branch.")
        send_notification(status_summary, 'INFO', config)


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(3))
def get_run_state(graphql_url, run_id, wes_token):
    graphql_query = {
        "operationName": "SINGLE_RUN_QUERY",
        "variables": {
            "runId": run_id
        },
        "query": '''query SINGLE_RUN_QUERY($runId: String!) {
            runs(filter: {runId: $runId}) {
                content {
                    runId
                    sessionId
                    commandLine
                    completeTime
                    duration
                    engineParameters {
                        launchDir
                        projectDir
                        resume
                        revision
                        workDir
                        __typename
                    }
                    errorReport
                    exitStatus
                    parameters
                    repository
                    startTime
                    state
                    success
                    __typename
                }
                __typename
            }
        }'''
    }

    response = requests.post(
        url=graphql_url,
        json=graphql_query,
        headers={"Authorization": f"Bearer {wes_token}"}
    )
    if response.status_code != 200:
        raise Exception(f"Unable to retrieve run state for {run_id} from {graphql_url}")

    response_obj = json.loads(response.text)
    return response_obj['data']['runs']['content'][0]


def move_job_to_new_state(new_state, job_batch_path, current_job_path):
    new_job_path = os.path.join(job_batch_path, new_state)
    cmd = f'git mv {current_job_path} {new_job_path}'
    stdout, stderr, returncode = run_cmd(cmd)
    if returncode:
        print(f"Unable to perform: {cmd}", file=sys.stderr)
    else:
        print(f"Job status change, new state: {new_state}, job: {new_job_path}/{os.path.basename(current_job_path)}")


def update_queued_jobs(env, config, wes_token):
    queued_run_path = os.path.join(JOB_DIR, '*', '*', 'queued', 'job.*', f'run.*.{env}.wes-*')
    queued_runs = sorted(glob(queued_run_path))
    latest_run_per_job = dict()
    for run in queued_runs:
        job_id = os.path.basename(os.path.dirname(run))
        latest_run_per_job[job_id] = run

    for run in list(latest_run_per_job.values()):
        study, batch_id, _, job_id, run_info = run.split(os.sep)[-5:]
        run_id = run_info.split('.')[3]

        job_batch_path = os.path.join(JOB_DIR, study, batch_id)
        current_job_path = os.path.dirname(run)

        graphql_url = config['compute_environments'][env]['graphql_url']
        new_state = None

        try:
            run_info = get_run_state(graphql_url, run_id, wes_token)
            if run_info['state'] == 'COMPLETE':
                new_state = 'completed'
            elif 'ERROR' in run_info['state']:
                new_state = 'failed'
        except Exception as ex:
            message = f"{ex}\nSkipping update status for: {run_id}"
            print(message, file=sys.stderr)
            send_notification(message, 'CRITICAL', config)

        if new_state in ('completed', 'failed'):
            move_job_to_new_state(new_state, job_batch_path, current_job_path)

    return len(set([os.path.dirname(r) for r in glob(queued_run_path)]))


def get_studies_in_priority_order(studies):
    study_path_pattern = os.path.join(JOB_DIR, '*-*')
    all_studies = [os.path.basename(s) for s in sorted(glob(study_path_pattern))]

    studies_in_priority_order = []
    for s in studies:
        if s in all_studies:
            if s not in studies_in_priority_order:
                studies_in_priority_order.append(s)
        else:
            print(f"No jobs for specified study: {s}, ignoring it.", file=sys.stderr)

    for s in all_studies:
        if s not in studies_in_priority_order:
            studies_in_priority_order.append(s)

    return studies_in_priority_order


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(3))
def wes_submit_run(params, wes_url, wes_token, api_token, resume, workflow_url, workflow_version, nfs):
    # TODO: support resume request

    params['api_token'] = api_token
    wes_post_body = {
        "workflow_url": workflow_url,
        "workflow_params": params,
        "workflow_engine_params": {
            "revision": workflow_version,
            "project_dir": f"{nfs}/{workflow_version}",
            "launch_dir": f"{nfs}/wfuser/{workflow_version}",
            "work_dir": f"{nfs}/wfuser/{workflow_version}/work"
        }
    }

    response = requests.post(
                        url=wes_url,
                        json=wes_post_body,
                        headers={"Authorization": f"Bearer {wes_token}"}
                    )

    if response.status_code != 200:
        message = f"Run request failed, HTTP status code: {response.status_code}. More details: {response.text}"
        print(message, file=sys.stderr)
        raise Exception(message)
    else:
        return json.loads(response.text)['run_id']


def queue_new_jobs(available_slots, env, config, studies, wes_token):
    jobs_to_queue = []

    studies_in_priority_order = get_studies_in_priority_order(studies)
    for study in studies_in_priority_order:
        current_available_slots = available_slots - len(jobs_to_queue)
        if current_available_slots <= 0:
            break

        # find backlog jobs for study
        backlog_job_path = os.path.join(JOB_DIR, study, '*', 'backlog', 'job.*')
        backlog_jobs = sorted(glob(backlog_job_path))

        if len(backlog_jobs) > current_available_slots:
            jobs_to_queue += backlog_jobs[:current_available_slots]
        else:
            jobs_to_queue += backlog_jobs

    nfs = random.choice(config['compute_environments'][env]['nfs_root_paths'])
    wes_url = config['compute_environments'][env]['wes_url']
    workflow_url = config['workflow']['url']
    workflow_version = config['workflow']['version']
    api_token = os.environ.get(config['compute_environments'][env]['ENV']['api_token'])

    # now queue and move the job one-by-one
    for job in jobs_to_queue:
        # TODO: support resume, detect whether run info file exists, if so get session id
        resume = False  # set resume to the session id, set to None for now

        params = json.load(open(os.path.join(job, 'params.json'), 'r'))
        run_id = None
        try:
            run_id = wes_submit_run(params, wes_url, wes_token, api_token, resume, workflow_url, workflow_version, nfs)
        except Exception as ex:
            error_msg = f"Unable to launch new runs. {ex}"
            send_notification(error_msg, 'CRITICAL', config)
            sys.exit(error_msg)

        if run_id:  # submission was successful, now let's create run info file and move the job dir
            run_file = f'run.{int(time.time())}.{env}.{run_id}'
            open(os.path.join(job, run_file), 'a').close()
            # now move job to queued
            new_job_path = job.replace('backlog', 'queued')
            cmd = f'git add {job} && git mv {job} {new_job_path}'
            stdout, stderr, returncode = run_cmd(cmd)
            if returncode:
                error_msg = f"Error: job queued with run_id: {run_id} but failed to perform command: {cmd}"
                print(error_msg, file=sys.stderr)
                send_notification(error_msg, 'CRITICAL', config)
                sys.exit(error_msg)
            else:
                print(f"Queued job: {new_job_path}, run_id: {run_id}")


def excessive_failure(env, config):
    # detect excessive failure flag
    flag_file = f"excessive_failure.{env}.txt"
    if os.path.isfile(os.path.join(JOB_DIR, flag_file)):
        error_msg = f"Excessive run failure flag exists: {flag_file}, skip scheduling new runs."
        print(error_msg, file=sys.stderr)
        send_notification(error_msg, 'CRITICAL', config)
        return True

    recently_scheduled_runs = []
    recently_failed_runs = []
    cmd = "git status jobs |grep run"
    out, err, rc = run_cmd(cmd)
    recently_scheduled_runs = out.split('\n') if out else []
    if not recently_scheduled_runs:
        return False

    for run in recently_scheduled_runs:
        # run_path, eg, jobs/CLLE-ES/2021-07-08/failed/job.0119/run.1625958035.rdpc_qa.wes-ce11613478634a3eb1129dd11e71f90e
        run_path = run.split(' ')[-1]
        if 'failed' in run_path:
            recently_failed_runs.append(run_path.split('.')[-1])

    if len(recently_failed_runs) >= config['compute_environments'][env]['excessive_failure_threshold']:
        with open(os.path.join(JOB_DIR, flag_file), 'w') as f:
            f.write(
                f"recently_failed_runs: [{', '.join(recently_failed_runs)}]\n"
            )

        error_msg = f"Excessive run failure detected: {recently_failed_runs} recently scheduled runs failed."
        print(error_msg, file=sys.stderr)
        send_notification(error_msg, 'CRITICAL', config)
        return True

    return False


def schedule_jobs(env, config, studies):
    wes_token = get_wes_token(env, config)
    queued_job_count = update_queued_jobs(env, config, wes_token)

    available_slots = config['compute_environments'][env]['max_parallel_runs'] - queued_job_count
    if not excessive_failure(env, config) and available_slots:
        queue_new_jobs(available_slots, env, config, studies, wes_token)


def send_notification(message, level, config):
    print(f"Slack notification:\nLevel: {level}\n{message}", file=sys.stderr)

    envv = config['slack_notification']['ENV']['web_hook_url']
    slack_hook_url = os.getenv(envv)
    if not slack_hook_url:
        print(f"Please set environment variable '{envv}' for Slack web hook url.", file=sys.stderr)
        return

    if level == 'CRITICAL':
        emoji = ':fire:\n'
    elif level == 'WARNING':
        emoji = ':warning:\n'
    elif level == 'INFO':
        emoji = ':information_source:\n'
    else:
        emoji = ''

    response = requests.post(
        url=slack_hook_url,
        json={
            'username': 'scheduler',
            'text': f"{emoji}{message}"
        }
    )

    if response.status_code != 200:
        print(f"Unable to send Slack notification. Error: {response.text}", file=sys.stderr)
    else:
        print("Slack notification sent.")


def main(studies, config):
    pull_job_batches(config)

    for env in config['compute_environments']:
        schedule_jobs(env, config, studies)

    push_job_status(config)


if __name__ == '__main__':
    # make sure cwd is BASE_DIR
    os.chdir(BASE_DIR)

    parser = argparse.ArgumentParser(description='ICGC 25k data Azure transfer job generator')
    parser.add_argument('-s', '--studies', type=str, nargs='+',
                        help='SONG studies for preferred priority order')
    args = parser.parse_args()

    with open(CONFIG_FILE) as f:
        config = yaml.safe_load(f)

    studies = args.studies if args.studies else []

    main(studies, config)
