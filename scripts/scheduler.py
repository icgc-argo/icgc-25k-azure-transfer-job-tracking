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


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(1))
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
        # we still need to make sure the current branch is 'scheduler'
        stdout, stderr, rc = run_cmd('git checkout scheduler > /dev/null 2>&1 && ( git stash pop > /dev/null 2>&1 || true )')
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

    report_summary_lines = job_status_changes + ['']  # add an empty line separator
    changed_studies = set([])
    for change in job_status_changes:
        changed_studies.add(change.split(' ')[0])

    for s in sorted(changed_studies):
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
       epoch_time_now - last_update_at < config['tracking_repo_push_interval'] * 60 * 60 - 300:  # allow 5 min earlier to compensate push time variation
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


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(1))
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
        headers={"Authorization": f"Bearer {wes_token}"},
        timeout=60
    )
    if response.status_code != 200:
        raise Exception(f"Unable to retrieve run state for {run_id} from {graphql_url}. Status code: {response.status_code}")

    response_obj = json.loads(response.text)
    run_info = response_obj['data']['runs']['content']
    if run_info:
        return run_info[0]
    else:
        raise Exception(f"GraphQL response does not return run details for {run_id} from {graphql_url}. Status code: 200")


def move_job_to_new_state(new_state, job_batch_path, current_job_path):
    new_job_path = os.path.join(job_batch_path, new_state)
    cmd = f'git mv {current_job_path} {new_job_path}'
    stdout, stderr, returncode = run_cmd(cmd)
    if returncode:
        print(f"Unable to perform: {cmd}", file=sys.stderr)
    else:
        print(f"Job status change, new state: {new_state}, job: {new_job_path}/{os.path.basename(current_job_path)}")


def update_queued_jobs(env, config, wes_token):
    queued_run_path = os.path.join(JOB_DIR, '*', '*', 'queued', 'job.*', f'run.*.*.wes-*')
    queued_runs = sorted(glob(queued_run_path))
    latest_run_per_job = dict()
    for run in queued_runs:
        job_path = os.path.dirname(run)
        latest_run_per_job[job_path] = run

    queued_run_count = 0
    for run in sorted(list(latest_run_per_job.values())):
        study, batch_id, _, job_id, run_file = run.split(os.sep)[-5:]
        run_env = run_file.split('.')[2]
        if run_env != env:  # skip if the run is not in the current env
            continue

        run_id = run_file.split('.')[3]
        queued_run_count += 1

        job_batch_path = os.path.join(JOB_DIR, study, batch_id)
        current_job_path = os.path.dirname(run)

        graphql_url = config['compute_environments'][env]['graphql_url']
        new_state = None

        try:
            run_info = get_run_state(graphql_url, run_id, wes_token)
            if run_info['state'] == 'COMPLETE':
                new_state = 'completed'
            elif 'ERROR' in run_info['state'] or 'FAILED' in run_info['state']:
                new_state = 'failed'
        except Exception as ex:
            message = f"{ex}\nSkipping update status for: {run_file}"
            print(message, file=sys.stderr)
            send_notification(message, 'CRITICAL', config)

        if new_state in ('completed', 'failed'):
            move_job_to_new_state(new_state, job_batch_path, current_job_path)
            queued_run_count -= 1

    return queued_run_count


def get_studies_in_priority_order(studies, exclude_studies):
    study_path_pattern = os.path.join(JOB_DIR, '*-*')
    all_studies = [os.path.basename(s) for s in sorted(glob(study_path_pattern)) if os.path.basename(s) not in exclude_studies]

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


@retry(reraise=True, wait=wait_random(min=5, max=15), stop=stop_after_attempt(1))
def wes_submit_run(params, wes_url, wes_token, api_token, resume, workflow_url, workflow_version, nfs, offline, work_dir, launch_dir):
    # support resume request

    params['api_token'] = api_token
    wes_post_body = {
        "workflow_url": workflow_url,
        "workflow_params": params,
        "workflow_engine_params": {
            "revision": workflow_version,
            "offline": True if offline else False,
            "project_dir": f"{nfs}/{workflow_version}",
            "launch_dir": f"{nfs}/wfuser/{workflow_version}" if not launch_dir else launch_dir,
            "work_dir": f"{nfs}/wfuser/{workflow_version}/work" if not work_dir else work_dir
        }
    }

    if resume:
      wes_post_body['workflow_engine_params']['resume'] = resume

    response = requests.post(
                        url=wes_url,
                        json=wes_post_body,
                        headers={"Authorization": f"Bearer {wes_token}"},
                        timeout=60
                    )

    if response.status_code != 200:
        message = f"Run request failed, HTTP status code: {response.status_code}. More details: {response.text}"
        print(message, file=sys.stderr)
        raise Exception(message)
    else:
        try:
            return json.loads(response.text)['run_id']
        except Exception as ex:
            message = f"Unable to load response text as JSON or 'run_id' field does not exist in the response. Exception is:\n{ex}\nResponse text is:\n{response.text}\n"
            print(message, file=sys.stderr)
            raise Exception(message)


def queue_new_jobs(available_slots, env, config, studies, wes_token, exclude_studies):
    jobs_to_queue = []

    studies_in_priority_order = get_studies_in_priority_order(studies, exclude_studies)
    for study in studies_in_priority_order:
        current_available_slots = available_slots - len(jobs_to_queue)
        if current_available_slots <= 0:
            break

        # find backlog jobs for study
        backlog_job_path = os.path.join(JOB_DIR, study, '*', 'backlog', 'job.*')
        backlog_jobs = sorted(glob(backlog_job_path))
        
        for job_path in backlog_jobs:
          exist_run_path = os.path.join(job_path, f'run.*.*.wes-*')
          exist_runs = sorted(glob(exist_run_path))
          # has failed run
          if exist_runs:              
            run_file = os.path.basename(exist_runs[-1])
            latest_env, latest_run_id = run_file.split('.')[-2:]
            if not latest_env == env: continue
          jobs_to_queue += job_path
          current_available_slots -= 1
          if current_available_slots <= 0:
            break

    nfs = random.choice(config['compute_environments'][env]['nfs_root_paths'])
    wes_url = config['compute_environments'][env]['wes_url']
    workflow_url = config['workflow']['url']
    workflow_version = config['workflow']['version']
    offline = config['workflow'].get('offline')
    api_token = os.environ.get(config['compute_environments'][env]['ENV']['api_token'])

    # now queue and move the job one-by-one
    for job in jobs_to_queue:
        # support resume, detect whether run info file exists, if so get session id
        resume = False  # set resume to the session id, set to None for now
        work_dir = None
        launch_dir = None

        exist_run_path = os.path.join(job, f'run.*.{env}.wes-*')
        exist_runs = sorted(glob(exist_run_path))
        if exist_runs:
          run_file = os.path.basename(exist_runs[-1])
          latest_run_id = run_file.split('.')[-1]
          graphql_url = config['compute_environments'][env]['graphql_url']
          try:
              run_info = get_run_state(graphql_url, latest_run_id, wes_token)
              resume = run_info['sessionId'] if run_info.get('sessionId') else False
              work_dir = run_info['engineParameters']['workDir']
              launch_dir = run_info['engineParameters']['launchDir']
              
          except Exception as ex:
              message = f"{ex}\nCan not get sessionId for: {run_file}"
              print(message, file=sys.stderr)
              send_notification(message, 'CRITICAL', config)
              continue

        params = json.load(open(os.path.join(job, 'params.json'), 'r'))
        run_id = None
        try:
            run_id = wes_submit_run(params, wes_url, wes_token, api_token, resume, workflow_url, workflow_version, nfs, offline, work_dir, launch_dir)
            time.sleep(5)  # pause for 5 seconds
        except Exception as ex:
            error_msg = f"Unable to launch new runs on '{env}'. {ex}"
            send_notification(error_msg, 'CRITICAL', config)

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
                print(f"Queued job: {new_job_path}, on: {env}, run_id: {run_id}")


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
    cmd = "git status jobs |grep run ||true"
    out, err, rc = run_cmd(cmd)
    recently_scheduled_runs = out.split('\n') if out else []
    if not recently_scheduled_runs:
        return False

    latest_run_per_job = dict()
    for run in sorted(recently_scheduled_runs):
        # run_path, eg, jobs/CLLE-ES/2021-07-08/failed/job.0119/run.1625958035.rdpc_qa.wes-ce11613478634a3eb1129dd11e71f90e
        run_path = run.split(' ')[-1]
        job_path = os.path.dirname(run_path)
        latest_run_per_job[job_path] = run_path

    for run_path in sorted(list(latest_run_per_job.values())):
        if 'failed' in run_path and env == run_path.split('.')[-2]:
            recently_failed_runs.append(os.path.basename(run_path))

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


def env_paused(env):
    pause_flag_file = os.path.join(JOB_DIR, f"pause.{env}")
    if os.path.isfile(pause_flag_file):
        message = f"Pause flag exists, skip scheduling new runs on compute environment '{env}'."
        send_notification(message, 'INFO', config)
        return True
    else:
        return False


def schedule_jobs(env, config, studies, exclude_studies):
    wes_token = ""
    try:
        wes_token = get_wes_token(env, config)
    except Exception as ex:
        error_msg = f"Unable to get token for '{env}'. Error: {ex}"
        send_notification(error_msg, 'CRITICAL', config)

    queued_job_count = update_queued_jobs(env, config, wes_token)

    available_slots = config['compute_environments'][env]['max_parallel_runs'] - queued_job_count
    if not excessive_failure(env, config) and not env_paused(env) and available_slots:
        queue_new_jobs(available_slots, env, config, studies, wes_token, exclude_studies)


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
        },
        timeout=60
    )

    if response.status_code != 200:
        print(f"Unable to send Slack notification. Error: {response.text}", file=sys.stderr)
    else:
        print("Slack notification sent.")


def main(studies, config, exclude_studies):
    pull_job_batches(config)

    for env in config['compute_environments']:
        schedule_jobs(env, config, studies, exclude_studies)

    push_job_status(config)


if __name__ == '__main__':
    # make sure cwd is BASE_DIR
    os.chdir(BASE_DIR)

    parser = argparse.ArgumentParser(description='ICGC 25k data Azure transfer job generator')
    parser.add_argument('-s', '--studies', type=str, nargs='+',
                        help='SONG studies for preferred priority order')
    parser.add_argument('-x', '--exclude-studies', type=str, nargs='+',
                        help='SONG studies to be excluded')
    args = parser.parse_args()

    with open(CONFIG_FILE) as f:
        config = yaml.safe_load(f)

    studies = args.studies if args.studies else []
    exclude_studies = args.exclude_studies if args.exclude_studies else []

    main(studies, config, exclude_studies)
