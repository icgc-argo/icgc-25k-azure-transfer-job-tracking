# Repository to track ICGC 25K Azure data transfer jobs

Data transfer job organization:
  1. all jobs are under `jobs` folder
  2. jobs are divided by studies, use study_id as subfolder name under `jobs` folder
  3. each study can further divide into batches, a batch is name by date in the format of 'YYYY-MM-DD'
  4. each batch has a `batch-summary.tsv` file that lists all SONG analysis IDs and their assignment
     to job_id. SONG analysis ID must be unique across all batches to avoid duplicate transfer

Configuration settings for job scheduler:
```
  job_tracking_repo: git@github.com:icgc-argo/icgc-25k-azure-transfer-job-tracking.git
  tracking_repo_push_interval: 12  # in hours
  compute_environments:
      rdpc_prod:
        wes_url: xxxxx
        max_parallel_runs: 2
      rdpc_qa:
        wes_url: xxxxx
        max_parallel_runs: 4
  scheduling_interval: 30  # in minutes
```

Job scheduler command line options:
  studies: []  # list of study IDs

Job scheduler procedure (run by cron) for each of the compute environments (update to the `scheduler` branch only):
  0. bring in latest backlog jobs:
    `git checkout main && git pull && git checkout scheduler && git merge main`
  1. check statuses of every queued jobs from WES endpoind
  2. if corresponding run is completed, verify all analysis objects are in PUBLISHED state at Azure,
     then move the job to `completed` folder
  3. if corresponding run is failed, move job to `failed` folder
  4. if all jobs started within the most recent two scheduling windows are failed, likely something
     wrong with the compute environment, send alert and stop schduling cycle
  5. get job count still in `queued` folder, stop job scheduling if it's greater than or
     equals to `max_parallel_runs`
  6. pickup a new job from `backlog` folder, start it (or resume it if it was failed previously) and
     then move the job to `queued` folder
  7. repeat step 6 until `queued` job count equals `max_parallel_runs`

Job tracking repo push procedure from scheduler:
  1. determine when was the last push, stop if the interval is less than `tracking_repo_push_interval`
  2. `git checkout main && git pull && git checkout scheduler && git merge main`
  3. `git add jobs && git commit -m '[scheduler] update job status' && git push`

Within 'jobs' direcotry, do this to report job status periodically:
```
for s in *-*;
do
  for f in `echo backlog queued failed completed`;
    do echo -n "$s $f "; ls $s/*/$f/*/params.json 2>/dev/null |wc -l |sed 's/ //g';
  done;
done
```

Example output, which can be sent to a Slack channel:
```
PACA-CA backlog 1
PACA-CA queued 2
PACA-CA failed 1
PACA-CA completed 1
```

Tracking repo updates by admins (update to the `main` branch only):
  1. NEVER touch the `queued` directory which is used only by the `scheduler`
  2. before any update, update to the latest `scheduler` branch and merge into `main`:
     `git checkout scheduler && git pull && git checkout main && git merge scheduler`
  3. do any updates as necessary, eg, adding more jobs; move failed jobs to backlog for resume; then push
