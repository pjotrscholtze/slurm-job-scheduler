from slurm import Slurm
import time
import threading
import logging
from entity import Experiment, Paths
from telegram import Telegram
import subprocess
import datetime
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")

class Limit:
    def __init__(self, defq, longq):
        self.limits = {
            "defq": defq,
            "longq": longq
        }
    
    def _get_partition_jobs(self, partition, running_jobs, idle_partitions):
        partition_jobs = [j for j in running_jobs if j.partition == partition]
        if partition not in idle_partitions: idle_partitions[partition] = 0
        return max(min(self.limits[partition] - len(partition_jobs), int(idle_partitions[partition])), 0)

    def get_allocation(self, running_jobs, idle_partitions):
        longq = self._get_partition_jobs("longq", running_jobs, idle_partitions)
        defq = self._get_partition_jobs("defq", running_jobs, idle_partitions)
        return {"longq": longq, "defq": defq}

class Scheduler:
    def __init__(self, slurm: Slurm, username: str, telegram: Telegram):
        self.slurm = slurm
        self.jobs = {}
        self.username = username
        self._itt_count = 0
        self._finished = []
        self.experiments = []
        self.telegram = telegram
        self.experiments_lock = threading.Lock()
        self.paths = Paths("/var/scratch/pse740/", "/var/scratch/pse740/cache/", "/var/scratch/pse740/db/")
    
    @property
    def limit(self):
        hour = datetime.datetime.now().hour
        if hour >= 8 and hour < 20:
            return Limit(1, 3)
        return Limit(5, 3)

    def _ensure_prepare(self, experiment: Experiment):
        if not experiment.is_prepared:
            msg = "Preparing experiment: %s" % experiment.name
            logging.info(msg)
            self.telegram.new_message(msg)
            experiment.prepare(self.paths)
            experiment.is_prepared = True

    def _alloc(self, partition, amount):
        if amount == 0: return
        self.experiments_lock.acquire()
        experiments = self.experiments[:amount]
        self.experiments = self.experiments[amount:]
        peek_experiment = None
        if self.experiments: peek_experiment = self.experiments[0]
        self.experiments_lock.release()
        [self._ensure_prepare(e) for e in experiments]
        [e.submit(partition, self.paths, self.slurm, self.telegram) for e in experiments]
        if peek_experiment:
            self._ensure_prepare(peek_experiment)

    def experiments_informer(self, experiments):
        experiments = [e for e in experiments if not e.exists(self.paths) or e.is_prepared]
        self.experiments_lock.acquire()
        self.experiments = experiments
        self.experiments_lock.release()

    def check_finished(self, running_jobs):
        updates = [] # type: (str, List[(str, str, str)])
        new_jobs = [] # type: List[Job]
        job_names = []
        job_ids = []
        job_count = 0
        for job in running_jobs:
            job_names.append(job.name)
            job_ids.append(job.jobid)
            job_count += 1
            if job.jobid not in self.jobs:
                self.jobs[job.jobid] = job
                new_jobs.append(job)
            else:
                changes = self.jobs[job.jobid].update(job)
                if changes: updates.append((job.name, changes))
        just_finished_jobs = []
        for job_id in self.jobs:
            if job_id not in self._finished and job_id not in job_ids:
                self.jobs[job_id].finished = True
                self._finished.append(job_id)
                just_finished_jobs.append(job_id)
        msg = self._make_message(updates, new_jobs, job_count, just_finished_jobs)
        if msg:
            self.telegram.new_message(msg)
            logging.info(msg)
        if self._itt_count % 10 == 0:
            logging.info("Still checking round #%d" % self._itt_count)
        self._itt_count += 1

    def _make_message(self, updates: (str, any), new_jobs, job_count, just_finished_jobs):
        res = []
        if new_jobs:
            res.append("*New jobs:*")
            for job in new_jobs:
                res.append("- %s [%s] id: %s" % (job.name, job.state, job.jobid))
        if updates:
            if res: res.append(" ")
            res.append("*Job updates:*")
            for job in updates:
                for change in job[1]:
                    res.append("- %s ['%s': '%s' -> '%s']" % (job[0], change[0], change[1], change[2]))
        if just_finished_jobs:
            if res: res.append(" ")
            res.append("*Just finished:*")
            for jobname in just_finished_jobs:
                dir = self.jobs[jobname].get_experiment_dir()
                if dir:
                    res.append("- %s | last few lines:```" % jobname)
                    stdout = subprocess.getoutput("tail %s/slurm_*.out -n 10" % dir)
                    res += stdout.split("\n")
                    res.append("```")
                else:
                    res.append("- %s" % jobname)

        if self._itt_count % 600 == 0:
            res.append("Still monitoring _%d_ jobs" % job_count)
        return "\n".join(res)

    def start(self):
        while True:
            running_jobs = [j for j in self.slurm.squeue_to_jobs() if j.user == self.username]
            idle_partitions = self.slurm.sinfo_get_idle_partitions()
            avail_alloc = self.limit.get_allocation(running_jobs, idle_partitions)
            for partition in avail_alloc:
                self._alloc(partition, avail_alloc[partition])
            
            self.check_finished(running_jobs)

            time.sleep(1)
