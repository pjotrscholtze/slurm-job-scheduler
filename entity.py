from glob import glob
import os
import git
import subprocess
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")

class Config:
    def __init__(self, secret, chat_id, experiment_folder):
        self.secret = secret
        self.chat_id = chat_id
        self.experiment_folder = experiment_folder

class Paths:
    def __init__(self, project_parent, build_cache, database):
        self.project_parent = project_parent
        self.build_cache = build_cache
        self.database = database
    
    def get_project_path(self, project_name: str):
        return self.project_parent + project_name

class Experiment:
    def __init__(self, name: str, description: str, script, github_url: str, github_checkout: str):
        self.name = name
        self.description = description
        self.script = script
        self.github_url = github_url
        self.github_checkout = github_checkout
        self.is_prepared = False
        self.job_id = None
    
    def build_trident(self, project_path):
        subprocess.call("cmake . -DSPARQL=1", cwd=project_path, shell=True)
        subprocess.call("make", cwd=project_path, shell=True)

    def exists(self, paths: Paths):
        return os.path.exists(paths.get_project_path(self.name))

    def prepare(self, paths: Paths):
        project_commit_hash_file = paths.get_project_path(self.name) + "/project-commit-hash.txt"

        os.mkdir(paths.get_project_path(self.name))
        g = git.Git(paths.build_cache)
        g.setup(self.github_checkout)

        with open(project_commit_hash_file, "w") as f:
            f.writelines([g.get_commit_hash()])

        self.build_trident(paths.build_cache + "/trident")

    def submit(self, partition, paths: Paths, slurm, telegram):
        sbatch_file = paths.get_project_path(self.name) + "/sbatch.sh"
        sbatch_output_file = paths.get_project_path(self.name) + "/slurm-%j.out"
        with open(sbatch_file, "w") as f:
            f.writelines("\n".join(self.script). \
                replace("#SBATCH -p longq", "#SLURM -p %s" % partition). \
                replace("$PROJECT_PATH", paths.get_project_path(self.name)). \
                replace("$BUILD_CACHE_PATH", paths.build_cache). \
                replace("$DATABASE_PATH", paths.database))

        stdout = slurm.sbatch(sbatch_file, sbatch_output_file)
        with open(paths.get_project_path(self.name) + "/job_id", "w") as f:
            f.writelines([stdout])
        self.job_id = stdout[len("Submitted batch job "):]

        msg = "Submitted experiment: %s (jobid: %s)" % (self.name, self.job_id)
        logging.info(msg)

        return self.job_id


class Job:
    def __init__(self, jobid, partition, name, user, state, time, time_limi,
        nodes, nodelist, *kwargs):
        self.jobid = jobid
        self.partition = partition
        self.name = name
        self.user = user
        self.state = state
        self.time = time
        self.time_limi = time_limi
        self.nodes = nodes
        self.nodelist = nodelist
        self.finished = False
    
    def update(self, job):
        res = []
        if self.state != job.state:
            res.append(("state", self.state, job.state))
            self.state = job.state
        if self.nodes != job.nodes:
            res.append(("nodes", self.nodes, job.nodes))
            self.nodes = job.nodes
        if self.nodelist != job.nodelist:
            res.append(("nodelist", self.nodelist, job.nodelist))
            self.nodelist = job.nodelist
        return res

    def get_experiment_dir(self):
        found = None
        for job_id_file in glob("/var/scratch/pse740/*/job_id"):
            with open(job_id_file, "r") as f:
                if f.readline() == "Submitted batch job %s" % self.jobid:
                    found = os.path.dirname(job_id_file)
                    break
        return found

    @staticmethod
    def prase_row(row):
        return Job(*row)