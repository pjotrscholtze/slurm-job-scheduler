from entity import Job
import subprocess

class Slurm:
    def squeue(self) -> str: return ""

    def squeue_to_jobs(self):
        data = self.squeue()
        HEADER_ENDING = "NODELIST(REASON)"
        rows = data[data.index(HEADER_ENDING)+len(HEADER_ENDING)+1:].split("\n")

        return [Job.prase_row(row) for row in [row.strip().split() for row in rows] if row]

    def sinfo(self) -> str: return ""

    def sinfo_get_idle_partitions(self):
        data = self.sinfo()
        HEADER_ENDING = "NODELIST"
        rows = data[data.index(HEADER_ENDING)+len(HEADER_ENDING)+1:].split("\n")
        COL_NAME = 0
        COL_AVAIL = 3
        COL_STATE = 4
        temp = [row.strip().split() for row in rows]
        parsed = []
        for row in temp:
            if row[COL_NAME] == "defq*": row[COL_NAME] = "defq"
            parsed.append({
                "name": row[COL_NAME],
                "avail": row[COL_AVAIL],
                "state": row[COL_STATE]
            })
        res = {}
        for line in parsed:
            if line["state"] == "idle":
                res[line["name"]] = line["avail"]

        return res

    def sbatch(self, sbatch_file: str, sbatch_output_file: str) -> str: pass

class SlurmMock(Slurm):
    def __init__(self):
        self._jobid = 2964443
        self._jobs = []
        self.avail = {
            "longq":1,
            "defq":60
        }
    def squeue(self) -> str:
        new_jobs = []
        for j in self._jobs:
            j["time"]-=1
            if j["time"] > 0:
                new_jobs.append(j)
            else:
                self.avail[j["partition"]]+=1
        self._jobs = new_jobs
        t = """Wed Feb 17 10:03:54 2021
JOBID PARTITION     NAME     USER    STATE       TIME TIME_LIMI  NODES NODELIST(REASON)
2961883      defq Pretrain   rwr850  PENDING       0:00  15:00:00      5 (BeginTime)
2961884      defq Pretrain   rwr850  PENDING       0:00  15:00:00      5 (BeginTime)
2961885      defq Pretrain   rwr850  PENDING       0:00 2-15:00:00      5 (BeginTime)
2964437      defq Pretrain   hcl700  PENDING       0:00  15:00:00      3 (BeginTime)
2959903     longq  am+.job   wwe300  RUNNING 1-05:48:30 7-00:00:00      1 node072
2959904     longq  am+.job   wwe300  RUNNING 1-07:42:30 7-00:00:00      1 node073
2963087      knlq  dmg.job   wwe300  RUNNING   22:51:10 7-00:00:00      1 node077
2964414      defq prun-job   ajs870  RUNNING   11:27:18 4-00:01:00      2 node[049-050]
2964443      defq prun-job   versto  RUNNING      17:01     31:00      1 node007""" + ("\n%s" % "\n".join(["%s      %s prun-job   pse740  RUNNING      00:%s     00:15      1 node007" % (j["jobid"], j["partition"], j["time"]) for j in self._jobs]))
        return t
    def sinfo(self) -> str:
        return """PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
defq*        up   infinite      8  alloc node[007,049-050,053-057]
defq*        up   infinite     %d   idle node[001-006,008-048,051-052,058-068]
longq        up   infinite      1  down* node074
longq        up   infinite      2  alloc node[072-073]
longq        up   infinite     %d   idle node075
knlq         up   infinite      1  alloc node077
knlq         up   infinite      1   idle node076
proq         up   infinite      4   idle node[069-071,078]""" % (self.avail["defq"], self.avail["longq"])

    def sbatch(self, sbatch_file: str, sbatch_output_file: str) -> str:
        parts = []
        with open(sbatch_file, "r") as f: parts = [l for l in f.readlines() if l.startswith("#SLURM -p ")]
        part = parts[0][10:].strip()
        self._jobid += 1
        self._jobs.append({
            "jobid": self._jobid,
            "time": 15,
            "partition": part
        })
        self.avail[part]-=1
        return "Submitted batch job %s" % self._jobid

class SlurmProd(Slurm):
    def squeue(self) -> str: return subprocess.getoutput("squeue -l")

    def sinfo(self) -> str: return subprocess.getoutput("sinfo")

    def sbatch(self, sbatch_file: str, sbatch_output_file: str) -> str: 
        cmd = "sbatch %s -o %s" % (sbatch_file, sbatch_output_file)
        return subprocess.getoutput(cmd)
