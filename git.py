import subprocess
class Git:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir

    def setup(self, checkout: str):
        subprocess.call("git pull", cwd=self.project_dir + "/trident", shell=True)
        subprocess.call("git checkout %s" % checkout, cwd=self.project_dir + "/trident", shell=True)
    
    def get_commit_hash(self) -> str:
        cmd = "git rev-parse HEAD"
        data = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT, cwd=self.project_dir + "/trident")
        if data[-1:] == '\n': data = data[:-1]
        return data
