import sys
import threading
import time
import telegram
import entity
import json
from scheduler import Scheduler
from slurm import SlurmMock, SlurmProd
import experiments

class Main:
    def __init__(self, mode, config_path: str):
        self.mode = mode
        self.config = entity.Config(**json.load(open(config_path, "r")))
        self.telegram = telegram.Telegram(self.config.secret, self.config.chat_id)
        threading.Thread(target=self.telegram.start).start()
        self.slurm = SlurmMock()
        if self.mode == "prod": self.slurm = SlurmProd()
    
    def start(self):
        scheduler = Scheduler(self.slurm, "pse740", self.telegram)
        threading.Thread(target=scheduler.start).start()
        while True:
            input("Press [enter] to update experiment list")
            scheduler.experiments_informer(list(experiments.get_experiments("temp")))
            self.telegram.new_message("test")
            print("x")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("main.py [prod|dev] <configfilepath>")
        sys.exit(0)
    MODE = sys.argv[1]
    CONFIG_PATH = sys.argv[2]
    Main(MODE, CONFIG_PATH).start()
