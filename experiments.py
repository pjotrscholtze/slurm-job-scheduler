import json
from glob import glob
from entity import Experiment

def get_experiments(directory_path):
    names = []
    for p in glob("%s/*.json" % directory_path):
        with open(p, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                p = Experiment(**data)
                if p.name in names: raise Exception("Experiment name should always be unique!")
                names.append(p.name)
                yield p
            else:
                for item in data:
                    p = Experiment(**item)
                    if p.name in names: raise Exception("Experiment name should always be unique!")
                    names.append(p.name)
                    yield p