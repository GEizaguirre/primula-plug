import numpy as np
from primula.profile import PRIMULA_HOME_DIR
import pickle
from lithops.config import load_config
import os
import datetime

from primula.profile.profile import MB

def get_throughput(data: dict):
    return data["ops"] / ( data["end_time"] - data["start_time"] )

def get_bandwidth(data: dict):
    return data["bytes"] / ( data["end_time"] - data["start_time"] ) / MB


def read_samples(dir: str = None):
    
    storage = load_config()['lithops']['storage']
    if dir is None:
        dir = os.path.join(PRIMULA_HOME_DIR, storage)
    
    samples = {"samples": [], "date": datetime.date.today().strftime("%Y-%m-%d")}

    files = []
    for file in os.listdir(dir):
        
        if file.endswith("write.pickle"):
            files.append(os.path.join(dir, file))
            

    for file in files:
        
        write_file = file
        read_file = file.replace("write", "read")
        
        write_data = pickle.load(open(write_file, "rb"))
        workers = len(write_data["results"])
        file_size = write_data["mb_per_file"]
        
        print(workers)
        print(file_size)
        
        write_throughput = sum([ get_throughput(d) for d in write_data["results"] ])
    
        worker_write_bandwidth = np.mean([ get_bandwidth(d) for d in write_data["results"] ])
        print("Worker write bandwidth: %.2f" % worker_write_bandwidth)
        
        read_data = pickle.load(open(read_file, "rb"))
        read_throughput = sum([ get_throughput(d) for d in read_data["results"] ])
        
        worker_read_bandwidth = np.mean([ get_bandwidth(d) for d in read_data["results"] ])
        print("Worker read bandwidth: %.2f" % worker_read_bandwidth)
        
        samples["samples"].append({"workers": workers, 
                                   "write": write_throughput,
                                   "read": read_throughput,
                                   "write_bandwidth": worker_write_bandwidth,
                                   "read_bandwidth": worker_read_bandwidth,
                                   "file_size": file_size})

        
    return samples
    
    
        
        
        
        
        
    
    
    
