from primula.profile import PRIMULA_HOME_DIR
import pickle
from lithops.config import load_config
import os
import datetime

def get_throughput(data: dict):
    return data["ops"] / ( data["end_time"] - data["start_time"] )

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
        write_throughput = sum([ get_throughput(d) for d in write_data["results"] ])
        
        read_data = pickle.load(open(read_file, "rb"))
        read_throughput = sum([ get_throughput(d) for d in read_data["results"] ])
        
        samples["samples"].append({"workers": workers, "write": write_throughput, "read": read_throughput, "file_size": file_size})

        
    return samples
    
    
        
        
        
        
        
    
    
    
