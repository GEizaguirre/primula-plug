from primula.profile import profile
from lithops.config import load_config
from primula.infer.data import read_samples
from primula.infer.model import Model
from primula.infer.Infer import infer_all_to_all, infer_read, infer_write
import os
from primula.profile import PRIMULA_HOME_DIR
import pickle
from typing import List

DEFAULT_PROFILE_WORKERS = [ 200, 400, 600, 800, 1000 ]
DEFAULT_FILE_SIZES =  [5, 25]

class Primula():

    def __init__(self):
        pass

    def setup(self, bucket_name: str = None, force: bool = False, workers: List[int] = DEFAULT_PROFILE_WORKERS, file_sizes: List[int] = DEFAULT_FILE_SIZES):
              
        lithops_config = load_config()
        storage_backend = lithops_config['lithops']['storage']

        if bucket_name is None:
           if storage_backend in lithops_config and "storage_bucket" in lithops_config[storage_backend]:
               bucket_name = lithops_config[storage_backend]["storage_bucket"]
               
        if bucket_name is None:
            raise ValueError("Bucket name is not provided and it is not found in the config file")
        print("Performing setup for backend: ", storage_backend, " against bucket: ", bucket_name)
               
               
        model_path = os.path.join(PRIMULA_HOME_DIR, storage_backend, "model.pickle")
        
        if not force and os.path.exists(model_path):
            print("Model for backend {storage_backend} already exists. Do you still want to profile the backend?")
            user_input = input("(Y/n): ")
            if user_input.lower() == "n": return
            
        
        for file_size in file_sizes:
            profile(bucket_name=bucket_name,
                    mb_per_file=file_size,
                    functions = workers,
                    runtime_memory=1769,
                    number_of_files=50//file_size,
                    replica_number=1)

        
    def create_model(self, force: bool = False):
        
        storage_backend = load_config()['lithops']['storage']
        
        model_path = os.path.join(PRIMULA_HOME_DIR, storage_backend, "model.pickle")

        
        if not force and os.path.exists(model_path):
            print("Model for backend {storage_backend} already exists. Do you still want to create a new one?")
            user_input = input("(Y/n): ")
            if user_input.lower() == "n": return

        samples = read_samples()

        model = Model()

        model.gen_models(samples)


        pickle.dump(model, open(model_path, "wb"))

    def infer_all_to_all(self, D: int, p: int = None, mb_per_file: float = None):

        storage = load_config()['lithops']['storage']
        model_path = os.path.join(PRIMULA_HOME_DIR, storage, "model.pickle")
        model = pickle.load(open(model_path, "rb"))

        return infer_all_to_all(D, model, p, mb_per_file)
    
    def infer_write(self, D: int, p: int = None, requests_per_worker: int = 1, mb_per_file: float = None):

        storage = load_config()['lithops']['storage']
        model_path = os.path.join(PRIMULA_HOME_DIR, storage, "model.pickle")
        model = pickle.load(open(model_path, "rb"))

        return infer_write(D, model, p, requests_per_worker, mb_per_file)
    
    def infer_read(self, D: int, p: int = None, requests_per_worker: int = 1, mb_per_file: float = None):

        storage = load_config()['lithops']['storage']
        model_path = os.path.join(PRIMULA_HOME_DIR, storage, "model.pickle")
        model = pickle.load(open(model_path, "rb"))

        return infer_read(D, model, p, requests_per_worker, mb_per_file)



        

        

