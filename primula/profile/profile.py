

import datetime
from typing import List, Union
import uuid
import numpy as np
import time
import hashlib
import pickle
import os

from lithops import FunctionExecutor, Storage
from lithops.config import load_config
import datetime

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

home_dir = os.path.expanduser("~")
PRIMULA_HOME_DIR = os.path.join(home_dir, ".primula")
DATA_PER_WORKER = 200 * MB
WAIT_INTERVAL = 5

RUNTIME_NAME = "primula-profiling:0.1"




class RandomDataGenerator(object):
    """
    A file-like object which generates random data.
    1. Never actually keeps all the data in memory so
    can be used to generate huge files.
    2. Actually generates random data to eliminate
    false metrics based on compression.

    It does this by generating data in 1MB blocks
    from np.random where each block is seeded with
    the block number.
    """

    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.pos = 0
        self.current_block_id = None
        self.current_block_data = ""
        self.BLOCK_SIZE_BYTES = 1024*1024
        self.block_random = np.random.randint(0, 256, dtype=np.uint8,
                                              size=self.BLOCK_SIZE_BYTES)

    def __len__(self):
        return self.bytes_total

    @property
    def len(self):
        return self.bytes_total 

    def tell(self):
        return self.pos

    def seek(self, pos, whence=0):
        if whence == 0:
            self.pos = pos
        elif whence == 1:
            self.pos += pos
        elif whence == 2:
            self.pos = self.bytes_total - pos

    def get_block(self, block_id):
        if block_id == self.current_block_id:
            return self.current_block_data

        self.current_block_id = block_id
        self.current_block_data = (block_id + self.block_random).tostring()
        return self.current_block_data

    def get_block_coords(self, abs_pos):
        block_id = abs_pos // self.BLOCK_SIZE_BYTES
        within_block_pos = abs_pos - block_id * self.BLOCK_SIZE_BYTES
        return block_id, within_block_pos

    def read(self, bytes_requested):
        remaining_bytes = self.bytes_total - self.pos
        if remaining_bytes == 0:
            return b''

        bytes_out = min(remaining_bytes, bytes_requested)
        start_pos = self.pos

        byte_data = b''
        byte_pos = 0
        while byte_pos < bytes_out:
            abs_pos = start_pos + byte_pos
            bytes_remaining = bytes_out - byte_pos

            block_id, within_block_pos = self.get_block_coords(abs_pos)
            block = self.get_block(block_id)
            # how many bytes can we copy?
            chunk = block[within_block_pos:within_block_pos + bytes_remaining]

            byte_data += chunk

            byte_pos += len(chunk)

        self.pos += bytes_out

        return byte_data


def synchro_simple(synchro_time: datetime.datetime, synchro_timezone):
    
    local_datetime = datetime.datetime.now()
    local_datetime = local_datetime.astimezone(synchro_timezone)
    while local_datetime < synchro_time:
        local_datetime = datetime.datetime.now()
        local_datetime = local_datetime.astimezone(synchro_timezone)
        time.sleep(0.1)



def write(bucket_name: str,
        mb_per_file: int, 
        number_of_functions: int, 
        runtime_memory: int = 1769, 
        number_of_files: int = None, 
        debug: bool = False):
    
    if number_of_files is None:
        number_of_files = DATA_PER_WORKER // (mb_per_file * MB)
    
    # create list of random keys
    keynames = [ 
                [ "%d%d%d%d"%(f, f, f, f) + str(uuid.uuid4().hex.upper()) for _ in range(number_of_files) ] 
                for f in range(number_of_functions)
                ]

    log_level = 'INFO' if not debug else 'DEBUG'
    fexec = FunctionExecutor(runtime_memory=runtime_memory, log_level=log_level, runtime = RUNTIME_NAME)
    
    
    begin_datetime = datetime.datetime.now().astimezone() + datetime.timedelta(seconds=WAIT_INTERVAL)
    

    def writer(key_names: List[str]):
        
        storage = Storage()
        
        bytes_n = mb_per_file * MB
        
        synchro_simple(begin_datetime, begin_datetime.tzinfo)
        
        before_time = time.time()
        
        for key_name in key_names:
            
            d = RandomDataGenerator(bytes_n)
            storage.put_object(bucket_name, key_name, d)
            
        after_time = time.time()
        Ops = len(key_names)/(after_time-before_time)
        print('WOP/s : '+str(Ops))

        return {'start_time': before_time, 'end_time': after_time, 'ops': Ops, 'bytes': bytes_n*len(key_names)}

    
    start_time = time.time()
    worker_futures = fexec.map(writer, keynames)
    results = fexec.get_result(throw_except=False)
    end_time = time.time()
    
    total_time = end_time-start_time
    results = [gbs for gbs in results if gbs is not None]
    worker_stats = [f.stats for f in worker_futures if not f.error]

    res = {'start_time': start_time,
           'total_time': total_time,
           'worker_stats': worker_stats,
           'bucket_name': bucket_name,
           'keynames': keynames,
           'results': results,
           "runtime_memory": runtime_memory,
           "mb_per_file": mb_per_file,
           "number_of_files": number_of_files}

    return res


def read(bucket_name: str,
        keylist: List[List[str]],
        mb_per_file: int, 
        number_of_files: int = None,
        runtime_memory: int = 1769, 
        debug: bool = False):
    
    blocksize = 1024*1024
    
    log_level = 'INFO' if not debug else 'DEBUG'
    fexec = FunctionExecutor(runtime_memory=runtime_memory, log_level=log_level, runtime = RUNTIME_NAME)
    
    begin_datetime = datetime.datetime.now().astimezone() + datetime.timedelta(seconds=WAIT_INTERVAL)

    def reader(keys):
        
        storage = Storage()
        # m = hashlib.md5()
        bytes_read = 0
        
        synchro_simple(begin_datetime, begin_datetime.tzinfo)

        start_time = time.time()
        for key_name in keys:
            fileobj = storage.get_object(bucket_name, key_name, stream=True)
            
            try:
                buf = fileobj.read(blocksize)
                
                
                while len(buf) > 0:
                    bytes_read += len(buf)
                    #if bytes_read % (blocksize *10) == 0:
                    #    mb_rate = bytes_read/(time.time()-t1)/1e6
                    #    print('POS:'+str(bytes_read)+' MB Rate: '+ str(mb_rate))
                    # m.update(buf)
                    buf = fileobj.read(blocksize)
            except Exception as e:
                print(e)
                pass
        end_time = time.time()

        Ops = len(keys)/(end_time-start_time)
        print('ROP/s : '+str(Ops))

        return {'start_time': start_time, 'end_time': end_time, 'ops': Ops, 'bytes': bytes_read}
 

    
    start_time = time.time()
    worker_futures = fexec.map(reader, keylist)
    results = fexec.get_result(throw_except=False)
    end_time = time.time()
    total_time = end_time-start_time

    results = [gbs for gbs in results if gbs is not None]
    
    worker_stats = [f.stats for f in worker_futures if not f.error]

    res = {'start_time': start_time,
           'total_time': total_time,
           'worker_stats': worker_stats,
           'results': results,
           "runtime_memory": runtime_memory,
           "mb_per_file": mb_per_file,
           "number_of_files": number_of_files
           }

    return res


def warm_up(runtime_memory: int, 
            number_of_functions: int):

    def foo(x):
        return x

    fexec = FunctionExecutor(runtime_memory=runtime_memory, runtime=RUNTIME_NAME)
    worker_futures = fexec.map(foo, range(number_of_functions))
    results = fexec.get_result(throw_except=False)


def delete_temp_data(bucket_name: str, 
                     keynames: Union[List[str], List[List[str]]],):
    
    if isinstance(keynames[0], list):
        keynames = [ key for keynames in keynames for key in keynames ]
        
    print('Deleting temp files...')
    storage = Storage()
    try:
        storage.delete_objects(bucket_name, keynames)
    except Exception as e:
        print(e)
        pass
    print('Done!')


def delete_command(key_file: str, 
                   outdir: str, 
                   name: str):
    
    if key_file:
        res_write = pickle.load(open(key_file, 'rb'))
    else:
        res_write = pickle.load(open('{}/{}_write.pickle'.format(outdir, name), 'rb'))
        
    bucket_name = res_write['bucket_name']
    keynames = [ key for keynames in res_write['keynames'] for key in keynames ]
    
    delete_temp_data(bucket_name, keynames)


def _profile(bucket_name: str, 
             mb_per_file: int, 
             number_of_functions: int,
             runtime_memory: int = 1769,
             number_of_files: int = None, 
             debug: bool = False, 
             replica_number: int = 1):
    
    config = load_config()
    
    storage = config["lithops"]["storage"]
    date = datetime.datetime.now().strftime("%d-%m-%Y")

    storage_dir = os.path.join(PRIMULA_HOME_DIR, storage)
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
        
    for r_n in range(replica_number):
    
        fname = f"{storage}/{number_of_functions}_{mb_per_file}_{date}_{r_n}"

        print('Executing Write Test:')
        
        res_write = write(bucket_name, mb_per_file, number_of_functions, runtime_memory, number_of_files, debug)
        
        pickle.dump(res_write, open(f'{PRIMULA_HOME_DIR}/{fname}_write.pickle', 'wb'), -1)
        print('Sleeping 4 seconds...')
        time.sleep(4)
        print('Executing Read Test:')

        res_read = read(bucket_name, res_write["keynames"], mb_per_file, number_of_files, runtime_memory, debug)
        pickle.dump(res_read, open(f'{PRIMULA_HOME_DIR}/{fname}_read.pickle', 'wb'), -1)

        delete_temp_data(bucket_name, res_write["keynames"])
    
    
def profile(bucket_name: str,
             mb_per_file: int, 
             functions: List[int],
             runtime_memory: int = 1769,
             number_of_files: int = None,
             debug: bool = False,
             replica_number: int = 1):
    
    sorted_functions = sorted(functions, reverse=True)
    print(f"Warming up {functions[0]} functions")
    warm_up(runtime_memory, max(functions))
    
    for num_functions in sorted_functions:
        
        print(f"Profiling {num_functions} functions")
        _profile(bucket_name, mb_per_file, num_functions, runtime_memory, number_of_files, debug, replica_number)

bucket_name="german-lithops"