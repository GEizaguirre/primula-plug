import os
import pickle

import logging
from math import ceil
from typing import Tuple

from primula.profile import MB
from primula.infer.model import Model
from lithops.config import load_config

MAX_PARTITION_SIZE = 250
MIN_PARTITION_SIZE = 25
MINIMUM_WORKER_MEMORY_PERC = 0.05
MAXIMUM_WORKER_MEMORY_PERC = 0.25
WORKER_NUMBER_STEP = 1


def infer_write(D: int, model: Model, p: int = None, requests_per_worker: int = 1, mb_per_file: float = None):

    dmb = D / MB

    if p is not None:
        io_time = eq_io(dmb, p, requests_per_worker,
                      model.get_write_bandwidth(mb_per_file if mb_per_file is not None else dmb / p / requests_per_worker ),
                      model.get_throughput_write(p, mb_per_file if mb_per_file is not None else dmb / p / requests_per_worker ))

        return p, io_time
    
    else:
        
        lithops_config = load_config()
        compute_backend = lithops_config['lithops']['backend']
        if compute_backend in lithops_config and "runtime_memory" in lithops_config[compute_backend]:
            worker_memory = lithops_config[compute_backend]["runtime_memory"]
            max_partition_size = int( worker_memory * MB * MAXIMUM_WORKER_MEMORY_PERC )
            min_partition_size = int( worker_memory * MB * MINIMUM_WORKER_MEMORY_PERC )
        else:
            max_partition_size = MAX_PARTITION_SIZE * MB 
            min_partition_size = MIN_PARTITION_SIZE * MB
            
            

        # We establish the minimum and maximum worker number
        # to analyze as multiples of the worker number
        # step.
        minimum_number_worker = ceil(D / max_partition_size)
        minimum_number_worker = max(minimum_number_worker, 3)
        maximum_number_worker = ceil(D / min_partition_size)
        maximum_number_worker = max(min(maximum_number_worker, 1000), 3)

        predicted_times_io = {
            w: eq_io(dmb, w, requests_per_worker,
                      model.get_write_bandwidth(mb_per_file if mb_per_file is not None else dmb / w / requests_per_worker),
                      model.get_throughput_write(w, mb_per_file if mb_per_file is not None else dmb / w / requests_per_worker ))
            for w in range(minimum_number_worker,
                        maximum_number_worker + 1,
                        WORKER_NUMBER_STEP)
        }

        io_time = max(predicted_times_io.values())
        optimal_worker_number_io = max(predicted_times_io, key=predicted_times_io.get)

        for w, val in predicted_times_io.items():
            if val < io_time:
                optimal_worker_number_io = w
                io_time = val

        return optimal_worker_number_io, io_time


def infer_read(D: int, model: Model, p: int = None, requests_per_worker: int = 1, mb_per_file: float = None):

    dmb = D / MB

    if p is not None:
        io_time = eq_io(dmb, p, requests_per_worker,
                      model.get_read_bandwidth(mb_per_file if mb_per_file is not None else dmb / p / requests_per_worker ),
                      model.get_throughput_read(p, mb_per_file if mb_per_file is not None else dmb / p / requests_per_worker ))

        return p, io_time
    
    else:
        
        lithops_config = load_config()
        compute_backend = lithops_config['lithops']['backend']
        if compute_backend in lithops_config and  "runtime_memory" in lithops_config[compute_backend]:
            worker_memory = lithops_config[compute_backend]["runtime_memory"]
            max_partition_size = int( worker_memory * MB * MAXIMUM_WORKER_MEMORY_PERC )
            min_partition_size = int( worker_memory * MB * MINIMUM_WORKER_MEMORY_PERC )
        else:
            max_partition_size = MAX_PARTITION_SIZE * MB 
            min_partition_size = MIN_PARTITION_SIZE * MB
            
            

        # We establish the minimum and maximum worker number
        # to analyze as multiples of the worker number
        # step.
        minimum_number_worker = ceil(D / max_partition_size)
        minimum_number_worker = max(minimum_number_worker, 3)
        maximum_number_worker = ceil(D / min_partition_size)
        maximum_number_worker = max(min(maximum_number_worker, 1000), 3)

        predicted_times_io = {
            w: eq_io(dmb, w, requests_per_worker,
                      model.get_read_bandwidth( mb_per_file if ( mb_per_file is not None ) else ( dmb / w / requests_per_worker ) ),
                      model.get_throughput_read(w, mb_per_file if mb_per_file is not None else dmb / w / requests_per_worker))
            for w in range(minimum_number_worker,
                        maximum_number_worker + 1,
                        WORKER_NUMBER_STEP)
        }

        io_time = max(predicted_times_io.values())
        optimal_worker_number_io = max(predicted_times_io, key=predicted_times_io.get)

        for w, val in predicted_times_io.items():
            if val < io_time:
                optimal_worker_number_io = w
                io_time = val

        return optimal_worker_number_io, io_time



def infer_all_to_all(D: int, model: Model, p: int = None, mb_per_file: float = None) -> Tuple[int, float]:

    dmb = D / MB

    if p is not None:
        shuffle_time = eq_shuffle(dmb, p,
                      model.get_read_bandwidth(mb_per_file if mb_per_file is not None else dmb / p / p),
                      model.get_write_bandwidth(mb_per_file if mb_per_file is not None else dmb / p / p),
                      model.get_throughput_read(p, mb_per_file if mb_per_file is not None else dmb / p / p),
                      model.get_throughput_write(p, mb_per_file if mb_per_file is not None else dmb / p / p))

        return p, shuffle_time
    
    else:
    
        lithops_config = load_config()
        
        compute_backend = lithops_config['lithops']['backend']
        if compute_backend in lithops_config and "runtime_memory" in lithops_config[compute_backend]:
            worker_memory = lithops_config[compute_backend]["runtime_memory"]
            max_partition_size = int( worker_memory * MB * MAXIMUM_WORKER_MEMORY_PERC )
            min_partition_size = int( worker_memory * MB * MINIMUM_WORKER_MEMORY_PERC )
        else:
            max_partition_size = MAX_PARTITION_SIZE * MB 
            min_partition_size = MIN_PARTITION_SIZE * MB
            
            

        # We establish the minimum and maximum worker number
        # to analyze as multiples of the worker number
        # step.
        minimum_number_worker = ceil(D / max_partition_size)
        minimum_number_worker = max(minimum_number_worker, 3)
        maximum_number_worker = ceil(D / min_partition_size)
        maximum_number_worker = max(min(maximum_number_worker, 1000), 3)


        predicted_times_shuffle = {
            w: eq_shuffle(dmb, w,
                        model.get_read_bandwidth( mb_per_file if mb_per_file is not None else dmb / w / w),
                        model.get_write_bandwidth( mb_per_file if mb_per_file is not None else dmb / w / w),
                        model.get_throughput_read(w, mb_per_file if mb_per_file is not None else dmb / w / w),
                        model.get_throughput_write(w, mb_per_file if mb_per_file is not None else dmb / w / w))
            for w in range(minimum_number_worker,
                        maximum_number_worker + 1,
                        WORKER_NUMBER_STEP)
        }


        shuffle_time = max(predicted_times_shuffle.values())
        optimal_worker_number_shuffle = max(predicted_times_shuffle, key=predicted_times_shuffle.get)

        for w, val in predicted_times_shuffle.items():
            if val < shuffle_time:
                optimal_worker_number_shuffle = w
                shuffle_time = val

        return optimal_worker_number_shuffle, shuffle_time


def eq_shuffle(D, p, bandwidth_read, bandwidth_write, throughput_read, throughput_write):

    return max(D / (bandwidth_write * p), p ** 2 / throughput_write ) + \
           max(D / (bandwidth_read * p), p ** 2 / throughput_read)


def eq_io(D, p, requests_per_worker, bandwidth_per_worker, agg_throughput):
        
    return max(D / (bandwidth_per_worker * p), p * requests_per_worker / agg_throughput )