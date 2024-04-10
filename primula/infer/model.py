from operator import itemgetter
from typing import List, Tuple, Dict
#from matplotlib import pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.preprocessing import PolynomialFeatures
from primula.profile import MB
from matplotlib import pyplot as plt

DEGREES = [3, 4, 5, 6]
LINEAR_THRESHOLD = 0.95
MAXIMUM_WORKERS = 10**4


class Model():

    file_size: int
    models_read: List[LinearRegression]
    models_write: List[LinearRegression]
    thresholds_read: List[int]
    thresholds_write: List[int]
    
    bandwidth_read: int
    bandwidth_write: int
    
    read_data: Dict[int, Dict[int, float]]
    write_data: Dict[int, Dict[int, float]]
    
    def __init__(self):
        pass
    
    def gen_models(self, data):
    
        self.get_data_model(data)

        self.models_write = {
            file_size : self._model(self.write_data[file_size]) for file_size in self.write_data.keys()
        }
        self.models_read = {
            file_size : self._model(self.read_data[file_size]) for file_size in self.read_data.keys()
        }
        

        self.bandwidth_write = {
            file_size : self._get_bandwidth(self.write_data[file_size], file_size) for file_size in self.write_data.keys()
        }
        self.bandwidth_read = {
            file_size : self._get_bandwidth(self.read_data[file_size], file_size) for file_size in self.read_data.keys()
        }
        
    
    def _model(self, data) -> Tuple[List[LinearRegression], List[int]]:
        
    
        num_entries = len(data.keys())
    
        # Find threshold in which the regression has degree 1
        _r2 = LINEAR_THRESHOLD
        r1_thr = 2
        model = None
        while _r2 >= LINEAR_THRESHOLD and r1_thr < num_entries:
            model1 = model
            data_lin = {k: data[k] for k in list(data.keys())[:(r1_thr + 1)]}
    
            model, _r2 = self._regression(data_lin, 1)
    
            r1_thr += 1
            
    
        if r1_thr == 3:
            # Need a minimal of 3 points to have a lineal model
            model1_points = 0
            threshold_workers = 0
        elif r1_thr == num_entries:
            model1_points = num_entries
            threshold_workers = MAXIMUM_WORKERS
        else:
            model1_points = r1_thr - 2
            threshold_workers = int(list(data.keys())[model1_points])
    
        models = []
        
        
        degrees = [d for d in DEGREES if d < len(list(data.keys())[model1_points:])]
    
        if len(degrees) > 0 and model1_points < num_entries:
            # From r1_thr and ahead, the regression will have degree > 2
            
            for d in degrees:
                data_lin = {k: data[k] for k in list(data.keys())[model1_points:]}
    
                model, _r2 = self._regression(data_lin, d)
    
                models.append([model, _r2])
    
            model2 = max(models, key=itemgetter(1))[0]
        else:
            model2 = None
            
    
        return [[model1, model2], [threshold_workers]]
    
    def _regression(self, data, degree):
    
        poly = PolynomialFeatures(degree=degree, include_bias=False)
    
        x = np.array(list(data.keys()))
        y = np.array(list(data.values()))
    
        poly_features = poly.fit_transform(x.reshape(-1, 1))
    
        poly_reg_model = LinearRegression()
    
        poly_reg_model.fit(poly_features, y)
    
        y_true = y
    
        poly_features = poly.fit_transform(x.reshape(-1, 1))
        y_pred = poly_reg_model.predict(poly_features)
    
        r2 = r2_score(y_true, y_pred)
    
        return poly_reg_model, r2
    
    def _get_bandwidth(self, throughput_samples, file_size: int):
    
        """
        @param throughput_samples: dict of throughput samples { p: throughput }
        @param file_size: file size in MB
        @return: maximum per-worker bandwidth
        """
        
        bandwidths = [ ( t / p ) * file_size for p, t in throughput_samples.items() ]
    
        return max(bandwidths)
    
    def get_read_bandwidth(self, file_size: int = None):

        if file_size is not None:

            available_file_size = self.define_available_file_size(file_size)
            
            return self.bandwidth_read[available_file_size]

        else:

            return np.mean(list(self.bandwidth_read.values()))
    
    def get_write_bandwidth(self, file_size: int = None):

        if file_size is not None:

            available_file_size = self.define_available_file_size(file_size)

            return self.bandwidth_write[available_file_size]
        
        else:

            return np.mean(list(self.bandwidth_write.values()))
    
    def define_available_file_size(self, file_size: float):
        
        available_files = list(self.models_write.keys())
        
        available_file_size = min(available_files, key=lambda x: abs(x - file_size))
        
        return available_file_size
    
    def get_throughput(self, models, thresholds, p):
        
        model_num = np.searchsorted(thresholds, p)
        model = models[model_num]
    
        poly = PolynomialFeatures(degree=model.rank_, include_bias=False)
        poly_features = poly.fit_transform([[p]])
    
        pred = model.predict(poly_features)

        return pred[0]
    
    def get_throughput_read(self, p: int, file_size: float):

        
        
        if file_size is None:
            
            throughputs = [
                self.get_throughput(self.models_read[fs][0], self.models_read[fs][1], p)
                for fs in self.models_read.keys()
            ]
            
            return np.mean(throughputs)
            
        else:
    
            available_file_size = self.define_available_file_size(file_size)
        
            return self.get_throughput(self.models_read[available_file_size][0], self.models_read[available_file_size][1], p)
    
    def get_throughput_write(self, p: int, file_size: float):
        
        if file_size is None:
            
            throughputs = [
                self.get_throughput(self.models_write[fs][0], self.models_write[fs][1], p)
                for fs in self.models_write.keys()
            ]
            
            return np.mean(throughputs)
            
        else:
    
            available_file_size = self.define_available_file_size(file_size)
        
            return self.get_throughput(self.models_write[available_file_size][0], self.models_write[available_file_size][1], p)
    
    def get_data_model(self, data: Dict) -> Tuple[Dict[int, float], Dict[int, float]]:
        
        """
        Calculates the average write and read data for each number of workers based on the given data.

        Args:
            data (Dict): A dictionary containing storages samples in the following format:
                {
                    'samples': [
                        {
                            'workers': int,
                            'write': float,
                            'read': float,
                            'file_size': int
                        },
                        ...
                    ]
                }
        """
        
        write_data = dict()
        read_data = dict()
        for s in data['samples']:
            
            file_size = s["file_size"]
            
            if file_size not in write_data.keys():
                write_data[file_size] = dict()
                read_data[file_size] = dict()
            
            w = s["workers"]
            
            if s["workers"] not in write_data[file_size].keys():
                write_data[file_size][w] = []
                
            if s["workers"] not in read_data[file_size].keys():
                read_data[file_size][w] = []
                
            write_data[file_size][w].append(s["write"])
            read_data[file_size][w].append(s["read"])
        
    
        self.write_data = dict()
        self.read_data = dict()
        for file_size in write_data:
            write_data[file_size] = {w: np.mean(write_data[file_size][w]) for w in write_data[file_size].keys()}
            self.write_data[file_size] = dict(sorted(write_data[file_size].items()))
            read_data[file_size] = {w: np.mean(read_data[file_size][w]) for w in read_data[file_size].keys()}
            self.read_data[file_size] = dict(sorted(read_data[file_size].items()))
    

    def plot_points(self):
    
        for file_size in self.write_data.keys():
            
            plt.plot(list(self.write_data[file_size].keys()),
                    [ v/1000 for v in list(self.write_data[file_size].values())],
                    marker = "o",
                    label = f"{file_size} MB")
            plt.ylabel("KOP/s")
            plt.xlabel("# functions")
        
        plt.title("Write")
        plt.legend()
        plt.show()
            
        for file_size in self.read_data.keys():
    
            plt.plot(list(self.read_data[file_size].keys()),
                    [ v/1000 for v in list(self.read_data[file_size].values())],
                    marker = "o",
                    label = f"{file_size} MB")
            plt.ylabel("KOP/s")
            plt.xlabel("# functions")
        
        plt.title("Read")
        plt.legend()
        plt.show()
        
        