# Primula (plug) 
## Pluggable Primula algorithm

### Installation

```bash
pip install -e .
lithops runtime build -f Dockerfile primula-profiling:0.1
```

### Usage

Once per machine and Lithops configuration (storage profiling data is stored at your home directory)...
```python
from primula import Primula

prim = Primula()
prim.setup(bucket_name = "my_bucket")
prim.create_model()
```

Then, to infer each time...
```python
GB = 1024 * 1024 * 1024
D = 100 * GB
predicted_workers_read, predicted_time_read = prim.infer_read(D)
predicted_workers_write, predicted_time_write = prim.infer_write(D)
```


