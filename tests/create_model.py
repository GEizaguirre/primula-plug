from primula.infer.data import read_samples
from primula.infer.model import Model

samples = read_samples()

model = Model()

model.gen_models(samples)

model.plot_points()