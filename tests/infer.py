from primula.infer.data import read_samples
from primula.infer.model import Model
from primula.infer.Infer import infer_all_to_all, infer_read, infer_write
from primula.profile import GB

samples = read_samples()

model = Model()

model.gen_models(samples)

# Inference examples
D = 100 * GB
predicted_p, predicted_time = infer_all_to_all(D, model)
print("Prediction for all-to-all shuffle of %d GB: "%(D/GB), predicted_p, " workers", predicted_time, " seconds")

# Inference examples
D = 50 * GB
predicted_p, predicted_time = infer_write(D, model, requests_per_worker=10)
print("Prediction for write of %d GB: "%(D/GB), predicted_p, " workers", predicted_time, " seconds")


D = 150 * GB
predicted_p, predicted_time = infer_read(D, model, requests_per_worker=50)
print("Prediction for read of %d GB: "%(D/GB), predicted_p, " workers", predicted_time, " seconds")