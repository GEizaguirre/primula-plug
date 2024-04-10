from primula import Primula


prim = Primula()


prim.setup()
prim.create_model()

GB = 1024 * 1024 * 1024
D = 100 * GB
predicted_workers_read, predicted_time_read = prim.infer_read(D)
print("Prediction for read of %d GB: "%(D/GB), predicted_workers_read, " workers", predicted_time_read, " seconds")
predicted_workers_write, predicted_time_write = prim.infer_write(D)
print("Prediction for write of %d GB: "%(D/GB), predicted_workers_write, " workers", predicted_time_write, " seconds")


predicted_workers_read, predicted_time_read = prim.infer_read(D, 500, requests_per_worker=10, mb_per_file=8)
print("Prediction for read of %d GB: "%(D/GB), predicted_workers_read, " workers", predicted_time_read, " seconds")
predicted_workers_write, predicted_time_write = prim.infer_write(D, 200, requests_per_worker=2, mb_per_file=45)
print("Prediction for write of %d GB: "%(D/GB), predicted_workers_write, " workers", predicted_time_write, " seconds")