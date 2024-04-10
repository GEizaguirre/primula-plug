from lithops import Storage
from primula.profile import profile

storage = Storage()

storage.create_bucket("sandbox")


profile(bucket_name="sandbox",
        mb_per_file=1,
        functions = [1,2,4,6,8,10],
        runtime_memory=256,
        number_of_files=3,
        replica_number=2)