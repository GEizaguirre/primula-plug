from primula.profile import PRIMULA_HOME_DIR
import os
import pickle

primula_home_dir = os.path.expanduser(PRIMULA_HOME_DIR)

matching_files = [file for file in os.listdir(primula_home_dir) if file.endswith("read.pickle")]

if matching_files:
    file_path = os.path.join(primula_home_dir, matching_files[0])
    with open(file_path, "rb") as file:
        unpickled_data = pickle.load(file)
else:
    print("No matching file found.")
    
print(unpickled_data.keys())