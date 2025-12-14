import os
from services.configs import Config
from services.serialization import decode_record

keydir = {}
current_active_file = None

def generate_keydir():
    storage_dir = Config.STORAGE_PATH
    os.makedirs(storage_dir, exist_ok=True)

    # Get all .bin files and sort them
    files = [f for f in os.listdir(storage_dir) if f.endswith(".bin")]
    files.sort() 

    # Read each file and build keydir
    for fname in files:
        file_path = os.path.join(storage_dir, fname)
        file_id = fname  # or int(fname.split(".")[0])

        with open(file_path, "rb") as f:
            offset = 0
            while True:
                key, value, timestamp = decode_record(f)
                if key is None:
                    break
                keydir[key] = {'file_path': file_id, 'key_pos': offset }
                offset = f.tell()

    # when no data files are stored
    if len(files) == 0:
        current_active_file = os.path.join(storage_dir, "1.bin")
        open(current_active_file, "a").close()
    else:
        current_active_file = os.path.join(storage_dir, files[-1])
        print(f'>> existing_file_as_active_file: {current_active_file}')
