import os
from services.configs import Config
from services.serialization import decode_record

keydir = {}
current_active_file = None
next_file_id = 1 # used create_next_file function
current_merge_file = None

# needed for compaction
bytes_on_ram = 0
bytes_on_disk = 0

def generate_keydir():
    global keydir
    global current_active_file
    global next_file_id
    global bytes_on_ram
    global bytes_on_disk
    
    os.makedirs(Config.STORAGE_PATH, exist_ok=True)
    files = [f for f in os.listdir(Config.STORAGE_PATH) if f.endswith(".bin")]
    files.sort() 

    # Read each file and build keydir
    for fname in files:
        file_path = os.path.join(Config.STORAGE_PATH, fname)
        file_id = fname  # or int(fname.split(".")[0])

        with open(file_path, "rb") as f:
            offset = 0
            while True:
                key, value, timestamp = decode_record(f)
                if key is None:
                    break
                keydir[key] = {'file_path': file_id, 'key_pos': offset, 'timestamp': timestamp }
                offset = f.tell()
                bytes_on_disk += len(key)

    # when no data files are stored
    if len(files) == 0:
        current_active_file = os.path.join(Config.STORAGE_PATH, "1.bin")
        open(current_active_file, "a").close()
        next_file_id = 2
    else:
        current_active_file = os.path.join(Config.STORAGE_PATH, files[-1])
        print(f'>> existing_file_as_active_file: {current_active_file}')
        next_file_id = int(current_active_file.split('/')[-1].split('.')[0]) + 1

    for key in keydir.keys():
        bytes_on_ram += len(key)
