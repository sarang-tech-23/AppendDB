import os
from services.configs import Config
import services.memory_var as mem
from services.serialization import encode_record, decode_record
from services.file_handler import get_cur_merge_file


def run_compaction():
    if (mem.bytes_on_disk / mem.bytes_on_ram) > 2:
        # get list of all files accept current_active_file
        cur_data_files = [
            f for f in os.listdir(Config.STORAGE_PATH)
            if f.endswith(".bin") and f != mem.current_active_file.split('/')[-1]
        ]
        keydir_snapshot = {**mem.keydir}
        for key, value in keydir_snapshot.items():
            file_path = value['file_path']
            offset = value['key_pos']

            key, value, timestamp = None, None, None
            full_file_path = os.path.join(Config.STORAGE_PATH, file_path)
            with open(full_file_path, 'rb') as f:
                f.seek(offset)
                key, value, timestamp = decode_record(f)

            write_merges(key, value, timestamp)

        # delete old data files
        for f in cur_data_files:
            os.remove(os.path.join(Config.STORAGE_PATH, f))

        mem.current_merge_file = None
            
def write_merges(key, value, timestamp):
    cur_merge_file = get_cur_merge_file()
    record, _ = encode_record(key, value, timestamp)

    cur_offset = 0
    with open(cur_merge_file, 'wb') as f:
        cur_offset = f.tell()
        f.write(record)

    # todo: use lock for this update
    cur_timestamp_of_key = mem.keydir[key]['timestamp']
    if cur_timestamp_of_key <= timestamp:
        mem.keydir[key] = {'file_path': cur_merge_file.split('/')[-1], 'key_pos': cur_offset, 'timestamp': timestamp }
