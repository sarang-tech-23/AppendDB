import os
from services.serialization import encode_record, decode_record
import services.memory_var as mem
from services.configs import Config


def process_req_data(data):
    req_type, kv_data = data.split(':::')

    if req_type in ['write', 'update']:
        key, value = kv_data.split(':')
        value = value.rstrip('\n')
        segment_writer(key, value)
        return b'ack=True'
    elif req_type in ['delete']:
        pass
    elif req_type in ['read']:
        key = kv_data.rstrip('\n')
        _ , value = segment_reader(key)
        return value.encode()
    else:
        return b'invalid request type'

def segment_writer(key, value):
    # get the latest active file, this needs to be store in memory 
    def create_next_file(file_path):
        file_prefix_int = file_path.split('/')[-1][:-4].isdigit()
        next_file_prefix_int = file_prefix_int + 1
        next_file_path = os.path.join(Config.STORAGE_PATH, f'{next_file_prefix_int}.bin')
        open(next_file_path, "a").close()
        return next_file_path

    def get_current_active_file():
        # create new file and return
        file_size_bytes = os.path.getsize(mem.current_active_file)

        if file_size_bytes > Config.MAX_DATA_FILE_SIZE:
            mem.current_active_file = create_next_file(mem.current_active_file)

        return mem.current_active_file
    

    record = encode_record(key, value)
    log_file = get_current_active_file()

    cur_offset = 0
    with open(log_file, 'ab') as f:
        cur_offset = f.tell()
        f.write(record)

    mem.keydir[key] = {'file_path': log_file, 'key_pos': cur_offset }

    
    
def segment_reader(key):
    # get the file details from in memory hash map
    key_info = mem.keydir.get(key)
    if key_info is None:
        raise 'data not available'
    
    log_file = key_info['file_path']
    offset = key_info['key_pos']

    file_path = os.path.join(Config.STORAGE_PATH, log_file)
    with open(file_path, 'rb') as f:
        f.seek(offset)
        key, value, _ = decode_record(f)
        return key, value


    