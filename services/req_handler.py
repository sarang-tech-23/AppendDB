import os
from services.serialization import encode_record, decode_record
import services.memory_var as mem
from services.configs import Config

def process_req_data(data):
    req_type, kv_data = data.split(':::')

    if req_type == 'write':
        return handle_write(kv_data)
    elif req_type == 'delete':
        return handle_delete(kv_data)
    elif req_type == 'read':
        return handle_read(kv_data)
    else:
        return b'invalid request type'

# get the latest active file, this needs to be store in memory 
def create_next_file(file_path):
    file_prefix_int = int(file_path.split('/')[-1][:-4])
    next_file_prefix_int = file_prefix_int + 1
    next_file_path = os.path.join(Config.STORAGE_PATH, f'{next_file_prefix_int}.bin')
    open(next_file_path, "a").close()
    return next_file_path

def get_current_active_file():
    # create new file and return
    file_size_bytes = os.path.getsize(mem.current_active_file)

    print(f'>>> new_bin_file_cond:::{file_size_bytes > Config.MAX_DATA_FILE_SIZE}')
    print(f'>>> {file_size_bytes}__{type(file_size_bytes)} :::: {Config.MAX_DATA_FILE_SIZE}___{type(Config.MAX_DATA_FILE_SIZE)}')
    if file_size_bytes > Config.MAX_DATA_FILE_SIZE:
        mem.current_active_file = create_next_file(mem.current_active_file)
    print(f'>>> current_active_file:::{mem.current_active_file}     file_size::::{file_size_bytes}')
    return mem.current_active_file


def handle_write(payload):
    payload = payload.rstrip('\n')
    key, value = payload.split(':')
    record = encode_record(key, value)
    log_file = get_current_active_file()

    cur_offset = 0
    with open(log_file, 'ab') as f:
        cur_offset = f.tell()
        f.write(record)
    file_name = log_file.split('/')[-1]
    mem.keydir[key] = {'file_path': file_name, 'key_pos': cur_offset }
    return b'ack'

def handle_delete(payload):
    key = payload.rstrip('\n')
    value = ""
    record = encode_record(key, value)
    log_file = get_current_active_file()

    cur_offset = 0
    with open(log_file, 'ab') as f:
        cur_offset = f.tell()
        f.write(record)
    file_name = log_file.split('/')[-1]
    mem.keydir[key] = {'file_path': file_name, 'key_pos': cur_offset }
    return b'ack'

def handle_read(payload):
    key = payload.rstrip('\n')

    key_info = mem.keydir.get(key)
    if key_info is None:
        return b'data not available'
    
    log_file = key_info['file_path']
    offset = key_info['key_pos']

    file_path = os.path.join(Config.STORAGE_PATH, log_file)
    with open(file_path, 'rb') as f:
        f.seek(offset)
        key, value, _ = decode_record(f)
        return value.encode()
  
