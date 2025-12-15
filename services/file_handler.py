import os
import services.memory_var as mem
from services.configs import Config

# get the latest active file, this needs to be store in memory 
def create_next_file():
    next_file_path = os.path.join(Config.STORAGE_PATH, f'{mem.next_file_id}.bin')
    open(next_file_path, "a").close()
    mem.next_file_id += 1
    return next_file_path




def get_cur_merge_file():
    if mem.current_merge_file is None or os.path.getsize(mem.current_merge_file) > Config.MAX_DATA_FILE_SIZE:
        mem.current_merge_file = create_next_file()
    return mem.current_merge_file

