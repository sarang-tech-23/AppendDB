# bitcask implemetation
# append only database, 
'''
-> folder which contains append only logs (file)
-> key/value store in memory which maps key to file and id
-> hint files
-> merge compaction
-> delete tombstone
'''

import os
import time
import zlib
import struct

class Reader():
    def __init__(self):
        self.DB_DATA_DIR = 'db_dir'

    def from_bytes(self, data):
        # Read type tag (1 byte) and length (4 bytes)
        type_tag, length = struct.unpack('>BI', data[:5])
        # Extract the actual data
        value_bytes = data[5:5 + length]
        if type_tag == 0:  # int
            return int.from_bytes(value_bytes, byteorder='big')
        elif type_tag == 1:  # str
            return value_bytes.decode('utf-8')
        else:
            raise TypeError("Unknown type tag")

    def read_key(self, file_name, key):
        data = None
        with open(file_name, 'rb') as f:
            f.seek(0)
            data = f.read(28)

        if data is not None:
            ts, key_len, value_len = struct.unpack('>QHI', data[:14])
            key = self.from_bytes(data[14: 14+key_len])
            value = self.from_bytes(data[14+key_len:])

            print(f'->> key: {key}    value:{value}')
            




class Writer():
    '''
    - create data dir if not present
    - search for the mutable file to write (append)
    - if not create one
    - append the data
    - make the file mutable is it has reached max size
    '''
    def __init__(self):
        self.DB_DATA_DIR = 'db_dir'
        self.MAX_FILE_SIZE = 10

    def to_bytes(self, data):
        if isinstance(data, int):
            # Store type tag (1 byte: 0 for int) + length (4 bytes) + data
            length = (data.bit_length() + 7) // 8
            return struct.pack('>BI', 0, length) + data.to_bytes(length, byteorder='big')
        elif isinstance(data, str):
            # Store type tag (1 byte: 1 for str) + length (4 bytes) + data
            encoded = data.encode('utf-8')
            return struct.pack('>BI', 1, len(encoded)) + encoded
        else:
            raise TypeError("Input must be int or str")

    def append_to_logs(file_name, data):
        with open(file_name, 'a') as f:
            f.append(data)

    def get_mutable_file(self):
        for file in os.listdir(self.DB_DATA_DIR):
            file_path = os.path.join(self.DB_DATA_DIR, file)
            if os.path.isfile(file_path) and os.access(file_path, os.W_OK):
                return file
        return None
        
    def write(self, key, value):
        print(f'key: {key}    value: {value}')
        key_bytes = self.to_bytes(key)
        value_bytes = self.to_bytes(value)

        key_len = len(key_bytes)
        value_len = len(value_bytes)
        print(f'-> key_bytes_len: {key_len}')
        print(f'-> value_bytes_len: {value_len}')

        # merge the info and convert to bytes
        ts = int(time.time_ns())
        header = struct.pack('>QHI', ts, key_len, value_len)  
        record = header + key_bytes + value_bytes
        print(f'header_len: {len(header)}')

        if not os.path.exists(self.DB_DATA_DIR):
            # Create the directory
            os.makedirs(self.DB_DATA_DIR)

        log_file_name = self.get_mutable_file()
        if log_file_name is None:
            log_file_name = str(time.time_ns())

        log_file_path = os.path.join(self.DB_DATA_DIR, log_file_name)
        print(f'->> log_file_path: {log_file_path}')
        with open(log_file_path, 'ab') as f:
            # Move to the start of the file to read
            f.seek(0, os.SEEK_END)
            f.write(record)

            eof = f.tell()
            print(f'->> eof_vlaue__>> {eof}')

        
        


        


# log_writer = Writer()

# for i in range(2, 3):
#     val = 'a_'+str(i)
#     log_writer.write(i, val)


# log_reader = Reader()
# log_reader.read_key('/Users/sarangpatil/Documents/developer/sarang/projects/spcask/db_dir/1754849496868096000', 'a_2')