'''
AppendDB, bitcask implementation

Client method:- read, write, update, delete
Args (key, value)

Server -> listening for crud calls, simpcple inbuilt tcp based server, python client only 

-> distinguish between the crud ops
-> send the file to segmentWriter
-> segMentWriter makes the entry an return details
-> this detail sotre to in memory hashmap, and then act to clients call

-> functnality to built in-memory hasmap at server statup

-> functionality to compact files at regular intervals
'''

import socket, os
from services.configs import Config
from services.serialization import decode_record
# from services.memory_var import keydir, current_active_file
import services.memory_var as mem
from services.request_processor import process_req_data

def conn_handler(conn):
    def recv_data():
        msg_bytes = b""
        while True:
            chunk = conn.recv(1024)
            if not chunk:
                break

            msg_bytes += chunk
            if b"\n" in chunk:
                break

        return msg_bytes.decode().strip()
    
    req_data = recv_data()
    
    print(f'clint_req: {req_data}')
    response = process_req_data(req_data) + b'\n'
    conn.send(response)

def generate_keydir():
    storage_dir = Config.STORAGE_PATH

    # create storage_dir if not present
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
                mem.keydir[key] = {'file_path': file_id, 'key_pos': offset }
                offset = f.tell()


            

    # todo: update keydir

    # when no data files are stored
    if len(files) == 0:
        mem.current_active_file = os.path.join(storage_dir, "1.bin")
        open(mem.current_active_file, "a").close()
    else:
        mem.current_active_file = os.path.join(storage_dir, files[-1])
        print(f'>> existing_file_as_active_file: {mem.current_active_file}')


def start_server(host="0.0.0.0", port=9001):
    # read old files to build keydir, sort the files in aascending order
    generate_keydir()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(200)

    print(f"Server listening on {host}:{port}")

    while True:
        conn, addr = server.accept()
        print(f"Client connected: {addr}")
        conn_handler(conn)
        # connection will be closed from the client

if __name__ == "__main__":
    start_server()
