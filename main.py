import socket
from services.req_handler import process_req_data
from services.memory_var import generate_keydir

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
