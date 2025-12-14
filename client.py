import socket


class AppendDbClient():
    def __init__(self, host='0.0.0.0', port=9001):
        self.sock_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_conn.connect((host, port))

    def write_data(self, key, val):
        msg = f'write:::{key}:{val}\n'
        msg_bytes = msg.encode()
        self.sock_conn.send(msg_bytes)
        response = self.sock_conn.recv(1024)
        return response
    
    def update_data(self, key, val):
        msg = f'update:::{key}:{val}\n'
        msg_bytes = msg.encode()
        self.sock_conn.send(msg_bytes)
        response = self.sock_conn.recv(1024)
        return response

    def read_data(self, key):
        msg = f'read:::{key}\n'
        msg_bytes = msg.encode()
        self.sock_conn.send(msg_bytes)
        response = self.sock_conn.recv(1024)
        return response

    def delete_data(self, key):
        msg = f'delete:::{key}\n'
        msg_bytes = msg.encode()
        self.sock_conn.send(msg_bytes)
        response = self.sock_conn.recv(1024)
        return response
    