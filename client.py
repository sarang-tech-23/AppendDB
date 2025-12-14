import socket

class AppendDbClient:
    def __init__(self, host="0.0.0.0", port=9001):
        self.host = host
        self.port = port

    def _send(self, msg):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(msg.encode())

            data = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in chunk: 
                    break
            # print(f'>> data_recv: {data}')
            return data

    def write_data(self, key, val):
        return self._send(f"write:::{key}:{val}\n")

    def read_data(self, key):
        return self._send(f"read:::{key}\n")

    def delete_data(self, key):
        return self._send(f"delete:::{key}\n")
