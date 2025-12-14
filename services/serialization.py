import time, struct, zlib

def encode_record(key, value):
    key_bytes = key.encode()
    value_bytes = value.encode()

    timestamp = int(time.time())
    key_size = len(key_bytes)
    value_size = len(value_bytes)

    # struct.pack, converts these 3 integer values to bytes
    # the header will be of 
    header = struct.pack('>III', timestamp, key_size, value_size)

    # body stores the complete infor for a particular ops
    body = header + key_bytes + value_bytes

    # crc is just a hash of the above body, which we will also store for checking
    # data corruption
    # this is an integer of 4 bytes
    crc = zlib.crc32(body) & 0xffffffff

    # we then store crc + body in the binary file
    record = struct.pack('>I', crc) + body

    return record

# f is the file object, its pointer should be to a particular offset for that key info
def decode_record(f):
    # reading 4 bytes of crc
    crc_bytes = f.read(4)
    if not crc_bytes:
        # end of file
        return None, None, None
    
    # reading 12 bytes of header, which packed using struct.pack
    # this containe, timestamp, key_size and value_size
    header = f.read(12)
    timestamp, key_size, value_size = struct.unpack('>III', header)

    key_bytes = f.read(key_size)
    value_bytes = f.read(value_size)

    # now we will again create the body, and check if the crc hash we can build from
    # the data we just read is matching what we had written 
    written_crc_val = struct.unpack('>I', crc_bytes)[0]
    read_body = header + key_bytes + value_bytes

    read_crc_val = zlib.crc32(read_body) & 0xffffffff
    if read_crc_val != written_crc_val:
        raise ValueError("CRC mismatch")

    key = key_bytes.decode()
    value = value_bytes.decode()

    return key, value, timestamp

