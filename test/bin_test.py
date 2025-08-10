# with open('bin_file', 'wb') as file:
#     # Write bytes directly
#     file.write(b'Hello, World!')
#     # Write an integer as bytes
#     file.write((42).to_bytes(4, byteorder='big'))  # 4 bytes, big-endian



with open('bin_file', 'rb') as file:
    # Read all bytes
    # print('file_end_ofset__>>', file.tell())
    content = file.read()
    # print(content)  # Output: b'Hello, World!\x00\x00\x00*'
    # Read 5 bytes
    file.seek(14)  # Reset to start
    chunk = file.read(4)
    print(chunk.decode())  # Output: b'Hello'

    file.seek(0, 2)  # Seek to end (2 = from end)
    file_size = file.tell()  # Get position (size in bytes)
    print(f"File size: {file_size} bytes")