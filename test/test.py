import time
import os

# File to append to
file_path = "small_file.txt"

# Ensure the file exists (create or clear it)
with open(file_path, "w") as f:
    f.write("Initial content\n")

# Measure time for a single append operation
start_time = time.time_ns()  # Nanosecond granularity
with open(file_path, "a") as f:
    f.write("Appended line\n")
end_time = time.time_ns()

# Calculate time taken in nanoseconds
time_taken_ns = end_time - start_time

print(f"Time taken for append operation: {time_taken_ns} nanoseconds")
print(f"Time taken in seconds: {time_taken_ns / 1_000_000_000:.6f} seconds")
