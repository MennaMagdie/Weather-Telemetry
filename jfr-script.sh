#!/bin/bash

# 1. GC pauses count and max duration
echo "=== GC Pauses Count ==="
jfr print --events "jdk.GCPhasePause" recording.jfr | grep -c "duration"

echo -e "\n=== GC Maximum Pause Duration ==="
jfr print --events "jdk.GCPhasePause" recording.jfr | grep "duration" | sort -V | tail -n 1

# 2. Top 10 Classes with highest total memory
echo -e "\n=== Top 10 Classes with Highest Total Memory ==="
jfr print --events "jdk.ObjectAllocationSample" recording.jfr | awk '/objectClass|weight/ {print $0}' | \
  sed 'N;s/\n//' | \
  sort | \
  uniq -c | \
  sort -rn | \
  head -n 10

# 3. List of I/O operations
echo -e "\n=== List of I/O Operations ==="
jfr print --events "jdk.FileRead" recording.jfr | grep -E "path|bytesRead|duration"

# #!/bin/bash

# # 1. GC pauses count and max duration
# jfr print --events "jdk.GCPhasePause" recording.jfr | grep duration

# # 2. Top 10 classes by memory
# # jfr print --events "jdk.OldObjectSample" recording.jfr | grep -E "objectSize|object = "
# jfr print --events "jdk.OldObjectSample" recording.jfr | grep -E "objectSize|objectAge|object ="

# # 3. List of I/O operations
# jfr print --events "jdk.FileRead" recording.jfr | grep -E "path|bytesRead|duration"