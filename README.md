# Weather Station Telemetry

| Name | ID |
| --- | --- |
| Menna Magdy | 8082 |
| Sama Abdou | 7978 |
| Asmaa Ghanem | 8277 |
| Omnia Farouk | 7977 |

## **System Overview**

This project implements a distributed weather station monitoring system using modern data-intensive application principles. The system collects real-time weather data from 10 simulated weather stations, processes and archives the data, and provides both real-time and historical querying capabilities.

The architecture follows a three-stage pipeline:

- Data Acquisition: 10 weather stations emit readings to a Kafka message queue
- Data Processing & Archiving: Central base station consumes and archives data in Parquet files
- Indexing: Bitcask key-value store for latest readings + ElasticSearch/Kibana for historical analysis

The system runs entirely on Kubernetes (Minikube) with the following
pods:

- 10 Weather Station pods (weather-station-1 through weather-station-10)
- 1 Kafka broker	pod (KRaft mode, no Zookeeper)
- 1 Central	Station pod
- 1 ElasticSearch + Kibana pod

## **System Pipeline**

![image.png](Weather%20Station%20Telemetry/image.png)

Go online to view this image

## **Pipeline Breakdown (Implementation)**

| 1. Weather Stations (Kafka Producers +  Kafka Processor) |
| --- |
| 2. Central Station (Kafka Consumer + Bitcask) |
| 3. BitCask Client |
| 4. Parquet Archiver |
| 5. Elastic Search & Kibana |
| 6. Kubernetes Deployment |
| 7. JFR Profiling |
| 8. Bonus Parts |

## **1) Weather Stations**

Each weather station emits a JSON status message every 1 second. Messages follow the required schema with randomized fields:

```jsx
public WeatherStatusMessage(long station_id, long s_no, String battery_status, long status_timestamp, WeatherDetails weather) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = weather;
    }
```

The battery_status distribution is randomized as specified:

- Low: 30% of messages
- Medium: 40% of messages
- High: 30% of messages

A 10% message droprate is implemented; messages are randomly discarded before
sending to Kafka to simulate real-world packet loss.

## 1.1. Kafka Producer

Each weather station uses the Kafka Producer Java API to send messages to the weather-data topic. The station ID is used as the partition key to ensure all messages from the same station go to the same partition.

```jsx
 private static final String TOPIC = "weather-data";
    private static final long STATION_ID = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    
    private final KafkaProducer<String, String> producer;
    private final WeatherStationMock weatherStationMock;
    private final ObjectMapper objectMapper;

    public WeatherStationProducer(){
        System.out.println("Connecting to Kafka at: " + BOOTSTRAP_SERVERS); 
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        properties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "5000");

        this.producer = new KafkaProducer<>(properties);
        this.weatherStationMock = new WeatherStationMock(STATION_ID); // a weather station for each ID
        
```

### Testing

```bash
kubectl logs -n pipeline deployment/weather-station-1 --tail=5
```

![Screenshot from 2026-05-22 23-30-14.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-30-14.png)

---

## **1.2. Kafka Processor - Raining Trigger**

A Kafka Streams processor detects rain conditions by filtering messages where humidity exceeds 70%. Matching messages are routed to a dedicated rain-alerts topic.

```bash
return """
                    {
                        "alert": "RAIN DETECTED",
                        "humidity": %d,
                        "temperature": %d
                    }
                    """.formatted(humidity, temperature);
------------------------------------------------------------------  
                    
.to("rain-alerts");
```

---

## 2. Central Station

## 2.1. Kafka Consumer

Same config as the producer, a kafka consumer was implemented in the central base station to read messages from the topic weather-data

![Screenshot from 2026-05-22 23-30-21.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-30-21.png)

## 2.2. Bitcask KV Storage Engine

The Bitcask Riak implementation maintains an always up-to-date view of each station's latest status. Key design decisions:

- Append-only segment files with a 64MB max segment size
- old values are never overwritten in place, only superseded by newer appends and eventually reclaimed by compaction.
- Therefore, O(1) for write operations
- In-memory KeyDir hash map for O(1) key lookups
- Hint files for fast index reconstruction on startup
- Background compaction scheduled every 3 minutes to merge old segments
- Read-write locks for concurrent access safety

## **2.2.1 BitCask Structure**

![Screenshot 2026-05-23 021545.png](Weather%20Station%20Telemetry/Screenshot_2026-05-23_021545.png)

### **1. Segment**

This class defines the attributes and methods that may be needed to manipulate the segment which is used as the append-only log file in the system

```
    private final String filePath;
    private final RandomAccessFile logFile;
    private final String fileId;
    private long curentOffset;
    private boolean isActive = false;
    // boolean isNew;
    private boolean isClosed = false;
    // private final FileChannel readChannel;
    
    String read(long valueOffset, int valueSize)
    long append(Record record)
    void sync()
```

The Segment uses a RandomAccessFile as the IO stream with the file on the disk for simplicity.

> FileChannel may be used for read operations better performance as it enforces the reading to be in an atomic manner where it prevents 2 different requests that may be interleaved between the `seek()`  and `read()`
> 

 New Active Segment is intialized and written to in a fixed manner to allow any value to be retrieved by a single seek to its known byte offset.

Important Notes:

- The format of each segment entry/record
    
    `[timestamp = 8 bytes][keylen = 4 bytes][valueLen = 4 bytes][key][value]` according to bitcask paper
    
- Methods defintions:
    - `read(valueOffset)`  for reading from segment and returning value via the given valueOff
    - `setlong append(Record record)`  for appending the new value -weather-station msg
    - `sync()` for manual trigger of flushing the IO stream to the file in disksync was used to prevent any lose of un-written value in case of crash
    
    ### **2. Record**
    
    Describes each message to be sent and saved in the Segment
    
    ### **3. KeyDirEntry**
    
    Describes each entry in the index map kept in the memory for fast O(1) retreival 
    
    ```
        private String key;
        private String Value;
        private long timestamp;
    ```
    
    ### **4. Segment Entry**
    
    Describes the entry saved in each segment + the segment it is actually saved to via fileId
    
    ```java
       private final long timestamp;
       private final int keysz;
       private final int valuesz;
       private final String key;
       private final long valueOffset;
       private final String fileId;
    ```
    
    ### **5. Hint File**
    
    This is a Static class which is used to define the methods to manipulate anything related to creating, generating or readings from the Hint Classes
    
    > Hint Files are used to save the data of the keyDir for fast reconstruction and recovery in case of crashes instead of reading data THEN reconstructing the Index from the `segment.data` files
    > 
    
    for every KeyDirEntry whose fileId matches this segment:
    write a row:  `[timestamp][keyLen][offset][valueSize][key]`
    
    Then save as `seg_X.hint` alongside `seg_X.data`
    
    They store only key-location metadata per entry: `timestamp, keyLen, valueOffset, valueSize`, and `key` bytes, but never the value itself, as it is not needed for the index reconstruction where the value read adds unnecessary overhead (the case when reading from .data)
    
    Methods definitions
    
    ```java
    void createHintFile(Segment segment)
    void rebuildFromDataFile(Segment segment, KeyDir keyDir)
    KeyDir rebuild(String directoryPath)
    void deleteHintFile(Segment segment)
    ```
    
    `createHintFile(segment)`  
    
    - Called when a merged segment is sealed/marked as full (Storage Completed)
    - Writes a compact index .hint file alongside the .data file
    - the hint file saves rows of
    
    ```java
    [timestamp][keysz][value_sz][value_pos][key]
    ```
    
    `rebuildFromDataFile()` 
    
    - Used to reconstuct the KeyDir from the .data files not .hint which is slower, hence why is prevented
    - Used only if a hint file is missing (e.g crash before hint was written)
    
    `rebuild()`
    
    - Called on startup — reads all hint files to restore KeyDir
    - Returns a fully populated KeyDir without reading .data files
    
    `deleteHintFile()`
    
    - Called to delete the old hint files which was attached to the previous .data files before merging and discarding those `segement.data` files
    
    ### **6. BitcaskServer**
    
    The Most Important Class which implements about all methods defined in the BitCask Paper implementation
    
    ![                                          referenced from bitcask paper
    ****](Weather%20Station%20Telemetry/Screenshot_2026-05-23_030606.png)
    
                                              referenced from bitcask paper
    ****
    
    **Class Attributes**
    
    ```java
    public final long MAX_SEGMENT_SIZE = 64 * 1024 * 1024;  // 64 MB    
    
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantLock mergeLock = new ReentrantLock();
    
    HashMap<String,Segment> segmentMap;
    Segment activeSegment;
    KeyDir keyDir;      // index Map
    ScheduledExecutorService scheduler;
    
    ```
    
    **Attributes Definitions**
    
    **`MAX_SEGMENT_SIZE = 64 MB`**
    
    - Industry-standard default used by LevelDB, RocksDB, and similar engines
    - At 10 stations × 1 msg/sec × ~200 bytes, a segment fills in roughly 9 hours of continuous operation
    - Long enough that compaction doesn't trigger too frequently under normal load
    - Short enough that individual files stay manageable for OS caching and hint file scanning on restart
    
    **`directoryPath`**
    
    - Root directory where all .data, .hint, and merged segment files live on disk
    
    **`rwLock` — `ReentrantReadWriteLock`**
    
    - Chosen over synchronized and only ReentrantLock to allow many concurrent get() calls with single fixed-rate writes
    - Allows multiple readers to proceed simultaneously
    - Write lock is acquired only during put(), segment rotation, and the compaction swap
    - Reentrant allows getAll() to call get() internally without deadlocking on the same thread
    
    **`mergeLock` — `ReentrantLock`**
    
    - Separate from rwLock ,solely prevents two compaction cycles from running concurrently
    - Scheduler uses tryLock(), if a merge is already running so, the next scheduled interval skips rather than queuing
    
    **`segmentMap` — `HashMap<String, Segment>`**
    
    - Maps each segment's fileId → Segment object for O(1) lookup during get()
    - The key matches exactly what is stored in each KeyDirEntry.fileId — making cross-reference direct
    - Holds all segments existing in the directory including the active one.
    
    **`activeSegment`**
    
    - Direct reference to the segment currently accepting writes (mutable)
    - put() always directly appends to
    
    **`keyDir` — `KeyDir`**
    
    - In-memory hash index mapping every key to its current value location on disk
    - Must be in memory, a disk-based index would require two seeks per read, defeating O(1) guarantee
    - Always kept up to date: every put() updates it, every compaction updates it, rebuilt fully on startup
    
    **`scheduler` — `ScheduledExecutorService`**
    
    - Runs merge() on a background thread every **3 minutes** after an initial **1-minute** delay
    - 3-minute period balances two concerns — too frequent wastes disk I/O, too infrequent wastes disk space
    - At 10 msg/sec, ~1,800 entries accumulate every 3 minutes — enough stale data to make compaction worthwhile
    - 1-minute initial delay lets the server fully start and receive real data before the first compaction attempt
    
    ### **Class Methods**
    
    ```
    public static BitcaskServer open(String directoryPath)
    public boolean put(String key, String value)
    public String get(String key) 
    public HashMap<String, String> getAll()
    public void merge() // can be later modified to private 
    private void rotateDataSegment()
    private void sync()
    public void close()
    ```
    
    **Methods Definitons**
    
    **`Open()`** 
    
    - Creates the directory if it doesn't exist, initializes empty segmentMap and keyDir
    - Scans directory for existing .data files and loads them as immutable segments into segmentMap
    - Rebuilds keyDir from .hint files, then falls back to scanning .data files for any segment missing a hint file
    - Creates a new active segment to append to during the server run
    - Starts the compaction scheduler to run merge() every 3 minutes
    
    **`put(String key, String value)`**
    
    - Creates a Record and appends it to the active segment, receiving back the exact byte offset of the value
    - Updates keyDir with the new location (fileId, valueOffset, valueSize, timestamp)
    - Returns true on success
    ****
    
    **`get(String key)`**
    
    - looks up the key in keyDir to get its location metadata
    - Finds the correct segment from segmentMap using the stored fileId
    - Seeks to valueOffset in that segment and reads exactly valueSize bytes
    - Returns the decoded string value, or null if key doesn't exist or an error occurs
    ****
    
    **`getAll()`**
    
    - Calls get() for each key to retrieve its current value
    - Builds and returns a HashMap<String, String> of all key-value pairs
    
    **`rotateDataSegment()`**
    
    - Marks the current active segment as full (immutable)
    - Creates a new active segment with a fresh timestamp-based ID
    - Adds the new segment to segmentMap and updates activeSegment reference
    
    **`merge()`**
    
    This is the main method used for merging and compaction
    
    - Called on Schedule fixed in open() at starting the server
    - Collects all immutable segments, scans their entries, and keeps only the latest value per key (by timestamp)
    - Writes the latest entries into one or more new merged segment files, rotating when a merged segment exceeds 64 MB
    - Deletes old .data and .hint files from disk after merging
    
    **`sync()`**
    
    - Iterates all segments in segmentMap and calls sync() on each
    - Forces the OS to flush file buffers to disk (durability guarantee)
    - Called at close() rather than after every write, to overcome the problem of complexity at syncing at every write
    
    > There is a tradeoff between sync() call on each write for durability and sync() call only at the end for keeping maximum throughput.
    > 
    
    **`close()`**
    
    - Shuts down the compaction scheduler and waits up to 60 seconds for any running merge to finish
    - Acquires write lock, marks active segment as full, and calls sync() to flush all data to disk
    - Closes all file descriptors for every segment in segmentMap

### **Design Decisions — Summary**

**1) Compaction Scheduler — Every 3 Minutes, First Run After 1 Minute**

- First run after **1 minute** — not to wait 3 minutes before the very first compaction, since the first compaction is what creates the initial hint files for fast future startups
- Then runs **every 3 minutes** periodically — leaves enough margin for the full merge + hint file creation to complete before the next cycle fires
- tryLock() ensures overlapping cycles skip rather than queue

**2) MAX_SEGMENT_SIZE — 64 MB**

- Too large → merge and hint file creation take too long per cycle, blocking disk I/O
- Too small → excessive .data file creation overhead and frequent segment rotations
- 64 MB is the established industry default for this exact tradeoff

**3) Hint Files — Content and Purpose**

- Store identical fields to data files **except the value** — entry format is `tstamp | ksz | value_sz | value_pos | key`
- Values are omitted because they are not needed to reconstruct the KeyDir — only key locations matter
- Without hint files, startup must scan every data file including all value bytes just to extract key positions — significantly slower
- Hint files are created **only after a successful merge** — never for the active segment, never on close alone
- After hint files are written, old .data and .hint files are deleted — only activeSegment.data and the latest merged.data + merged.hint remain on disk

**4) fsync — When and Why**

- fsync forces the OS to flush in-memory file cache to physical disk — without it, a crash could lose data sitting in the OS buffer
- Implemented at exactly **two places** only — not after every put()
- **At close()** — flushes all segment files when the server shuts down gracefully
- **After each merged segment is written** — ensures merged data is safely on disk before the old segments it replaced are deleted; skipping this would cause permanent data loss on a crash between the delete and the flush
- Intentionally skipped on every individual put() — doing it per write at 10 msg/sec would add 1–10ms latency per message and cap throughput unnecessarily for data that is continuously re-emitted by live stations

**5) Timestamp Design Decision**

- `System.currentTimeMillis()` risks collision if two records arrive within the same millisecond — acceptable for weather data since readings don't change at millisecond resolution
- `System.nanoTime()` resolves collisions but produces values meaningless to humans

### **2. BitCask LSM Directory Sample**

![Screenshot from 2026-05-22 23-40-55.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-40-55.png)

---

## 3. BitCask Client

A bash script client communicates with the central station's HTTP REST API to query Bitcask data:

The central station exposes two HTTP endpoints on port 8080:

![Screenshot from 2026-05-22 23-32-28.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-32-28.png)

- GET /bitcask/all — returns all key-value pairs as JSON
- GET /bitcask/key/{key} — returns value for a specific station key

Testing:

![Screenshot from 2026-05-22 23-33-39.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-33-39.png)

---

## 4. Parquet Archiving

All weather status messages are archived in Parquet format, partitioned by station ID, year, and month. Records are written in batches of 10 to reduce I/O overhead.

Testing:

![Screenshot from 2026-05-22 23-45-34.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-45-34.png)

Investigating the parquet files content to make sure they are correct:

![Screenshot from 2026-05-22 23-54-53.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-54-53.png)

---

## 5. Historical Analysis (Elastic Search and Kibana)

Parquet files are used as a data source for ElasticSearch indexing.
Kibana dashboards visualize:

- Battery status	distribution per station (confirming 30%/40%/30% split)
- Dropped message	count per station (confirming ~10% drop rate)

Testing

![Screenshot from 2026-05-21 18-00-19.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-21_18-00-19.png)

The distribution of battery status implies the messages were sent correctly

![Screenshot from 2026-05-23 01-16-14.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-23_01-16-14.png)

The number of dropped messages implies that the drop rate was implemented correctly

---

## 6. Kubernetes Deployment

The entire system is deployed on Kubernetes using a single YAML
manifest. All services run in the pipeline namespace with shared
persistent storage for Parquet files and Bitcask data.

Kubernetes
resources:

- Namespace:	pipeline
- PersistentVolumeClaim:	1Gi shared storage for Bitcask and Parquet data
- 10 Weather Station Deployments with individual STATION_ID env vars
- 1 Kafka Deployment (KRaft mode, no Zookeeper required)
- 1 Central	Station Deployment with HTTP server on port 8080
- 1 ElasticSearch + Kibana Deployment
- Services for Kafka, Central Station, and ElasticSearch

Testing

```bash
kubectl get pods -n pipeline
```

// ADD PODS IMAGE HERE YARAB

---

## 7. JFR PROFILING

The Central Station was profiled using Java Flight Recorder (JFR) for 60 seconds under production load with all 10 weather stations sending messages.

- We added the Flight Recorder to the ENTRYPOINTS of the docker file, and started recording for 60 seconds
- Then we used this bashscript to analyze the requirements

```bash
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
```

Testing:

![Screenshot from 2026-05-23 00-11-35.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-23_00-11-35.png)

---

## Bonus Requirements:

8.1. Open-Meteo

A Channel Adapter integrates the Open-Meteo external weather API with the Kafka pipeline. The adapter fetches real weather data from the API and publishes it to the weather-data topic in the same format as the simulated station messages. 

Channel Adapter is an enterprise integration pattern: a component (the adapter code) connects an external system (open-meteo API) to the messaging channel (Kafka). 

**Connection to external system (open-mateo)**

The data is fetched through an http request to the api url (from the open-meteo site) and the response is stored in string format and the body (that is the weather data ) reply is extracted.  

```java
private String getOpenMeteoData() throws IOException, InterruptedException {
    HttpRequest req = HttpRequest.newBuilder().uri(URI.create(CONNECTION_API_URL)).GET().build();
    HttpResponse<String> apiReply = httpclient.send(req, HttpResponse.BodyHandlers.ofString());
    return apiReply.body();
}
```

**Adapting the API response**

The API response from open-meteo:

```jsx
"current": {
  time": "2026-05-22T12:30",
  "interval": 900,
  "temperature_2m": 23.6,
  "relative_humidity_2m": 65,
  "wind_speed_10m": 16.2
 }
```

The response was then mapped to the weather status messages used by the weather station previously integrated:

```java
  private WeatherStatusMessage adapWeatherStatusMessage(double temp, int humidity, int windspeed){
      WeatherStatusMessage msg = new WeatherStatusMessage(
          STATION_ID,
          sequenceNumber++,
          "high",
          System.currentTimeMillis()/1000L,
          new WeatherDetails(humidity, (int)temp, windspeed)
      );
      return msg;
}
```

**Connection to Kafka (messaging channel)**

An instance of the Kafka producer was used to send the Open meteo data over the same topic as the other weather stations. The open mateo adapter was identified with a station ID similarly. The producer’s sendMessages metho was adapted to route the Open meteo data to the producer to get published over the topic.

```java
 public void sendOpenMeteoData() {
        while(true){try {
            String apiResponse = getOpenMeteoData();
            // json parsing and formatting to weatherStatus msg type:
            JsonNode rootNode = objectMapper.readTree(apiResponse);
            JsonNode currentWeather = rootNode.path("current");
            double temp = currentWeather.path("temperature_2m").asDouble();
            int humidity = currentWeather.path("relative_humidity_2m").asInt();
            int windspeed = currentWeather.path("wind_speed_10m").asInt();

            WeatherStatusMessage msg = adapWeatherStatusMessage(temp, humidity, windspeed);
            // String jsonMsg = objectMapper.writeValueAsString(msg);
            producer.sendMessages(msg); // send to Kafka

            System.out.println("Fetched OpenMeteo data and sent to Kafka: " + objectMapper.writeValueAsString(msg));

            Thread.sleep(10000); //10 secs bw every fetch 

        } catch (IOException | InterruptedException e) {
            System.err.println("Error fetching or processing OpenMeteo data: " + e.getMessage());
        }}
        
    }
```

---

8.2. Enterprise Integration Patterns:

5+ Integration Patterns were implemented in our code either implicitly or explicitly:

1. **Invalid Message Pattern (Dead Letter)**
    
    ![Screenshot from 2026-05-22 23-04-25.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-04-25.png)
    

Malformed or non-JSON messages are detected and routed to a dedicated `invalid-messages` Kafka topic instead of being silently dropped, allowing future inspection and reprocessing

---

1. **Message Filter Pattern**

![Screenshot from 2026-05-22 23-15-02.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-15-02.png)

The Kafka Streams processor filters messages where humidity exceeds 70% and routes them to the `rain-alerts` topic, acting as a rain detection trigger

1. **Message Channel Pattern**

![Screenshot from 2026-05-22 23-05-29.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-05-29.png)

The `weather-data` Kafka topic acts as the Message Channel connecting all 10 weather stations to the central station. Additional channels include `rain-alerts` for rain detection events and `invalid-messages` as a dead letter channel

---

1. **Consumer Polling Pattern**

![Screenshot from 2026-05-22 23-06-45.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-06-45.png)

The central station uses Kafka's pull-based polling model, explicitly calling `consumer.poll(Duration.ofMillis(1000))` every 1000ms to fetch messages at a controlled rate

---

1. **Envelope Wrapper**

![Screenshot from 2026-05-22 23-11-19.png](Weather%20Station%20Telemetry/Screenshot_from_2026-05-22_23-11-19.png)

Each weather message wraps the actual weather data (`humidity`, `temperature`, `wind_speed`) inside an envelope containing metadata fields (`station_id`, `s_no`, `battery_status`, `status_timestamp`). The envelope provides routing and identification information that the central station uses to store and process the message, while the inner `weather` object carries the actual payload.
**-** Could’ve been done like this as well

```bash
{
  "header": {
    "station_id": 1,
    "s_no": 1,
    "timestamp": 1681521224
  },
  "payload": {
    "battery_status": "low",
    "weather": {
      "humidity": 35,
      "temperature": 100,
      "wind_speed": 13
    }
  }
}
```

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)

[](data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7)