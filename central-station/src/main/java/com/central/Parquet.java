package com.central;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class Parquet {

    private static final int BATCH_SIZE = 10;
    private static final String BASE_DIR = System.getProperty("user.dir") + "/parquet-data";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Avro schema matching WeatherStatusMessage
    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "WeatherStatus",
          "fields": [
            {"name": "station_id",       "type": "long"},
            {"name": "s_no",             "type": "long"},
            {"name": "battery_status",   "type": "string"},
            {"name": "status_timestamp", "type": "long"},
            {"name": "humidity",         "type": "int"},
            {"name": "temperature",      "type": "int"},
            {"name": "wind_speed",       "type": "int"}
          ]
        }
        """;

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private final List<String> buffer = new ArrayList<>();

    // Called from the main Kafka loop for every incoming message
    public void write(String rawJson) throws IOException {
        buffer.add(rawJson);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    // Force-flush remaining records (call on shutdown)
    public void flush() throws IOException {
        if (buffer.isEmpty()) return;

        System.out.println("[PARQUET] Flushing " + buffer.size() + " records...");

        // Group by station + time partition then write
        for (String rawJson : buffer) {
            writeSingle(rawJson);
        }

        buffer.clear();
        System.out.println("[PARQUET] Flush complete.");
    }

    private void writeSingle(String rawJson) throws IOException {
        JsonNode node = mapper.readTree(rawJson);

        long stationId       = node.get("station_id").asLong();
        long timestamp       = node.get("status_timestamp").asLong();
        String batteryStatus = node.get("battery_status").asText();
        long sNo             = node.get("s_no").asLong();
        JsonNode weather     = node.get("weather");
        int humidity         = weather.get("humidity").asInt();
        int temperature      = weather.get("temperature").asInt();
        int windSpeed        = weather.get("wind_speed").asInt();

        // Partition path: parquet-data/station_id=1/year=2024/month=04/
        ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);
        String partitionPath = String.format("%s/station_id=%d/year=%d/month=%02d",
                BASE_DIR, stationId, dt.getYear(), dt.getMonthValue());

        // File name = timestamp of write
        java.io.File dir = new java.io.File(partitionPath);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            System.out.println("[PARQUET] Created directory: " + partitionPath + " → " + created);
        }

        String filePath = partitionPath + "/" + System.currentTimeMillis() + ".parquet";
        System.out.println("[PARQUET] Writing to: " + filePath);

        // Build Avro record
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("station_id",       stationId);
        record.put("s_no",             sNo);
        record.put("battery_status",   batteryStatus);
        record.put("status_timestamp", timestamp);
        record.put("humidity",         humidity);
        record.put("temperature",      temperature);
        record.put("wind_speed",       windSpeed);

        // Write single record to its partition file
        Configuration conf = new Configuration();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new Path(filePath))
                .withSchema(SCHEMA)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            writer.write(record);
        }
    }

    public void close() throws IOException {
        flush();
    }
}