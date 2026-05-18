package com.central;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parquet {

    private static final int BATCH_SIZE = 10;
    private static final String BASE_DIR = System.getProperty("user.dir") + "/parquet-data";
    private static final ObjectMapper mapper = new ObjectMapper();

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

    public void write(String rawJson) throws IOException {
        buffer.add(rawJson);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    public void flush() throws IOException {
        if (buffer.isEmpty()) return;

        System.out.println("[PARQUET] Flushing " + buffer.size() + " records...");

        // Group records by partition key
        Map<String, List<GenericRecord>> partitionMap = new HashMap<>();

        for (String rawJson : buffer) {
            JsonNode node = mapper.readTree(rawJson);

            long stationId       = node.get("station_id").asLong();
            long timestamp       = node.get("status_timestamp").asLong();
            String batteryStatus = node.get("battery_status").asText();
            long sNo             = node.get("s_no").asLong();
            JsonNode weather     = node.get("weather");
            int humidity         = weather.get("humidity").asInt();
            int temperature      = weather.get("temperature").asInt();
            int windSpeed        = weather.get("wind_speed").asInt();

            ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(ZoneOffset.UTC);
            String partitionKey = String.format("%s/station_id=%d/year=%d/month=%02d",
                    BASE_DIR, stationId, dt.getYear(), dt.getMonthValue());

            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("station_id",       stationId);
            record.put("s_no",             sNo);
            record.put("battery_status",   batteryStatus);
            record.put("status_timestamp", timestamp);
            record.put("humidity",         humidity);
            record.put("temperature",      temperature);
            record.put("wind_speed",       windSpeed);

            partitionMap.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(record);
        }

        // Write each partition in one file with one writer
        for (Map.Entry<String, List<GenericRecord>> entry : partitionMap.entrySet()) {
            String partitionPath = entry.getKey();
            List<GenericRecord> records = entry.getValue();

            File dir = new File(partitionPath);
            if (!dir.exists()) dir.mkdirs();

            String filePath = partitionPath + "/" + System.currentTimeMillis() + ".avro";
            System.out.println("[PARQUET] Writing " + records.size() + " records to: " + filePath);

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SCHEMA);
            try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
                writer.create(SCHEMA, new File(filePath));
                for (GenericRecord record : records) {
                    writer.append(record);
                }
            }
        }

        buffer.clear();
        System.out.println("[PARQUET] Flush complete.");
    }

    public void close() throws IOException {
        flush();
    }
}