package com.central;

import com.central.Bitcask.BitcaskServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
// import java.util.concurrent.LinkedBlockingQueue;


public class CentralStationApp {

    private static final String TOPIC = "weather-data";
    // private static final String BITCASK_DIR = "./bitcask-data";
    // private static final int BATCH_SIZE = 5000;

    // Thread-safe buffer transferring records from Kafka to Parquet worker
    // private static final LinkedBlockingQueue<String> recordBuffer = new LinkedBlockingQueue<>(50000);

    public static void main(String[] args) throws Exception {

        String bitcaskDir = System.getenv("BITCASK_DATA_DIR");
        if (bitcaskDir == null) {
            bitcaskDir = "./bitcask-data"; 
        }

        KafkaConsumer<String, String> consumer = null;
        Parquet parquet = null;

        System.out.println("LAUNCHING CENTRAL BASE STATION <3");
        try {
            // 1. Open Bitcask (Normal local object initialization)
            BitcaskServer bitcask = BitcaskServer.open(bitcaskDir);
            System.out.println("init bitcask storage engine");

            // 2. Initialize Parquet Writer / Buffer
            // samsouma hate3melha
            parquet = new Parquet();
            System.out.println("init parquet done");

            // 3. Initialize Kafka Consumer
            // KafkaConsumer<String, String> consumer = ...
            Properties props = new Properties();
            // String bootstrapServers= System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group-v3");made an error
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


            // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC));
            
            // ObjectMapper objectMapper = new ObjectMapper(); for sama bardo
            System.out.println("[KAFKA] Connected to broker. Listening on topic: " + TOPIC);
            
            // System.out.println("Central Station running smoothly");

            // 4. The Single Processing Loop
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // System.out.println("manouna is debugging, poll completed. found records count: " + records.count());
                
                
                for (ConsumerRecord<String, String> record : records) {
                    String rawJson = record.value();
                    String stationKey = record.key(); // station_id passed as the partition key

                    if (stationKey == null) {
                        stationKey = "unknown_station";
                    }

                    if (rawJson == null || !rawJson.trim().startsWith("{")) {
                        System.err.println("[PARQUET WARNING] Skipping malformed non-JSON record: " + rawJson);
                        continue; // Skip this poison pill record completely!
                    }

                    System.out.println("[INGEST] Received stream packet from Station " + stationKey);

                    // Operation A: Update the Bitcask Key-Value view instantly
                    bitcask.put(stationKey, rawJson);
                    System.out.println("[BITCASK] Stored key: " + stationKey);
                    String readBack = bitcask.get(stationKey);
                    System.out.println("[BITCASK] Read back for key " + stationKey + ": " + readBack);
                    try {
                        parquet.write(rawJson);
                    } catch (Exception e) {
                        System.err.println("[PARQUET ERROR] " + e.getMessage());
                        e.printStackTrace();
                    }

                }
            }


        } catch (Exception e) {
            System.err.println("Error running ingestion worker: " + e.getMessage());
            e.printStackTrace();
        } finally {

            if (consumer != null) {
                System.out.println("Closing Kafka consumer network ports...");
                consumer.close();
            }
            if (parquet != null) {
                parquet.close();
            }
        }
    }
}