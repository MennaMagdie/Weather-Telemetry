package com.central;

import com.central.Bitcask.BitcaskServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
// import java.util.concurrent.LinkedBlockingQueue;


public class CentralStationApp {

    private static final String TOPIC = "weather-data";
    private static final String BITCASK_DIR = "./bitcask-data";
    // private static final int BATCH_SIZE = 5000;

    // Thread-safe buffer transferring records from Kafka to Parquet worker
    // private static final LinkedBlockingQueue<String> recordBuffer = new LinkedBlockingQueue<>(50000);

    public static void main(String[] args) throws Exception {

        KafkaConsumer<String, String> consumer = null;
        Parquet parquet = null;

        System.out.println("LAUNCHING CENTRAL BASE STATION <3");
        try {
            // 1. Open Bitcask (Normal local object initialization)
            BitcaskServer bitcask = BitcaskServer.open(BITCASK_DIR);
            System.out.println("init bitcask storage engine");

            // 2. Initialize Parquet Writer / Buffer
            // samsouma hate3melha
            parquet = new Parquet();

            // 3. Initialize Kafka Consumer
            // KafkaConsumer<String, String> consumer = ...
            Properties props = new Properties();
            // String bootstrapServers= System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            String bootstrapServers = System.getenv().getOrDefault("KAFKA_SERVERS", "localhost:9092");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group-v3");made an error
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

                    System.out.println("[INGEST] Received stream packet from Station " + stationKey);

                    // Operation A: Update the Bitcask Key-Value view instantly
                    bitcask.put(stationKey, rawJson);
                    try {
                        parquet.write(rawJson);
                        // Operation C: Send to ElasticSearch
                        String esHost = System.getenv().getOrDefault("ELASTICSEARCH_HOST", "localhost:9200");
                        URI uri = new URI("http://" + esHost + "/weather-data/_doc");
                        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/json");
                        conn.setDoOutput(true);
                        conn.getOutputStream().write(rawJson.getBytes());
                        conn.getResponseCode();
                        conn.disconnect();
                    } catch (Exception e) {
                        System.err.println("[PARQUET ERROR] " + e.getMessage());
                        e.printStackTrace();
                    }

                    // Operation B: Offer to the buffer queue for background Parquet writing
                    // boolean added = recordBuffer.offer(rawJson);
                    // if (!added) {
                    //     System.err.println("[WARNING] Memory buffer full! Slowing ingestion down to preserve data integrity.");
                    //     // Fallback to blocking write if the queue gets flooded
                    //     recordBuffer.put(rawJson);
                    // }
                }
            }
                // Read from Kafka
                // ConsumerRecords<String, String> records = consumer.poll(...);
                
                // For each incoming weather message:
                // String stationId = record.key();
                // String jsonMessage = record.value();

                // SIDE-BY-SIDE OPERATIONS:
                
                // Operation A: Update Bitcask for the latest view
                // bitcask.put(stationId, jsonMessage); 
                
                // Operation B: Append to Parquet for historical logging
                // parquetArchiver.write(jsonMessage);

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